use crate::execution::live_pool_cache::LivePoolCache;
use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use serde_json::{json, Value};
use solana_account_decoder::UiAccountEncoding;
use solana_client::rpc_config::{
    RpcAccountInfoConfig, RpcProgramAccountsConfig, RpcTransactionConfig,
};
use solana_client::rpc_filter::{Memcmp, MemcmpEncodedBytes, RpcFilterType};
use solana_sdk::hash::hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use solana_sdk::signature::Signature;
use solana_transaction_status::UiTransactionEncoding;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, info, warn};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMPFUN_AMM_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
// Observed on-chain in PumpSwap/Pump.fun AMM swaps: `fee_program` is this program id.
const PUMPFUN_AMM_FEE_PROGRAM_ID: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
// Global fee_config account - owned by Fee Program, same for ALL pools.
// Observed in successful on-chain SELL and BUY transactions.
const PUMPFUN_AMM_FEE_CONFIG: &str = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx";
/// Global config account — same for **all** PumpSwap pools (swap instruction account #2).
/// Verified from successful mainnet SELL/BUY txs; must not be read from market account bytes at
/// misaligned offsets (see incident: wrong bytes → pubkey with no account → Anchor 3012 on `global_config`).
const PUMPFUN_AMM_GLOBAL_CONFIG: &str = "ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw";

/// Canonical PumpSwap `event_authority` PDA for swap instructions (Anchor seed `__event_authority`).
///
/// Same for all pools under `PUMPFUN_AMM_PROGRAM_ID`. DexPoolAccounts / cache slot [8] may carry a
/// wrong pubkey (mis-parse or stale layout); the program validates this account with seeds →
/// `ConstraintSeeds` (2006) if the passed key is not this PDA. See `dex_parser.rs` (SELL ix account 15).
fn pump_amm_canonical_event_authority(program_id: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(&[b"__event_authority"], program_id).0
}

/// Canonical `protocol_fee_recipient` + `protocol_fee_recipient_ta` for PumpSwap swap metas (#9/#10).
///
/// Same derivation as `dex_parser.rs` `build_pool_accounts_from_create_pool` and
/// `build_swap_ix_from_pool_accounts`. Quote side is WSOL (SPL Token program) for all supported pools.
fn pump_amm_canonical_protocol_fee_accounts(
    quote_mint: Pubkey,
    quote_token_program: Pubkey,
) -> (Pubkey, Pubkey) {
    let protocol_fee_recipient = Pubkey::from_str(PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT)
        .expect("PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT must be valid base58");
    let protocol_fee_recipient_ta = PumpFunAmmDex::derive_ata_with_program(
        protocol_fee_recipient,
        quote_mint,
        quote_token_program,
    );
    (protocol_fee_recipient, protocol_fee_recipient_ta)
}

// Fallback protocol fee recipient when automatic discovery fails (observed in many PumpSwap pools).
// This is the canonical Pump.fun protocol fee recipient wallet (owned by System Program, not a PDA).
// Account: JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU (verified from multiple successful swap txs).
const PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT: &str =
    "JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU";

// Best-effort: observed Pump.fun AMM "market" account layout contains
// - base_mint at byte offset 43
// - quote_mint at byte offset 75
// Using `getProgramAccounts` with memcmp filters avoids reliance on tx-history on pruned RPC.
const PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET: u64 = 43;
const PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET: u64 = 75;
/// Minimum market account data length to read base_mint + quote_mint at fixed offsets.
const PUMPFUN_AMM_MARKET_MIN_DATA_LEN: usize = PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32;

// PumpSwap AMM market account (301 bytes on mainnet): seed pubkey for `creator_vault` PDA lives here.
// Distinct from bonding-curve `creator-vault` (hyphen) — AMM uses underscore `creator_vault` + this seed.
const PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET: usize = 211;

// Observed on-chain: buy_exact_quote_in fee fields sum to 125 bps (lp 2 + protocol 93 + creator 30).
// We use that as a conservative default for quoting.
const DEFAULT_TOTAL_FEE_BPS: u32 = 125;

fn anchor_disc(ix_name: &str) -> [u8; 8] {
    let out = hash(format!("global:{ix_name}").as_bytes());
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&out.as_ref()[..8]);
    disc
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct PumpAmmPoolStatic {
    pool_market: Pubkey,
    global_config: Pubkey,
    base_mint: Pubkey,
    quote_mint: Pubkey,
    pool_base_vault: Pubkey,
    pool_quote_vault: Pubkey,
    protocol_fee_recipient: Pubkey,
    protocol_fee_recipient_ta: Pubkey,
    event_authority: Pubkey,
    coin_creator_vault_ata: Pubkey,
    coin_creator_vault_authority: Pubkey,
    global_volume_accumulator: Pubkey,
    fee_config: Pubkey,
    fee_program: Pubkey,
}

#[derive(Debug, Clone)]
struct PumpAmmUserAccounts {
    user_base_ta: Pubkey,
    user_quote_ta: Pubkey,
    user_volume_accumulator: Pubkey,
}

#[derive(Debug, Clone)]
struct TokenAccountMeta {
    address: Pubkey,
    mint: Pubkey,
    token_owner: Pubkey,
    balance: u64,
}

#[derive(Debug, Clone)]
struct ProgramOwnedAccountMeta {
    address: Pubkey,
    data_len: usize,
}

#[derive(Clone)]
pub struct PumpFunAmmDex {
    rpc: Arc<SolanaRpc>,
    user_authority: Option<Pubkey>,

    // Prevent concurrent pool discovery storms (e.g. parallel exits) from hammering RPC.
    discovery_lock: Arc<Mutex<()>>,

    // Cache by base mint (WSOL quote only for now)
    pools_by_base: DashMap<Pubkey, PumpAmmPoolStatic>,
    // Index by pool_market address (for load_pool_by_address)
    pools_by_market: DashMap<Pubkey, Pubkey>, // pool_market -> base_mint
    user_accounts: DashMap<(Pubkey, Pubkey), PumpAmmUserAccounts>, // (pool_market, user)
    // Extra cached data (e.g., token_program:<mint> → program_id)
    cached_data: DashMap<String, String>,

    /// Optional reference to the Geyser-fed LivePoolCache.
    /// When present, quote_exact_in() reads reserves from cache instead of RPC,
    /// and discover_pool_static() checks for cached pool_accounts first.
    /// This is the primary mechanism for eliminating RPC calls from the hot path.
    live_pool_cache: Option<Arc<LivePoolCache>>,

    /// When LivePoolCache is set and cache miss: if false, return None (Hot Path, no RPC).
    /// If true, fall back to RPC discovery (Cold Path, e.g. Liquidation). P3 #12.
    allow_rpc_on_miss: bool,
}

impl PumpFunAmmDex {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            user_authority: None,
            discovery_lock: Arc::new(Mutex::new(())),
            pools_by_base: DashMap::new(),
            pools_by_market: DashMap::new(),
            user_accounts: DashMap::new(),
            cached_data: DashMap::new(),
            live_pool_cache: None,
            allow_rpc_on_miss: true, // No cache: always RPC (Cold Path only)
        }
    }

    /// Create a new PumpFunAmmDex with a LivePoolCache reference for Geyser-first quoting.
    /// When the cache is provided, quote_exact_in() reads reserves from cache instead of RPC.
    /// `allow_rpc_on_miss`: false = Hot Path (Cache miss → None), true = Cold Path (Cache miss → RPC fallback). P3 #12.
    pub fn new_with_cache(
        rpc: Arc<SolanaRpc>,
        live_pool_cache: Arc<LivePoolCache>,
        allow_rpc_on_miss: bool,
    ) -> Self {
        let mut dex = Self::new(rpc);
        dex.live_pool_cache = Some(live_pool_cache);
        dex.allow_rpc_on_miss = allow_rpc_on_miss;
        dex
    }

    pub fn set_user_authority(&mut self, user: Pubkey) {
        self.user_authority = Some(user);
    }

    /// Fetch transaction via SolanaRpc and return as Value for legacy JSON parsers.
    async fn fetch_tx_as_value(&self, sig: &str) -> Result<Value> {
        let sig_parsed = sig.parse::<Signature>().context("invalid signature")?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::JsonParsed),
            max_supported_transaction_version: Some(0),
            commitment: None,
        };
        let tx = self
            .rpc
            .get_transaction_with_config_retry(&sig_parsed, cfg)
            .await
            .map_err(|e| anyhow!("getTransaction failed: {e}"))?;
        let tx_val = serde_json::to_value(&tx).context("serialize transaction")?;
        Ok(json!({"result": tx_val}))
    }

    /// Return the deterministic v1 pool-accounts list for a Pump.fun AMM pool.
    ///
    /// Ordering matches `MarketEventKind::DexPoolAccounts` (PumpSwap v1) and
    /// `PumpFunAmmDex::build_swap_ix_from_pool_accounts`.
    ///
    /// Returns 14 accounts:
    /// [0] pool_market, [1] global_config, [2] base_mint, [3] quote_mint,
    /// [4] pool_base_vault, [5] pool_quote_vault, [6] protocol_fee_recipient,
    /// [7] protocol_fee_recipient_ta, [8] event_authority, [9] coin_creator_vault_ata,
    /// [10] coin_creator_vault_authority, [11] global_volume_accumulator,
    /// [12] fee_config, [13] fee_program
    /// Optional pool_address hint for fast-path discovery (single getAccount vs slow getProgramAccounts).
    /// Used by I-24d EnsurePumpAmmPoolAccounts when execution-engine knows pool from cache/position.
    pub async fn pool_accounts_v1_for_base_mint_with_hint(
        &self,
        base_mint: Pubkey,
        pool_address_hint: Option<Pubkey>,
    ) -> Result<Option<Vec<Pubkey>>> {
        // GEYSER-FIRST: Check LivePoolCache for pre-cached pool_accounts before RPC discovery.
        // These come from DexPoolAccounts events (parsed from verified on-chain swap txs)
        // and are more reliable than the heuristic-based RPC discovery.
        if let Some(ref cache) = self.live_pool_cache {
            if let Some(accounts) = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint) {
                if accounts.len() >= 14 {
                    debug!(
                        base_mint = %base_mint,
                        accounts_len = accounts.len(),
                        "pump_amm: pool_accounts from LivePoolCache (ZERO RPC)"
                    );
                    return Ok(Some(accounts));
                }
            }
            // Cache miss: Hot Path (allow_rpc_on_miss=false) → None. Cold Path (true) → RPC fallback. P3 #12.
            if !self.allow_rpc_on_miss {
                debug!(base_mint = %base_mint, "pump_amm: pool_accounts cache miss, returning None (no RPC)");
                return Ok(None);
            }
        }

        // I-24d FAST PATH: When pool_address_hint provided, try single getAccount first.
        // Avoids slow getProgramAccounts scan that routinely exceeds 15s discovery timeout.
        if let Some(pool_market) = pool_address_hint {
            info!(
                base_mint = %base_mint,
                pool = %pool_market,
                "pump_amm: pool_address hint provided, trying direct getAccount (fast path)"
            );
            match self
                .try_parse_pool_static_from_market_account(pool_market, base_mint)
                .await
            {
                Ok(Some(pool)) => {
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    info!(
                        base_mint = %base_mint,
                        pool_market = %pool.pool_market,
                        "pump_amm: PumpAmmPoolStatic from pool_address hint (fast path)"
                    );
                    return Ok(Some(vec![
                        pool.pool_market,
                        pool.global_config,
                        pool.base_mint,
                        pool.quote_mint,
                        pool.pool_base_vault,
                        pool.pool_quote_vault,
                        pool.protocol_fee_recipient,
                        pool.protocol_fee_recipient_ta,
                        pool.event_authority,
                        pool.coin_creator_vault_ata,
                        pool.coin_creator_vault_authority,
                        pool.global_volume_accumulator,
                        pool.fee_config,
                        pool.fee_program,
                    ]));
                }
                Ok(None) => {
                    warn!(
                        base_mint = %base_mint,
                        pool = %pool_market,
                        "pump_amm: pool_address hint parse returned None; refusing discover_pool_static (no unbounded getProgramAccounts)"
                    );
                    return Err(anyhow!(
                        "pump_amm: pool_address hint parse returned no usable pool (base_mint={}, pool={}); refusing unbounded RPC discovery (I-24d)",
                        base_mint,
                        pool_market
                    ));
                }
                Err(e) => {
                    warn!(
                        base_mint = %base_mint,
                        pool = %pool_market,
                        error = %e,
                        "pump_amm: pool_address hint parse failed; refusing discover_pool_static (no unbounded getProgramAccounts)"
                    );
                    return Err(e.context(format!(
                        "pump_amm: pool_address hint parse error (base_mint={}, pool={}); refusing unbounded RPC discovery (I-24d)",
                        base_mint, pool_market
                    )));
                }
            }
        }

        // RPC FALLBACK (Cold Path only): No LivePoolCache or allow_rpc_on_miss — discover pool via RPC heuristics.
        let pool = match self.discover_pool_static(base_mint).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // CRITICAL: global_volume_accumulator is required for BUY (BuyExactQuoteIn).
        // The PumpSwap program validates it exists and is initialized.
        // Without it: "AccountNotInitialized" error (Custom(3012)).
        Ok(Some(vec![
            pool.pool_market,                  // [0]
            pool.global_config,                // [1]
            pool.base_mint,                    // [2]
            pool.quote_mint,                   // [3]
            pool.pool_base_vault,              // [4]
            pool.pool_quote_vault,             // [5]
            pool.protocol_fee_recipient,       // [6]
            pool.protocol_fee_recipient_ta,    // [7]
            pool.event_authority,              // [8]
            pool.coin_creator_vault_ata,       // [9]
            pool.coin_creator_vault_authority, // [10]
            pool.global_volume_accumulator,    // [11] - REQUIRED for BUY!
            pool.fee_config,                   // [12]
            pool.fee_program,                  // [13]
        ]))
    }

    /// Convenience wrapper: pool_accounts_v1_for_base_mint without hint.
    pub async fn pool_accounts_v1_for_base_mint(
        &self,
        base_mint: Pubkey,
    ) -> Result<Option<Vec<Pubkey>>> {
        self.pool_accounts_v1_for_base_mint_with_hint(base_mint, None)
            .await
    }

    fn derive_user_volume_accumulator(
        program_id: Pubkey,
        pool_market: Pubkey,
        user: Pubkey,
    ) -> Pubkey {
        // Best-effort PDA derivation.
        // Observed accounts suggest this is a user-specific PDA; we default to a common Anchor seed
        // pattern: ("user_volume_accumulator", pool_market, user).
        //
        // If this is wrong, the tx will fail with an account constraint error; we can then adjust
        // based on observed on-chain addresses.
        let seeds: [&[u8]; 3] = [
            b"user_volume_accumulator",
            pool_market.as_ref(),
            user.as_ref(),
        ];
        Pubkey::find_program_address(&seeds, &program_id).0
    }

    async fn rpc_get_account_owner_and_executable(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool)>> {
        let acc = match self.rpc.get_account_opt_retry(&address).await {
            Ok(Some(a)) => a,
            Ok(None) => return Ok(None),
            Err(e) => return Err(anyhow!("get_account failed: {e}")),
        };
        Ok(Some((acc.owner, acc.executable)))
    }

    async fn rpc_get_account_owner_executable_and_data(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool, Vec<u8>)>> {
        let acc = match self.rpc.get_account_opt_retry(&address).await {
            Ok(Some(a)) => a,
            Ok(None) => return Ok(None),
            Err(e) => return Err(anyhow!("get_account failed: {e}")),
        };
        Ok(Some((acc.owner, acc.executable, acc.data)))
    }

    fn parse_spl_token_account_mint_and_owner(data: &[u8]) -> Option<(Pubkey, Pubkey)> {
        // SPL token account layout: mint @ 0..32, owner @ 32..64
        if data.len() < 64 {
            return None;
        }
        let mint = Pubkey::new_from_array(data.get(0..32)?.try_into().ok()?);
        let owner = Pubkey::new_from_array(data.get(32..64)?.try_into().ok()?);
        Some((mint, owner))
    }

    fn parse_spl_token_account_amount(data: &[u8]) -> Option<u64> {
        // SPL token account layout: amount @ 64..72 (little-endian u64).
        let amt_bytes: [u8; 8] = data.get(64..72)?.try_into().ok()?;
        Some(u64::from_le_bytes(amt_bytes))
    }

    fn derive_ata_with_program(owner: Pubkey, mint: Pubkey, token_program: Pubkey) -> Pubkey {
        let owner_spl = SplProgramPubkey::new_from_array(owner.to_bytes());
        let mint_spl = SplProgramPubkey::new_from_array(mint.to_bytes());
        let token_program_spl = SplProgramPubkey::new_from_array(token_program.to_bytes());
        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &owner_spl,
            &mint_spl,
            &token_program_spl,
        );
        Pubkey::new_from_array(ata_spl.to_bytes())
    }

    async fn derive_existing_pda(
        &self,
        program_id: Pubkey,
        seed_sets: &[Vec<Vec<u8>>],
    ) -> Result<Option<Pubkey>> {
        for seed_set in seed_sets {
            let seed_slices: Vec<&[u8]> = seed_set.iter().map(|s| s.as_slice()).collect();
            let candidate = Pubkey::find_program_address(&seed_slices, &program_id).0;
            let Some((owner, executable)) =
                self.rpc_get_account_owner_and_executable(candidate).await?
            else {
                continue;
            };
            if !executable && owner == program_id {
                return Ok(Some(candidate));
            }
        }
        Ok(None)
    }

    async fn try_parse_pool_static_from_market_account(
        &self,
        pool_market: Pubkey,
        expected_base_mint: Pubkey,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        self.try_parse_pool_static_from_market_account_inner(pool_market, expected_base_mint, None)
            .await
    }

    /// Parse PumpSwap AMM pool structure from on-chain market account.
    /// If `prefetched_data` is Some, use it instead of re-fetching via RPC.
    /// This avoids transient RPC inconsistencies when the account was already fetched.
    ///
    /// COLD PATH ONLY. Uses RPC (getAccountInfo). Never call from hot path.
    async fn try_parse_pool_static_from_market_account_inner(
        &self,
        pool_market: Pubkey,
        expected_base_mint: Pubkey,
        prefetched_data: Option<(Pubkey, bool, Vec<u8>)>,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_quote_mint = Pubkey::from_str(WSOL_MINT)?;

        let (owner, executable, data) = if let Some(pf) = prefetched_data {
            pf
        } else {
            let Some(r) = self
                .rpc_get_account_owner_executable_and_data(pool_market)
                .await?
            else {
                return Ok(None);
            };
            r
        };
        if executable || owner != pump_amm_program {
            return Ok(None);
        }

        if data.len() < PUMPFUN_AMM_MARKET_MIN_DATA_LEN {
            return Ok(None);
        }

        // Canonical global config (same for every pool). Do **not** slice from market bytes:
        // misaligned/wrong offsets yield pubkeys with no on-chain account → Anchor 3012 on swap.
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        let base_mint = Pubkey::new_from_array(
            data[PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("market base_mint slice"))?,
        );
        let quote_mint = Pubkey::new_from_array(
            data[PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("market quote_mint slice"))?,
        );

        // This fallback is only used for WSOL pairs and assumes the program's swap semantics
        // (buy uses quote-in, sell uses base-in).
        if base_mint != expected_base_mint || quote_mint != expected_quote_mint {
            return Ok(None);
        }

        // Parse the remaining 32-byte fields after quote_mint; these typically include the
        // pool vaults + fee/creator accounts.
        let mut rest_pubkeys: Vec<Pubkey> = Vec::new();
        let mut off = (PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize) + 32;
        while off + 32 <= data.len() {
            let pk = Pubkey::new_from_array(
                data[off..off + 32]
                    .try_into()
                    .map_err(|_| anyhow!("market rest pubkey slice"))?,
            );
            rest_pubkeys.push(pk);
            off += 32;
        }

        // The observed market layout is not 32-byte aligned (base_mint starts at offset 43), so
        // vault/accounts can appear at non-32-byte boundaries. Scan raw bytes for candidate
        // Pubkeys and resolve them in batches.
        let _embedded_pubkeys: HashSet<Pubkey> = rest_pubkeys.iter().copied().collect();
        let mut scanned_pubkeys: Vec<Pubkey> = Vec::new();
        if data.len() >= 32 {
            for i in 0..=(data.len() - 32) {
                let pk = Pubkey::new_from_array(
                    data[i..i + 32]
                        .try_into()
                        .map_err(|_| anyhow!("market scan pubkey slice"))?,
                );
                scanned_pubkeys.push(pk);
            }
        }
        scanned_pubkeys.sort();
        scanned_pubkeys.dedup();

        let mut all_candidates = rest_pubkeys.clone();
        all_candidates.extend(scanned_pubkeys);
        all_candidates.sort();
        all_candidates.dedup();

        let token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        let token_2022_program = Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")?;
        let associated_token_program =
            Pubkey::new_from_array(spl_associated_token_account::id().to_bytes());
        let system_program = Pubkey::from_str("11111111111111111111111111111111")?;
        let fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
        let mut token_accounts: Vec<TokenAccountMeta> = Vec::new();
        let mut non_token_pubkeys: Vec<Pubkey> = Vec::new();
        let mut program_owned_accounts: Vec<ProgramOwnedAccountMeta> = Vec::new();
        let mut fee_program_owned_accounts: Vec<ProgramOwnedAccountMeta> = Vec::new();

        const MULTI_ACCOUNTS_CHUNK: usize = 100;
        for chunk in all_candidates.chunks(MULTI_ACCOUNTS_CHUNK) {
            let accounts = self
                .rpc
                .rpc
                .get_multiple_accounts(chunk)
                .await
                .map_err(|e| anyhow!("get_multiple_accounts failed: {e}"))?;

            for (pk, acc_opt) in chunk.iter().copied().zip(accounts.into_iter()) {
                let Some(acc) = acc_opt else {
                    continue;
                };

                let acc_owner = acc.owner;
                let acc_data = acc.data;

                if acc_owner == token_program || acc_owner == token_2022_program {
                    if let Some((mint, token_owner)) =
                        Self::parse_spl_token_account_mint_and_owner(&acc_data)
                    {
                        let balance = Self::parse_spl_token_account_amount(&acc_data).unwrap_or(0);
                        token_accounts.push(TokenAccountMeta {
                            address: pk,
                            mint,
                            token_owner,
                            balance,
                        });
                        continue;
                    }
                }

                if acc_owner == pump_amm_program {
                    program_owned_accounts.push(ProgramOwnedAccountMeta {
                        address: pk,
                        data_len: acc_data.len(),
                    });
                    continue;
                }

                // fee_config must be owned by the Fee Program, not the AMM program
                if acc_owner == fee_program {
                    fee_program_owned_accounts.push(ProgramOwnedAccountMeta {
                        address: pk,
                        data_len: acc_data.len(),
                    });
                    continue;
                }

                if !acc.executable && pk != Pubkey::default() {
                    non_token_pubkeys.push(pk);
                }
            }
        }

        // Helper: try to find an authority whose token account for `mint` exists.
        // Prefer ATA; if none exists, fall back to an indexed getProgramAccounts lookup
        // (covers non-ATA fee recipient accounts).
        let find_authority_with_existing_token_account = |candidates: Vec<Pubkey>, mint: Pubkey| async move {
            for cand in candidates {
                // 1) Fast path: ATA exists.
                for tp in [token_program, token_2022_program] {
                    let ata = Self::derive_ata_with_program(cand, mint, tp);
                    let Some((ata_owner, ata_exec, ata_data)) =
                        self.rpc_get_account_owner_executable_and_data(ata).await?
                    else {
                        continue;
                    };
                    if ata_exec || (ata_owner != token_program && ata_owner != token_2022_program) {
                        continue;
                    }
                    let Some((ata_mint, ata_token_owner)) =
                        Self::parse_spl_token_account_mint_and_owner(&ata_data)
                    else {
                        continue;
                    };
                    if ata_mint == mint && ata_token_owner == cand {
                        return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((cand, ata)));
                    }
                }

                // FIX-32: ATA not found on-chain — derive and use anyway.
                // PumpSwap creates it via CreateIdempotent during swap.
                // No getTokenAccountsByOwner fallback (incompatible with restricted validator secondary indexes).
                if mint == expected_quote_mint {
                    let derived_ata = Self::derive_ata_with_program(cand, mint, token_program);
                    warn!(
                        candidate = %cand,
                        mint = %mint,
                        derived_ata = %derived_ata,
                        "pump_amm: ATA not on-chain, using derived address (PumpSwap will create via CreateIdempotent)"
                    );
                    return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((
                        cand,
                        derived_ata,
                    )));
                }
            }
            Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(None)
        };

        let mut base_token_accounts: Vec<TokenAccountMeta> = token_accounts
            .iter()
            .filter(|t| t.mint == base_mint)
            .cloned()
            .collect();
        let mut quote_token_accounts: Vec<TokenAccountMeta> = token_accounts
            .iter()
            .filter(|t| t.mint == quote_mint)
            .cloned()
            .collect();

        base_token_accounts.sort_by_key(|t| std::cmp::Reverse(t.balance));
        quote_token_accounts.sort_by_key(|t| std::cmp::Reverse(t.balance));

        let pool_base_vault = base_token_accounts
            .first()
            .map(|t| t.address)
            .ok_or_else(|| anyhow!("pump_amm market parse: no base vault token accounts"))?;
        let pool_quote_vault = quote_token_accounts
            .first()
            .map(|t| t.address)
            .ok_or_else(|| anyhow!("pump_amm market parse: no quote vault token accounts"))?;

        // Build a list of plausible authorities for fee/creator recipients.
        // These can appear as plain Pubkeys in the market account even when the corresponding
        // token accounts are not embedded.
        let mut authority_candidates: Vec<Pubkey> = non_token_pubkeys
            .iter()
            .copied()
            .filter(|pk| {
                *pk != Pubkey::default()
                    && *pk != pool_market
                    && *pk != global_config
                    && *pk != base_mint
                    && *pk != quote_mint
                    && *pk != pool_base_vault
                    && *pk != pool_quote_vault
                    && *pk != pump_amm_program
                    && *pk != token_program
                    && *pk != token_2022_program
                    && *pk != associated_token_program
                    && *pk != system_program
            })
            .collect();
        authority_candidates.sort();
        authority_candidates.dedup();

        // Protocol fee recipient: prefer an embedded quote token account; otherwise derive ATA.
        // Some PumpSwap markets do not embed the fee recipient TA in the market account.
        // In that case, fall back to scanning the global_config account for either:
        // - an embedded quote token account (best), or
        // - additional authority pubkeys (then derive ATA and validate existence).
        let (protocol_fee_recipient, protocol_fee_recipient_ta) = if let Some(t) =
            quote_token_accounts
                .iter()
                .find(|t| t.address != pool_quote_vault)
        {
            (t.token_owner, t.address)
        } else {
            let mut extra_authority_candidates: Vec<Pubkey> = Vec::new();
            let mut embedded_fee_ta_from_global: Option<(Pubkey, Pubkey, u64)> = None;

            if let Some((gc_owner, gc_exec, gc_data)) = self
                .rpc_get_account_owner_executable_and_data(global_config)
                .await?
            {
                if !gc_exec && gc_owner == pump_amm_program && gc_data.len() >= 32 {
                    // Scan the global_config raw bytes for candidate pubkeys.
                    let mut gc_pubkeys: Vec<Pubkey> = Vec::new();
                    for i in 0..=(gc_data.len().saturating_sub(32)) {
                        let pk = Pubkey::new_from_array(
                            gc_data[i..i + 32]
                                .try_into()
                                .map_err(|_| anyhow!("global_config scan pubkey slice"))?,
                        );
                        gc_pubkeys.push(pk);
                    }
                    gc_pubkeys.sort();
                    gc_pubkeys.dedup();

                    for chunk in gc_pubkeys.chunks(MULTI_ACCOUNTS_CHUNK) {
                        let accounts =
                            self.rpc
                                .rpc
                                .get_multiple_accounts(chunk)
                                .await
                                .map_err(|e| {
                                    anyhow!("get_multiple_accounts (global_config) failed: {e}")
                                })?;

                        for (pk, acc_opt) in chunk.iter().copied().zip(accounts.into_iter()) {
                            let Some(acc) = acc_opt else {
                                continue;
                            };
                            let acc_owner = acc.owner;
                            let acc_data = acc.data;

                            if acc_owner == token_program || acc_owner == token_2022_program {
                                if let Some((mint, token_owner)) =
                                    Self::parse_spl_token_account_mint_and_owner(&acc_data)
                                {
                                    if mint == quote_mint && pk != pool_quote_vault {
                                        let bal = Self::parse_spl_token_account_amount(&acc_data)
                                            .unwrap_or(0);
                                        match embedded_fee_ta_from_global {
                                            None => {
                                                embedded_fee_ta_from_global =
                                                    Some((token_owner, pk, bal));
                                            }
                                            Some((_prev_owner, _prev_ta, prev_bal)) => {
                                                // Heuristic: prefer the smaller balance (fee TA
                                                // tends to hold little vs pool vault).
                                                if bal < prev_bal {
                                                    embedded_fee_ta_from_global =
                                                        Some((token_owner, pk, bal));
                                                }
                                            }
                                        }
                                    }
                                }
                                continue;
                            }

                            // Keep any existing, non-token pubkeys as additional authority candidates.
                            if !acc.executable
                                && pk != Pubkey::default()
                                && pk != pool_market
                                && pk != global_config
                                && pk != base_mint
                                && pk != quote_mint
                                && pk != pool_base_vault
                                && pk != pool_quote_vault
                                && pk != pump_amm_program
                                && pk != token_program
                                && pk != token_2022_program
                                && pk != associated_token_program
                                && pk != system_program
                            {
                                extra_authority_candidates.push(pk);
                            }
                        }
                    }
                }
            }

            if let Some((owner, ta, _bal)) = embedded_fee_ta_from_global {
                (owner, ta)
            } else {
                // Retry ATA derivation with any extra authorities found in global_config.
                let mut combined = authority_candidates.clone();
                combined.extend(extra_authority_candidates);
                combined.sort();
                combined.dedup();

                if let Some((auth, ta)) =
                    find_authority_with_existing_token_account(combined.clone(), quote_mint).await?
                {
                    (auth, ta)
                } else if let Some((auth, ta)) =
                    // Some fee flows (notably `sell`) can accrue fees in the input mint.
                    // If the protocol fee recipient TA is not for WSOL, fall back to base mint.
                    find_authority_with_existing_token_account(
                        combined.clone(),
                        base_mint,
                    )
                    .await?
                {
                    (auth, ta)
                } else {
                    // CRITICAL FALLBACK: Use known PumpSwap protocol fee recipient when automatic
                    // discovery fails (e.g., when global_config doesn't exist or authority candidates
                    // are stale/deleted accounts). This is observed in many PumpSwap pools.
                    let fallback_recipient =
                        Pubkey::from_str(PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT)?;
                    if let Some((auth, ta)) = find_authority_with_existing_token_account(
                        vec![fallback_recipient],
                        quote_mint,
                    )
                    .await?
                    {
                        (auth, ta)
                    } else if let Some((auth, ta)) = find_authority_with_existing_token_account(
                        vec![fallback_recipient],
                        base_mint,
                    )
                    .await?
                    {
                        (auth, ta)
                    } else {
                        // Cannot construct swap instruction without protocol_fee_recipient_ta.
                        // Skip this pool rather than failing hard.
                        warn!(
                            pool = %pool_market,
                            "pump_amm market parse FAIL: no protocol fee recipient token account \
                             (global_config={global_config} tried_mints=[{quote_mint},{base_mint}] \
                             authority_candidates_count={} fallback={fallback_recipient})",
                            combined.len(),
                        );
                        return Ok(None);
                    }
                }
            }
        };

        // Creator vault: prefer an embedded second base token account; else canonical layout at
        // offset 211 (`creator_vault` PDA + WSOL ATA); else authority heuristics / legacy PDAs.
        let (coin_creator_vault_authority, coin_creator_vault_ata) = if let Some(t) =
            base_token_accounts
                .iter()
                .find(|t| t.address != pool_base_vault)
        {
            (t.token_owner, t.address)
        } else if data.len() >= PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32 {
            let creator_seed = Pubkey::new_from_array(
                data[PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET
                    ..PUMPFUN_AMM_MARKET_CREATOR_SEED_OFFSET + 32]
                    .try_into()
                    .map_err(|_| anyhow!("market creator_seed slice"))?,
            );
            if creator_seed != Pubkey::default() {
                let (auth, _) = Pubkey::find_program_address(
                    &[b"creator_vault", creator_seed.as_ref()],
                    &pump_amm_program,
                );
                // On-chain swaps use the creator fee vault as a WSOL (quote) token account for this authority.
                let ata = Self::derive_ata_with_program(auth, quote_mint, token_program);
                info!(
                    pool = %pool_market,
                    creator_seed = %creator_seed,
                    coin_creator_vault_authority = %auth,
                    coin_creator_vault_ata = %ata,
                    "pump_amm: creator vault from market offset 211 + creator_vault PDA (quote-mint ATA)"
                );
                (auth, ata)
            } else if let Some((auth, ta)) =
                find_authority_with_existing_token_account(authority_candidates.clone(), base_mint)
                    .await?
            {
                (auth, ta)
            } else if let Some((auth, ta)) =
                find_authority_with_existing_token_account(authority_candidates.clone(), quote_mint)
                    .await?
            {
                (auth, ta)
            } else {
                match self
                    .derive_existing_pda(
                        pump_amm_program,
                        &[
                            vec![
                                b"creator_vault_authority".to_vec(),
                                pool_market.to_bytes().to_vec(),
                            ],
                            vec![b"creator_vault".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"creator".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"vault_authority".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"token_creator".to_vec(), pool_market.to_bytes().to_vec()],
                        ],
                    )
                    .await?
                {
                    Some(derived_authority) => {
                        let derived_ata = Self::derive_ata(derived_authority, base_mint);
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            derived_ata = %derived_ata,
                            authority_candidates_count = authority_candidates.len(),
                            "pump_amm: creator vault ATA not on-chain; using derived address (FIX-32 parity)"
                        );
                        (derived_authority, derived_ata)
                    }
                    None => {
                        warn!(
                            pool = %pool_market,
                            base_mint = %base_mint,
                            "pump_amm market parse FAIL: no creator vault token account \
                             (no embedded ATA; offset 211 path unavailable; no ATA found; no valid PDA; \
                             authority_candidates_count={})",
                            authority_candidates.len()
                        );
                        return Ok(None);
                    }
                }
            }
        } else if let Some((auth, ta)) =
            find_authority_with_existing_token_account(authority_candidates.clone(), base_mint)
                .await?
        {
            (auth, ta)
        } else if let Some((auth, ta)) =
            find_authority_with_existing_token_account(authority_candidates.clone(), quote_mint)
                .await?
        {
            (auth, ta)
        } else {
            match self
                .derive_existing_pda(
                    pump_amm_program,
                    &[
                        vec![
                            b"creator_vault_authority".to_vec(),
                            pool_market.to_bytes().to_vec(),
                        ],
                        vec![b"creator_vault".to_vec(), pool_market.to_bytes().to_vec()],
                        vec![b"creator".to_vec(), pool_market.to_bytes().to_vec()],
                        vec![b"vault_authority".to_vec(), pool_market.to_bytes().to_vec()],
                        vec![b"token_creator".to_vec(), pool_market.to_bytes().to_vec()],
                    ],
                )
                .await?
            {
                Some(derived_authority) => {
                    let derived_ata = Self::derive_ata(derived_authority, base_mint);
                    warn!(
                        pool = %pool_market,
                        base_mint = %base_mint,
                        derived_ata = %derived_ata,
                        authority_candidates_count = authority_candidates.len(),
                        "pump_amm: creator vault ATA not on-chain; using derived address (FIX-32 parity)"
                    );
                    (derived_authority, derived_ata)
                }
                None => {
                    warn!(
                        pool = %pool_market,
                        base_mint = %base_mint,
                        "pump_amm market parse FAIL: no creator vault token account \
                         (no embedded ATA; no ATA found; no valid PDA; \
                         authority_candidates_count={})",
                        authority_candidates.len()
                    );
                    return Ok(None);
                }
            }
        };

        // Swap ix account #15 must be the `__event_authority` PDA (ConstraintSeeds). Do not infer
        // from market bytes; same pattern as `global_config`.
        let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);

        // Extract fee_config from fee-program owned accounts (CRITICAL: fee_config must be owned
        // by pfeeUxB6... Fee Program, not the AMM program).
        // Extract global_volume_accumulator from AMM-program owned accounts.
        let fee_config = if !fee_program_owned_accounts.is_empty() {
            // Prefer the first fee-program owned account found in market data
            fee_program_owned_accounts
                .first()
                .map(|m| m.address)
                .unwrap_or_default()
        } else {
            // Fallback: try deriving fee_config PDA from Fee Program
            self.derive_existing_pda(
                fee_program,
                &[
                    vec![b"fee_config".to_vec(), global_config.to_bytes().to_vec()],
                    vec![b"fee_config".to_vec(), pool_market.to_bytes().to_vec()],
                    vec![b"fee_config".to_vec()],
                    vec![b"fees".to_vec(), global_config.to_bytes().to_vec()],
                    vec![b"fees".to_vec(), pool_market.to_bytes().to_vec()],
                ],
            )
            .await?
            .unwrap_or_default()
        };

        let global_volume_accumulator = if !program_owned_accounts.is_empty() {
            // global_volume_accumulator is owned by the AMM program
            // Heuristic: prefer the larger account (volume accumulator tends to be bigger)
            let mut sorted = program_owned_accounts.clone();
            sorted.sort_by_key(|m| m.data_len);
            sorted.last().map(|m| m.address).unwrap_or_default()
        } else {
            // Fallback: try deriving from AMM program (singleton first — matches on-chain PumpSwap).
            self.derive_existing_pda(
                pump_amm_program,
                &[
                    vec![b"global_volume_accumulator".to_vec()],
                    vec![
                        b"global_volume_accumulator".to_vec(),
                        global_config.to_bytes().to_vec(),
                    ],
                    vec![
                        b"global_volume_accumulator".to_vec(),
                        pool_market.to_bytes().to_vec(),
                    ],
                    vec![
                        b"volume_accumulator".to_vec(),
                        global_config.to_bytes().to_vec(),
                    ],
                    vec![
                        b"volume_accumulator".to_vec(),
                        pool_market.to_bytes().to_vec(),
                    ],
                ],
            )
            .await?
            .unwrap_or_default()
        };

        // Same global fee_config pubkey for all PumpSwap pools (see build_swap_ix_from_pool_accounts).
        let fee_config = if fee_config == Pubkey::default() {
            Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?
        } else {
            fee_config
        };

        if fee_config == Pubkey::default() || global_volume_accumulator == Pubkey::default() {
            warn!(
                pool = %pool_market,
                fee_config_default = (fee_config == Pubkey::default()),
                vol_accum_default = (global_volume_accumulator == Pubkey::default()),
                "pump_amm market parse FAIL: fee_config or global_volume_accumulator is default"
            );
            return Ok(None);
        }

        // CRITICAL FIX: If protocol_fee_recipient is still Pubkey::default() (could not be
        // discovered from market/global_config), derive it from Fee Program PDA seeds.
        // Observed pattern: protocol_fee_recipient is a PDA owned by Fee Program with seeds
        // like [b"protocol_fee", index] where index can vary (e.g., 8).
        let (final_protocol_fee_recipient, final_protocol_fee_recipient_ta) =
            if protocol_fee_recipient == Pubkey::default()
                || protocol_fee_recipient_ta == Pubkey::default()
            {
                // Try deriving protocol_fee_recipient from Fee Program with common seed patterns
                let derived_recipient = match self
                    .derive_existing_pda(
                        fee_program,
                        &[
                            // Common patterns observed in PumpSwap pools
                            vec![b"protocol_fee".to_vec(), vec![8]],
                            vec![b"protocol_fee".to_vec(), vec![0]],
                            vec![b"protocol_fee".to_vec(), pool_market.to_bytes().to_vec()],
                            vec![b"protocol_fee".to_vec(), global_config.to_bytes().to_vec()],
                            vec![b"protocol_fee".to_vec(), fee_config.to_bytes().to_vec()],
                            vec![b"fee_recipient".to_vec()],
                            vec![b"protocol".to_vec()],
                        ],
                    )
                    .await?
                {
                    Some(v) => v,
                    None => {
                        warn!(
                            pool = %pool_market,
                            "pump_amm market parse FAIL: could not derive protocol_fee_recipient PDA \
                             (fee_program={fee_program} fee_config={fee_config})"
                        );
                        return Ok(None);
                    }
                };

                // Derive ATA for derived_recipient
                let derived_ta = Self::derive_ata(derived_recipient, quote_mint);

                // Verify the ATA exists
                match self
                    .rpc_get_account_owner_and_executable(derived_ta)
                    .await?
                {
                    Some(_) => (derived_recipient, derived_ta),
                    None => {
                        warn!(
                            pool = %pool_market,
                            "pump_amm market parse FAIL: derived protocol_fee_recipient_ta does not exist \
                             (recipient={derived_recipient} ta={derived_ta})"
                        );
                        return Ok(None);
                    }
                }
            } else {
                (protocol_fee_recipient, protocol_fee_recipient_ta)
            };

        Ok(Some(PumpAmmPoolStatic {
            pool_market,
            global_config,
            base_mint,
            quote_mint,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient: final_protocol_fee_recipient,
            protocol_fee_recipient_ta: final_protocol_fee_recipient_ta,
            event_authority,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            global_volume_accumulator,
            fee_config,
            fee_program,
        }))
    }

    /// Discover PumpSwap pool market addresses by base_mint via getProgramAccounts RPC.
    ///
    /// COLD PATH ONLY. Uses RPC (getProgramAccounts). Never call from hot path.
    async fn discover_pool_markets_via_program_accounts(
        &self,
        base_mint: Pubkey,
    ) -> Result<Vec<Pubkey>> {
        use solana_commitment_config::CommitmentConfig;

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

        let filters = vec![
            RpcFilterType::Memcmp(Memcmp::new(
                PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize,
                MemcmpEncodedBytes::Base58(base_mint.to_string()),
            )),
            RpcFilterType::Memcmp(Memcmp::new(
                PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize,
                MemcmpEncodedBytes::Base58(WSOL_MINT.to_string()),
            )),
        ];

        let config = RpcProgramAccountsConfig {
            filters: Some(filters),
            account_config: RpcAccountInfoConfig {
                encoding: Some(UiAccountEncoding::Base64),
                commitment: Some(CommitmentConfig::confirmed()),
                ..Default::default()
            },
            ..Default::default()
        };

        let accounts = self
            .rpc
            .get_program_accounts_with_config_retry(&program_id, config)
            .await
            .map_err(|e| anyhow!("getProgramAccounts failed: {e}"))?;

        let mut out: Vec<Pubkey> = accounts.into_iter().map(|(pk, _)| pk).collect();
        out.sort();
        out.dedup();
        Ok(out)
    }

    async fn discover_pool_static_via_tx_history_market_only(
        &self,
        pool_market: Pubkey,
        base_mint: Pubkey,
    ) -> Result<Option<PumpAmmPoolStatic>> {
        // Minimal, bounded tx-history fallback.
        // Some Pump AMM market accounts do not embed the fee/creator token accounts we need to
        // build a full swap ix. In that case, scan only the market's txs to find a successful
        // swap and extract the canonical account set from the on-chain transaction.

        info!(
            "pump_amm TX-history: starting getSignaturesForAddress for market={} base_mint={} limit=200",
            pool_market, base_mint
        );

        let sigs = self
            .rpc
            .get_signatures_for_address(&pool_market, Some(200))
            .await
            .map_err(|e| anyhow!("getSignaturesForAddress failed: {e}"))?;

        info!(
            "pump_amm TX-history: found {} signatures for market={} base_mint={}, starting transaction scan...",
            sigs.len(), pool_market, base_mint
        );

        if sigs.is_empty() {
            info!(
                "pump_amm TX-history: no signatures found for market={}, returning None",
                pool_market
            );
            return Ok(None);
        }

        const MAX_TX_FETCHES: usize = 200;
        let mut fetched = 0usize;
        let mut scanned_tx_count = 0usize;
        const DEBUG_REF_TX: &str = "3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY";

        for s in &sigs {
            if fetched >= MAX_TX_FETCHES {
                break;
            }
            if s.err.is_some() {
                continue;
            }
            let sig = s.signature.to_string();

            if sig == DEBUG_REF_TX {
                info!(
                    "pump_amm TX-history: FOUND reference TX in signature list! sig={}",
                    sig
                );
            }

            fetched += 1;

            if fetched % 20 == 0 {
                info!(
                    "pump_amm TX-history: scanned {}/{} transactions for market={}...",
                    fetched,
                    sigs.len(),
                    pool_market
                );
            }

            let tx_v = self.fetch_tx_as_value(&sig).await?;

            let msg = match tx_v
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
            {
                Some(v) => v,
                None => continue,
            };
            let meta = tx_v
                .get("result")
                .and_then(|r| r.get("meta"))
                .unwrap_or(&Value::Null);

            let mut account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            Self::extend_with_loaded_addresses(&mut account_keys, meta);

            scanned_tx_count += 1;

            let is_ref_tx = sig == DEBUG_REF_TX;
            if is_ref_tx {
                info!(
                    "pump_amm TX-history: processing reference TX sig={} account_keys_count={}",
                    sig,
                    account_keys.len()
                );
            }

            for ix in Self::collect_all_instructions(msg, meta) {
                let program_id_index = match ix.get("programIdIndex").and_then(|v| v.as_u64()) {
                    Some(v) => v as usize,
                    None => continue,
                };
                let program_id = match account_keys.get(program_id_index) {
                    Some(v) => v,
                    None => continue,
                };
                if program_id != PUMPFUN_AMM_PROGRAM_ID {
                    if is_ref_tx {
                        info!(
                            "pump_amm TX-history: reference TX ix program_id={} (not PumpSwap AMM, skipping)",
                            program_id
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX has PumpSwap AMM instruction!");
                }

                let accounts: Vec<usize> = match ix.get("accounts").and_then(|v| v.as_array()) {
                    Some(a) => a
                        .iter()
                        .filter_map(|v| v.as_u64().map(|x| x as usize))
                        .collect(),
                    None => continue,
                };
                // PumpSwap AMM swap instructions have 21 accounts (not 23 as originally assumed).
                if accounts.len() != 21 {
                    // Log first mismatch or reference TX
                    if scanned_tx_count == 1 || is_ref_tx {
                        info!(
                            "pump_amm TX-history: account count mismatch sig={} expected=21 actual={}",
                            sig, accounts.len()
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX account count OK (21)");
                }

                // Ensure we're extracting accounts for the market we scanned.
                let pool_market_ix = match account_keys.get(accounts[0]) {
                    Some(v) => v,
                    None => {
                        if is_ref_tx {
                            info!("pump_amm TX-history: reference TX accounts[0] out of bounds");
                        }
                        continue;
                    }
                };
                if Pubkey::from_str(pool_market_ix).ok() != Some(pool_market) {
                    // Log first mismatch or reference TX
                    if scanned_tx_count == 1 || is_ref_tx {
                        info!(
                            "pump_amm TX-history: market mismatch sig={} expected={} actual={}",
                            sig, pool_market, pool_market_ix
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX market match OK");
                }

                // Base mint is accounts[3] for pump_amm v1.
                let base_mint_ix = match account_keys.get(accounts[3]) {
                    Some(v) => v,
                    None => {
                        if is_ref_tx {
                            info!("pump_amm TX-history: reference TX accounts[3] out of bounds");
                        }
                        continue;
                    }
                };
                if Pubkey::from_str(base_mint_ix).ok() != Some(base_mint) {
                    // Log first mismatch or reference TX
                    if scanned_tx_count == 1 || is_ref_tx {
                        info!(
                            "pump_amm TX-history: base_mint mismatch sig={} expected={} actual={}",
                            sig, base_mint, base_mint_ix
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX base_mint match OK, proceeding to build pool...");
                }

                // Build the subset we store as pool static.
                // Account mapping from actual PumpSwap AMM swap instruction (23 accounts total):
                // #0 = pool_market, #2 = global_config, #3 = base_mint, #4 = quote_mint,
                // #7 = pool_base_vault, #8 = pool_quote_vault,
                // #9 = protocol_fee_recipient, #10 = protocol_fee_recipient_ta,
                // #15 = event_authority, #17 = coin_creator_vault_ata,
                // #18 = coin_creator_vault_authority, #16 = global_volume_accumulator,
                // #19 = fee_config, #20 = fee_program
                let pool = PumpAmmPoolStatic {
                    pool_market: Pubkey::from_str(&account_keys[accounts[0]])?,
                    global_config: Pubkey::from_str(&account_keys[accounts[2]])?,
                    base_mint: Pubkey::from_str(&account_keys[accounts[3]])?,
                    quote_mint: Pubkey::from_str(&account_keys[accounts[4]])?,
                    pool_base_vault: Pubkey::from_str(&account_keys[accounts[7]])?,
                    pool_quote_vault: Pubkey::from_str(&account_keys[accounts[8]])?,
                    protocol_fee_recipient: Pubkey::from_str(&account_keys[accounts[9]])?,
                    protocol_fee_recipient_ta: Pubkey::from_str(&account_keys[accounts[10]])?,
                    event_authority: Pubkey::from_str(&account_keys[accounts[15]])?,
                    coin_creator_vault_ata: Pubkey::from_str(&account_keys[accounts[17]])?,
                    coin_creator_vault_authority: Pubkey::from_str(&account_keys[accounts[18]])?,
                    global_volume_accumulator: Pubkey::from_str(&account_keys[accounts[16]])?,
                    fee_config: Pubkey::from_str(&account_keys[accounts[19]])?,
                    fee_program: Pubkey::from_str(&account_keys[accounts[20]])?,
                };

                // Fee guardrails (same as the broader scanner).
                let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
                if pool.fee_program != expected_fee_program {
                    if is_ref_tx {
                        info!(
                            "pump_amm TX-history: reference TX fee_program mismatch expected={} actual={}",
                            expected_fee_program, pool.fee_program
                        );
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX fee_program OK, checking fee_config owner...");
                }

                // CRITICAL: fee_config must be owned by the Fee Program, not the AMM Program!
                // This matches the fix in discover_pool_markets_via_program_accounts (lines 763-818).
                let Some((fee_owner, fee_executable)) = self
                    .rpc_get_account_owner_and_executable(pool.fee_config)
                    .await?
                else {
                    if is_ref_tx {
                        info!("pump_amm TX-history: reference TX fee_config account not found");
                    }
                    continue;
                };

                if is_ref_tx {
                    info!(
                        "pump_amm TX-history: reference TX fee_config owner={} executable={} (expected owner={} Fee Program)",
                        fee_owner, fee_executable, expected_fee_program
                    );
                }

                // fee_config must be owned by Fee Program and not be executable
                if fee_executable || fee_owner != expected_fee_program {
                    if is_ref_tx {
                        info!("pump_amm TX-history: reference TX fee_config owner check FAILED");
                    }
                    continue;
                }

                if is_ref_tx {
                    info!("pump_amm TX-history: reference TX SUCCESS! Returning pool...");
                }

                return Ok(Some(pool));
            }
        }

        info!(
            "pump_amm TX-history: scanned {} transactions, no matching swap found for market={} base_mint={}",
            scanned_tx_count, pool_market, base_mint
        );
        Ok(None)
    }

    /// COLD PATH ONLY — RPC fallback when LivePoolCache misses. Never called in Hot Path. P3 #12.
    async fn discover_pool_static(&self, base_mint: Pubkey) -> Result<Option<PumpAmmPoolStatic>> {
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

        // FIX-23: GEYSER-FIRST — Construct PumpAmmPoolStatic from LivePoolCache before any RPC.
        // The cache contains all 14 pool_accounts from DexPoolAccounts events (parsed from
        // verified on-chain swap txs). This eliminates getProgramAccounts + getMultipleAccounts
        // RPC calls (~500-3000ms) in the hot path.
        if let Some(ref cache) = self.live_pool_cache {
            if let Some(accounts) = cache.get_pump_amm_pool_accounts_by_base_mint(&base_mint) {
                if accounts.len() >= 14 {
                    let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                    let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
                    if accounts[1] != global_config {
                        warn!(
                            cached = %accounts[1],
                            expected = %global_config,
                            "pump_amm: cache pool_accounts[1] != canonical global_config; using canonical"
                        );
                    }
                    let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);
                    if accounts[8] != event_authority {
                        warn!(
                            cached = %accounts[8],
                            expected = %event_authority,
                            "pump_amm: cache pool_accounts[8] != canonical event_authority; using canonical"
                        );
                    }
                    let pool = PumpAmmPoolStatic {
                        pool_market: accounts[0],
                        global_config,
                        base_mint: accounts[2],
                        quote_mint: accounts[3],
                        pool_base_vault: accounts[4],
                        pool_quote_vault: accounts[5],
                        protocol_fee_recipient: accounts[6],
                        protocol_fee_recipient_ta: accounts[7],
                        event_authority,
                        coin_creator_vault_ata: accounts[9],
                        coin_creator_vault_authority: accounts[10],
                        global_volume_accumulator: accounts[11],
                        fee_config: accounts[12],
                        fee_program: accounts[13],
                    };
                    // Cache internally for build_swap_ix() (sync path)
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    info!(
                        base_mint = %base_mint,
                        pool_market = %pool.pool_market,
                        "pump_amm: PumpAmmPoolStatic from LivePoolCache (ZERO RPC discovery)"
                    );
                    return Ok(Some(pool));
                }
            }
        }

        // FIX-31: FAST PATH — If the LivePoolCache has the pool address (even without
        // pool_accounts), use a single getAccount call to parse the pool account data.
        // This avoids the slow getProgramAccounts scan that routinely times out (>10s).
        if let Some(ref cache) = self.live_pool_cache {
            if let Some(pool_address) = cache.get_pump_amm_pool_address_by_base_mint(&base_mint) {
                info!(
                    base_mint = %base_mint,
                    pool = %pool_address,
                    "pump_amm: LivePoolCache has pool address but no pool_accounts, trying direct getAccount"
                );
                match self
                    .try_parse_pool_static_from_market_account(pool_address, base_mint)
                    .await
                {
                    Ok(Some(pool)) => {
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(pool.pool_market, base_mint);
                        info!(
                            base_mint = %base_mint,
                            pool_market = %pool.pool_market,
                            "pump_amm: PumpAmmPoolStatic from direct getAccount (fast path)"
                        );
                        return Ok(Some(pool));
                    }
                    Ok(None) => {
                        warn!(
                            base_mint = %base_mint,
                            pool = %pool_address,
                            "pump_amm: cached pool address parse returned None; refusing getProgramAccounts (no unbounded scan)"
                        );
                        return Err(anyhow!(
                            "pump_amm: LivePoolCache pool address present but market parse returned no usable pool (base_mint={}, pool={}); refusing unbounded RPC discovery",
                            base_mint,
                            pool_address
                        ));
                    }
                    Err(e) => {
                        warn!(
                            base_mint = %base_mint,
                            pool = %pool_address,
                            error = %e,
                            "pump_amm: cached pool address parse failed; refusing getProgramAccounts (no unbounded scan)"
                        );
                        return Err(e.context(format!(
                            "pump_amm: cached pool address parse error (base_mint={}, pool={}); refusing unbounded RPC discovery",
                            base_mint, pool_address
                        )));
                    }
                }
            }
        }

        // Avoid concurrent discovery attempts for the same base mint.
        // This significantly reduces RPC rate-limits when `parallel_exits` is enabled.
        let _guard = self.discovery_lock.lock().await;
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

        // RPC FALLBACK: LivePoolCache miss (new pool not yet indexed by Geyser, or cold start
        // before PoolCacheUpdate events arrive). This is the cold path — acceptable per architecture.
        warn!(
            base_mint = %base_mint,
            "pump_amm: LivePoolCache miss for pool discovery, falling back to RPC"
        );

        let mut discovery_err: Option<anyhow::Error> = None;
        let markets = match self
            .discover_pool_markets_via_program_accounts(base_mint)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                discovery_err = Some(e);
                Vec::new()
            }
        };

        // Fast path: if we can locate the pool market via program-accounts lookup, attempt to
        // build the full static account set by parsing on-chain state + deriving PDAs. This avoids
        // relying on tx-history (some pools can exist with no successful swaps yet).
        let mut market_parse_err: Option<anyhow::Error> = None;
        for m in &markets {
            match self
                .try_parse_pool_static_from_market_account(*m, base_mint)
                .await
            {
                Ok(Some(pool)) => {
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    return Ok(Some(pool));
                }
                Ok(None) => {}
                Err(e) => {
                    market_parse_err = Some(anyhow!("{e:#}").context(format!(
                        "pump_amm market parse failed market={m} base_mint={base_mint}"
                    )));
                }
            }
        }

        // If we found market accounts but couldn't parse a usable static account set, do a narrow
        // tx-history fallback by scanning only the market address. This is far cheaper than the
        // legacy scan across multiple addresses/pages.
        if let Some(m) = markets.first().copied() {
            info!(
                "pump_amm attempting TX-history fallback for market {} base_mint {}",
                m, base_mint
            );

            match self
                .discover_pool_static_via_tx_history_market_only(m, base_mint)
                .await
            {
                Ok(Some(pool)) => {
                    info!(
                        "pump_amm TX-history fallback SUCCESS for market {} base_mint {}",
                        m, base_mint
                    );
                    self.pools_by_base.insert(base_mint, pool.clone());
                    self.pools_by_market.insert(pool.pool_market, base_mint);
                    return Ok(Some(pool));
                }
                Ok(None) => {
                    warn!(
                        "pump_amm TX-history fallback returned None for market {} base_mint {}",
                        m, base_mint
                    );
                }
                Err(e) => {
                    warn!(
                        "pump_amm TX-history fallback ERROR for market {} base_mint {}: {:#}",
                        m, base_mint, e
                    );
                }
            }

            if let Some(e) = market_parse_err {
                return Err(e);
            }

            return Err(anyhow!(
                "pump_amm market(s) found but could not build pool static base_mint={base_mint} markets={markets:?}"
            ));
        }

        // TX-based discovery: prefer scanning the pool market(s) (stable) over the mint address.
        // On pruned RPC, mint-address history can be missing; program-accounts lookup + market
        // history tends to be more reliable.
        let mut scan_addresses: Vec<String> = Vec::new();
        for m in &markets {
            scan_addresses.push(m.to_string());
        }
        scan_addresses.push(base_mint.to_string());
        scan_addresses.sort();
        scan_addresses.dedup();

        for addr in scan_addresses {
            // We paginate because the newest signatures can be dominated by our own failed
            // liquidation attempts (which we intentionally skip), and the first successful
            // PumpSwap trades can be older than the initial page on busy pools.
            // IMPORTANT: tx-history calls are expensive; cap requests to avoid rate-limits.
            const SIG_PAGE_SIZE: usize = 200;
            const SIG_MAX_PAGES: usize = 100; // up to ~20k signatures
            const SIG_TX_PER_PAGE: usize = 40; // cap getTransaction calls per page
            let addr_pk = match Pubkey::from_str(&addr) {
                Ok(p) => p,
                Err(_) => continue,
            };
            let mut before: Option<Signature> = None;

            for _page in 0..SIG_MAX_PAGES {
                let sigs = match self
                    .rpc
                    .get_signatures_for_address_with_config(
                        &addr_pk,
                        Some(SIG_PAGE_SIZE),
                        before.as_ref(),
                        None,
                    )
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        discovery_err = Some(anyhow!("{e}"));
                        break;
                    }
                };
                if sigs.is_empty() {
                    break;
                }

                // Update pagination cursor
                before = sigs.last().and_then(|s| s.signature.parse().ok());

                let page_len = sigs.len();
                let take_n = SIG_TX_PER_PAGE.min(page_len);
                let step = if take_n <= 1 {
                    1
                } else {
                    (page_len - 1) / (take_n - 1)
                };

                for i in 0..take_n {
                    let idx = (i * step).min(page_len.saturating_sub(1));
                    let s = &sigs[idx];
                    if s.err.is_some() {
                        continue;
                    }
                    let sig = s.signature.to_string();

                    let tx_v = match self.fetch_tx_as_value(&sig).await {
                        Ok(v) => v,
                        Err(e) => {
                            discovery_err = Some(anyhow!("{e}"));
                            break;
                        }
                    };

                    let msg = match tx_v
                        .get("result")
                        .and_then(|r| r.get("transaction"))
                        .and_then(|t| t.get("message"))
                    {
                        Some(v) => v,
                        None => continue,
                    };

                    let meta = tx_v
                        .get("result")
                        .and_then(|r| r.get("meta"))
                        .unwrap_or(&Value::Null);

                    let mut account_keys = match Self::parse_account_keys(msg) {
                        Ok(v) => v,
                        Err(_) => continue,
                    };
                    Self::extend_with_loaded_addresses(&mut account_keys, meta);

                    for ix in Self::collect_all_instructions(msg, meta) {
                        let program_id_index =
                            match ix.get("programIdIndex").and_then(|v| v.as_u64()) {
                                Some(v) => v as usize,
                                None => continue,
                            };
                        let program_id = match account_keys.get(program_id_index) {
                            Some(v) => v,
                            None => continue,
                        };
                        if program_id != PUMPFUN_AMM_PROGRAM_ID {
                            continue;
                        }

                        let accounts: Vec<usize> =
                            match ix.get("accounts").and_then(|v| v.as_array()) {
                                Some(a) => a
                                    .iter()
                                    .filter_map(|v| v.as_u64().map(|x| x as usize))
                                    .collect(),
                                None => continue,
                            };
                        // PumpSwap AMM swap instructions have 21 accounts (not 23 as originally assumed).
                        if accounts.len() != 21 {
                            continue;
                        }

                        let base_mint_ix = match account_keys.get(accounts[3]) {
                            Some(v) => v,
                            None => continue,
                        };
                        let quote_mint_ix = match account_keys.get(accounts[4]) {
                            Some(v) => v,
                            None => continue,
                        };
                        if base_mint_ix != base_mint.to_string().as_str() {
                            continue;
                        }
                        if quote_mint_ix != WSOL_MINT {
                            continue;
                        }

                        let pool = PumpAmmPoolStatic {
                            pool_market: Pubkey::from_str(&account_keys[accounts[0]])?,
                            global_config: Pubkey::from_str(&account_keys[accounts[2]])?,
                            base_mint,
                            quote_mint: Pubkey::from_str(WSOL_MINT)?,
                            pool_base_vault: Pubkey::from_str(&account_keys[accounts[7]])?,
                            pool_quote_vault: Pubkey::from_str(&account_keys[accounts[8]])?,
                            protocol_fee_recipient: Pubkey::from_str(&account_keys[accounts[9]])?,
                            protocol_fee_recipient_ta: Pubkey::from_str(
                                &account_keys[accounts[10]],
                            )?,
                            event_authority: Pubkey::from_str(&account_keys[accounts[15]])?,
                            coin_creator_vault_ata: Pubkey::from_str(&account_keys[accounts[17]])?,
                            coin_creator_vault_authority: Pubkey::from_str(
                                &account_keys[accounts[18]],
                            )?,
                            global_volume_accumulator: Pubkey::from_str(
                                &account_keys[accounts[19]],
                            )?,
                            fee_config: Pubkey::default(),
                            fee_program: Pubkey::default(),
                        };

                        // Deterministic mapping: our Geyser parser and observed on-chain swaps agree that
                        // PumpSwap v1 uses fixed indices.
                        // - fee_config: accounts[21]
                        // - fee_program: accounts[22]
                        let fee_config = Pubkey::from_str(&account_keys[accounts[21]])?;
                        let fee_program = Pubkey::from_str(&account_keys[accounts[22]])?;

                        // Guardrails: fee_config must be owned by pump_amm (Anchor constraint), and the
                        // fee program is expected to be pfee.
                        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
                        let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
                        if fee_program != expected_fee_program {
                            continue;
                        }
                        let Some((fee_owner, fee_executable)) = self
                            .rpc_get_account_owner_and_executable(fee_config)
                            .await?
                        else {
                            continue;
                        };
                        if fee_executable || fee_owner != pump_amm_program {
                            continue;
                        }

                        let mut pool = pool;
                        pool.fee_program = fee_program;
                        pool.fee_config = fee_config;

                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(pool.pool_market, base_mint);
                        return Ok(Some(pool));
                    }
                }

                if discovery_err.is_some() {
                    break;
                }

                // Small delay between pages to reduce rate-limit pressure.
                sleep(Duration::from_millis(200)).await;

                if discovery_err.is_some() {
                    break;
                }
            }

            if discovery_err.is_some() {
                break;
            }
        }

        if let Some(e) = discovery_err {
            return Err(anyhow!(e).context("pump_amm pool discovery failed"));
        }

        Ok(None)
    }

    async fn discover_user_accounts(
        &self,
        pool_market: Pubkey,
        base_mint: Pubkey,
        user: Pubkey,
    ) -> Result<Option<PumpAmmUserAccounts>> {
        if let Some(v) = self.user_accounts.get(&(pool_market, user)) {
            return Ok(Some(v.clone()));
        }

        // Scan transactions of the user for a Pump.fun AMM ix on this pool.
        // Scan deeper because recent txs may be unrelated (or failed).
        let sigs = self
            .rpc
            .get_signatures_for_address(&user, Some(500))
            .await
            .map_err(|e| anyhow!("getSignaturesForAddress failed: {e}"))?;

        for s in &sigs {
            if s.err.is_some() {
                continue;
            }
            let sig = s.signature.to_string();

            let tx_v = match self.fetch_tx_as_value(&sig).await {
                Ok(v) => v,
                Err(_) => continue,
            };

            let msg = match tx_v
                .get("result")
                .and_then(|r| r.get("transaction"))
                .and_then(|t| t.get("message"))
            {
                Some(v) => v,
                None => continue,
            };

            let meta = tx_v
                .get("result")
                .and_then(|r| r.get("meta"))
                .unwrap_or(&Value::Null);

            let mut account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            Self::extend_with_loaded_addresses(&mut account_keys, meta);

            for ix in Self::collect_all_instructions(msg, meta) {
                let program_id_index = match ix.get("programIdIndex").and_then(|v| v.as_u64()) {
                    Some(v) => v as usize,
                    None => continue,
                };
                let program_id = match account_keys.get(program_id_index) {
                    Some(v) => v,
                    None => continue,
                };
                if program_id != PUMPFUN_AMM_PROGRAM_ID {
                    continue;
                }

                let accounts: Vec<usize> = match ix.get("accounts").and_then(|v| v.as_array()) {
                    Some(a) => a
                        .iter()
                        .filter_map(|v| v.as_u64().map(|x| x as usize))
                        .collect(),
                    None => continue,
                };
                // PumpSwap AMM swap instructions have 21 accounts (not 23 as originally assumed).
                if accounts.len() != 21 {
                    continue;
                }

                let pool_ix = Pubkey::from_str(&account_keys[accounts[0]])?;
                if pool_ix != pool_market {
                    continue;
                }
                let base_ix = Pubkey::from_str(&account_keys[accounts[3]])?;
                if base_ix != base_mint {
                    continue;
                }

                let ua = PumpAmmUserAccounts {
                    user_base_ta: Pubkey::from_str(&account_keys[accounts[5]])?,
                    user_quote_ta: Pubkey::from_str(&account_keys[accounts[6]])?,
                    user_volume_accumulator: Pubkey::from_str(&account_keys[accounts[20]])?,
                };
                self.user_accounts.insert((pool_market, user), ua.clone());
                return Ok(Some(ua));
            }
        }

        // No prior tx found for this user/pool. Fall back to deterministic ATAs and a derived
        // user-volume PDA. This matches the on-chain account layout we observed, and allows new
        // wallets to trade without requiring historical lookups.
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let ua = PumpAmmUserAccounts {
            user_base_ta: Self::derive_ata(user, base_mint),
            user_quote_ta: Self::derive_ata(user, Pubkey::from_str(WSOL_MINT)?),
            user_volume_accumulator: Self::derive_user_volume_accumulator(
                program_id,
                pool_market,
                user,
            ),
        };
        self.user_accounts.insert((pool_market, user), ua.clone());
        Ok(Some(ua))
    }

    fn parse_account_keys(msg: &Value) -> Result<Vec<String>> {
        let keys = msg
            .get("accountKeys")
            .and_then(|v| v.as_array())
            .ok_or_else(|| anyhow!("missing message.accountKeys"))?;
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(s) = k.as_str() {
                out.push(s.to_string());
            } else if let Some(obj) = k.as_object() {
                let s = obj
                    .get("pubkey")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| anyhow!("accountKeys element missing pubkey"))?;
                out.push(s.to_string());
            } else {
                return Err(anyhow!("unexpected accountKeys element: {k}"));
            }
        }
        Ok(out)
    }

    fn extend_with_loaded_addresses(out: &mut Vec<String>, meta: &Value) {
        let Some(loaded) = meta.get("loadedAddresses") else {
            return;
        };

        if let Some(w) = loaded.get("writable").and_then(|v| v.as_array()) {
            for k in w.iter().filter_map(|v| v.as_str()) {
                out.push(k.to_string());
            }
        }

        if let Some(r) = loaded.get("readonly").and_then(|v| v.as_array()) {
            for k in r.iter().filter_map(|v| v.as_str()) {
                out.push(k.to_string());
            }
        }
    }

    fn collect_all_instructions<'a>(msg: &'a Value, meta: &'a Value) -> Vec<&'a Value> {
        let mut out: Vec<&'a Value> = Vec::new();

        if let Some(ixs) = msg.get("instructions").and_then(|v| v.as_array()) {
            out.extend(ixs.iter());
        }

        if let Some(inner) = meta.get("innerInstructions").and_then(|v| v.as_array()) {
            for entry in inner {
                if let Some(ixs) = entry.get("instructions").and_then(|v| v.as_array()) {
                    out.extend(ixs.iter());
                }
            }
        }

        out
    }

    async fn get_vault_amount(&self, ta: Pubkey) -> Result<u64> {
        let acc = self
            .rpc
            .get_account_opt_retry(&ta)
            .await
            .map_err(|e| anyhow!("get_account failed: {e}"))?
            .ok_or_else(|| anyhow!("token account {ta} not found"))?;
        Self::parse_spl_token_account_amount(&acc.data)
            .ok_or_else(|| anyhow!("invalid token account data for {ta}"))
    }

    /// Cold Path only: authoritative SPL token balances for PumpSwap pool vaults.
    ///
    /// `pool_accounts` must follow the canonical layout where `[4]` is `pool_base_vault` and
    /// `[5]` is `pool_quote_vault` (same as `PumpAmmState::pool_base_token_account` / quote).
    /// Used after successful I-24d discovery so JetStream/SLAVE caches receive non-degenerate
    /// reserves without local healing in execution-engine.
    pub async fn fetch_pump_amm_vault_reserves(
        &self,
        pool_accounts: &[Pubkey],
    ) -> Result<(u64, u64)> {
        if pool_accounts.len() < 6 {
            return Err(anyhow!(
                "pool_accounts len {} < 6 (cannot read vault pubkeys)",
                pool_accounts.len()
            ));
        }
        let base_vault = pool_accounts[4];
        let quote_vault = pool_accounts[5];
        let (base_res, quote_res) = tokio::try_join!(
            self.get_vault_amount(base_vault),
            self.get_vault_amount(quote_vault),
        )?;
        Ok((base_res, quote_res))
    }

    fn quote_cp(
        &self,
        amount_in: u64,
        in_reserve: u128,
        out_reserve: u128,
        fee_bps: u32,
    ) -> (u64, u32) {
        if in_reserve == 0 || out_reserve == 0 {
            return (0, 0);
        }
        let amt_in_post_fee = (amount_in as u128)
            .saturating_mul((10_000u32.saturating_sub(fee_bps)) as u128)
            / 10_000u128;
        if amt_in_post_fee == 0 {
            return (0, 0);
        }
        let out = (amt_in_post_fee.saturating_mul(out_reserve))
            / (in_reserve.saturating_add(amt_in_post_fee));
        let out_u64 = out.min(u64::MAX as u128) as u64;

        // Rough impact approximation: in / in_reserve
        let impact = ((amount_in as u128) * 10_000u128 / in_reserve).min(10_000u128) as u32;
        (out_u64, impact)
    }

    fn build_ix_data(disc: [u8; 8], a: u64, b: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(8 + 16);
        out.extend_from_slice(&disc);
        out.extend_from_slice(&a.to_le_bytes());
        out.extend_from_slice(&b.to_le_bytes());
        out
    }

    fn derive_ata(owner: Pubkey, mint: Pubkey) -> Pubkey {
        // Default to SPL Token if no specific program provided
        let spl_token_program = Pubkey::new_from_array(spl_token::id().to_bytes());
        Self::derive_ata_with_program(owner, mint, spl_token_program)
    }
}

#[async_trait]
impl Dex for PumpFunAmmDex {
    async fn refresh_pools(&self) -> Result<()> {
        Ok(())
    }

    /// Load a single pool by its market address (pool_address) via getAccount RPC.
    ///
    /// COLD PATH ONLY. For PumpFunAmm, the primary path uses LivePoolCache (Geyser);
    /// this RPC-based load is for Multi-Hop / fallback when cache misses.
    /// NOT getProgramAccounts - no scanning.
    async fn load_pool_by_address(&self, pool_address: &Pubkey) -> Result<()> {
        // Check if already cached via market index
        if self.pools_by_market.contains_key(pool_address) {
            debug!("pump_amm pool {} already in cache", pool_address);
            return Ok(());
        }

        debug!(
            "Loading pump_amm pool {} via single getAccount",
            pool_address
        );

        // Fetch the pool market account to get base_mint
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_quote_mint = Pubkey::from_str(WSOL_MINT)?;

        let account = self
            .rpc
            .get_account_retry(pool_address)
            .await
            .map_err(|e| anyhow!("Failed to fetch pump_amm pool {}: {}", pool_address, e))?;

        if account.owner != pump_amm_program {
            return Err(anyhow!(
                "pump_amm pool {} has wrong owner: {}",
                pool_address,
                account.owner
            ));
        }

        // Parse base_mint from market account data
        if account.data.len() < PUMPFUN_AMM_MARKET_MIN_DATA_LEN {
            return Err(anyhow!(
                "pump_amm pool {} data too short: {} < {}",
                pool_address,
                account.data.len(),
                PUMPFUN_AMM_MARKET_MIN_DATA_LEN
            ));
        }

        let base_mint = Pubkey::new_from_array(
            account.data[PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("pump_amm market base_mint slice"))?,
        );
        let quote_mint = Pubkey::new_from_array(
            account.data[PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize
                ..(PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET as usize + 32)]
                .try_into()
                .map_err(|_| anyhow!("pump_amm market quote_mint slice"))?,
        );

        // PumpSwap AMM only supports WSOL quote
        if quote_mint != expected_quote_mint {
            return Err(anyhow!(
                "pump_amm pool {} has unexpected quote_mint: {} (expected WSOL)",
                pool_address,
                quote_mint
            ));
        }

        // Use existing method to parse full pool structure.
        // Pass pre-fetched data to avoid redundant RPC call that can fail
        // due to transient RPC endpoint inconsistencies.
        let prefetched = (account.owner, account.executable, account.data);
        match self
            .try_parse_pool_static_from_market_account_inner(
                *pool_address,
                base_mint,
                Some(prefetched),
            )
            .await
        {
            Ok(Some(pool)) => {
                self.pools_by_base.insert(base_mint, pool.clone());
                self.pools_by_market.insert(*pool_address, base_mint);
                debug!(
                    "Loaded pump_amm pool {}: base_mint={} via single RPC call",
                    pool_address, base_mint
                );
                Ok(())
            }
            Ok(None) => {
                // Market-account heuristics failed (common for Token-2022 pools or
                // pools with non-standard account layouts).
                // Fallback: parse instruction accounts from a real on-chain swap tx.
                warn!(
                    pool = %pool_address,
                    base_mint = %base_mint,
                    "pump_amm load_pool_by_address: market-account parse returned None, trying TX-history fallback"
                );
                match self
                    .discover_pool_static_via_tx_history_market_only(*pool_address, base_mint)
                    .await
                {
                    Ok(Some(pool)) => {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint,
                            "pump_amm load_pool_by_address: TX-history fallback SUCCESS"
                        );
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(*pool_address, base_mint);
                        Ok(())
                    }
                    Ok(None) => Err(anyhow!(
                        "pump_amm pool {} could not be parsed (market-account returned None, TX-history also None)",
                        pool_address
                    )),
                    Err(e) => Err(anyhow!(
                        "pump_amm pool {} market-account returned None, TX-history fallback failed: {}",
                        pool_address,
                        e
                    )),
                }
            }
            Err(e) => {
                // Market-account parse produced an error. Also try TX-history.
                warn!(
                    pool = %pool_address,
                    base_mint = %base_mint,
                    error = %e,
                    "pump_amm load_pool_by_address: market-account parse error, trying TX-history fallback"
                );
                match self
                    .discover_pool_static_via_tx_history_market_only(*pool_address, base_mint)
                    .await
                {
                    Ok(Some(pool)) => {
                        info!(
                            pool = %pool_address,
                            base_mint = %base_mint,
                            "pump_amm load_pool_by_address: TX-history fallback SUCCESS (after market-account error)"
                        );
                        self.pools_by_base.insert(base_mint, pool.clone());
                        self.pools_by_market.insert(*pool_address, base_mint);
                        Ok(())
                    }
                    Ok(None) => Err(anyhow!(
                        "pump_amm pool {} parse failed: {} (TX-history also None)",
                        pool_address,
                        e
                    )),
                    Err(e2) => Err(anyhow!(
                        "pump_amm pool {} parse failed: {} (TX-history also failed: {})",
                        pool_address,
                        e,
                        e2
                    )),
                }
            }
        }
    }

    /// Set pool data directly from accounts list (NO RPC!)
    ///
    /// Accounts format (v1 from DexPoolAccounts, 14 elements):
    /// [0] pool_market
    /// [1] global_config
    /// [2] base_mint
    /// [3] quote_mint
    /// [4] pool_base_vault
    /// [5] pool_quote_vault
    /// [6] protocol_fee_recipient (intent/cache slot; persisted value is canonical — see set_pool_from_accounts)
    /// [7] protocol_fee_recipient_ta (intent/cache slot; persisted value is canonical derivation)
    /// [8] event_authority
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    fn set_pool_from_accounts(&self, pool_address: &str, accounts: &[String]) -> Result<()> {
        // Accept both 14-element (v1 with volume accumulators) and 12-element (v2 without)
        if accounts.len() < 12 {
            return Err(anyhow!(
                "pump_amm set_pool_from_accounts requires at least 12 accounts, got {}",
                accounts.len()
            ));
        }

        let parse_pubkey = |s: &str, name: &str| -> Result<Pubkey> {
            Pubkey::from_str(s).map_err(|e| anyhow!("Invalid {} pubkey '{}': {}", name, s, e))
        };

        let pool_market = parse_pubkey(&accounts[0], "pool_market")?;
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if accounts.len() > 1 {
            if let Ok(parsed_gc) = parse_pubkey(&accounts[1], "global_config") {
                if parsed_gc != global_config {
                    warn!(
                        intent = %parsed_gc,
                        expected = %global_config,
                        "pump_amm set_pool_from_accounts: accounts[1] != canonical global_config; using canonical"
                    );
                }
            }
        }
        let base_mint = parse_pubkey(&accounts[2], "base_mint")?;
        let quote_mint = parse_pubkey(&accounts[3], "quote_mint")?;
        let pool_base_vault = parse_pubkey(&accounts[4], "pool_base_vault")?;
        let pool_quote_vault = parse_pubkey(&accounts[5], "pool_quote_vault")?;
        let parsed_protocol_fee_recipient = parse_pubkey(&accounts[6], "protocol_fee_recipient")?;
        let parsed_protocol_fee_recipient_ta =
            parse_pubkey(&accounts[7], "protocol_fee_recipient_ta")?;
        // PumpSwap only uses WSOL quote; fee recipient ATA uses SPL Token program for quote mint.
        let quote_token_program_for_fee = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_canonical_protocol_fee_accounts(quote_mint, quote_token_program_for_fee);
        if parsed_protocol_fee_recipient != protocol_fee_recipient {
            warn!(
                intent = %parsed_protocol_fee_recipient,
                expected = %protocol_fee_recipient,
                "pump_amm set_pool_from_accounts: accounts[6] != canonical protocol_fee_recipient; using canonical"
            );
        }
        if parsed_protocol_fee_recipient_ta != protocol_fee_recipient_ta {
            warn!(
                intent = %parsed_protocol_fee_recipient_ta,
                expected = %protocol_fee_recipient_ta,
                "pump_amm set_pool_from_accounts: accounts[7] != derived protocol_fee_recipient_ta; using canonical derivation"
            );
        }
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let parsed_event_authority = parse_pubkey(&accounts[8], "event_authority")?;
        let event_authority = pump_amm_canonical_event_authority(&pump_amm_program);
        if parsed_event_authority != event_authority {
            warn!(
                intent = %parsed_event_authority,
                expected = %event_authority,
                "pump_amm set_pool_from_accounts: accounts[8] != canonical event_authority; using canonical"
            );
        }
        let coin_creator_vault_ata = parse_pubkey(&accounts[9], "coin_creator_vault_ata")?;
        let coin_creator_vault_authority =
            parse_pubkey(&accounts[10], "coin_creator_vault_authority")?;

        // CRITICAL: v1 format (14 accounts) vs v2 format (12 accounts) have different layouts!
        // v1: [0..10]=common, [11]=global_volume_accumulator, [12]=fee_config, [13]=fee_program
        // v2: [0..10]=common, [11]=fee_config (no volume accumulators)
        let (global_volume_accumulator, fee_config, fee_program) = if accounts.len() >= 14 {
            // v1 format: 14 accounts with volume accumulators
            let gva = parse_pubkey(&accounts[11], "global_volume_accumulator")?;
            let fc = parse_pubkey(&accounts[12], "fee_config")?;
            let fp = parse_pubkey(&accounts[13], "fee_program")?;
            info!(
                accounts_len = accounts.len(),
                format = "v1",
                fee_config = %fc,
                fee_program = %fp,
                "pump_amm set_pool_from_accounts: parsed v1 format"
            );
            (gva, fc, fp)
        } else {
            // v2 format: 12 accounts without volume accumulators
            let fc = parse_pubkey(&accounts[11], "fee_config")?;
            let fp = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;
            info!(
                accounts_len = accounts.len(),
                format = "v2",
                fee_config = %fc,
                fee_program = %fp,
                "pump_amm set_pool_from_accounts: parsed v2 format"
            );
            (Pubkey::default(), fc, fp)
        };

        // Validate pool_address matches accounts[0]
        let expected_pool = parse_pubkey(pool_address, "pool_address")?;
        if expected_pool != pool_market {
            return Err(anyhow!(
                "pool_address {} does not match accounts[0] {}",
                pool_address,
                pool_market
            ));
        }

        let pool = PumpAmmPoolStatic {
            pool_market,
            global_config,
            base_mint,
            quote_mint,
            pool_base_vault,
            pool_quote_vault,
            protocol_fee_recipient,
            protocol_fee_recipient_ta,
            event_authority,
            coin_creator_vault_ata,
            coin_creator_vault_authority,
            global_volume_accumulator,
            fee_config,
            fee_program,
        };

        debug!(
            pool_market = %pool_market,
            base_mint = %base_mint,
            "pump_amm pool set from intent accounts (NO RPC)"
        );

        self.pools_by_base.insert(base_mint, pool);
        self.pools_by_market.insert(pool_market, base_mint);

        Ok(())
    }

    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        // WSOL pairs only.
        let (base_mint_str, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Ok(None);
        };

        let base_mint = Pubkey::from_str(base_mint_str).context("invalid base mint")?;

        // GEYSER-FIRST: Try LivePoolCache for reserves before any RPC call.
        // The cache is populated by market-data via Geyser account subscriptions
        // and propagated to execution-engine via PoolCacheUpdate JetStream events.
        if let Some(ref cache) = self.live_pool_cache {
            if let Some((base_r, quote_r, pool_market)) =
                cache.get_pump_amm_reserves_by_base_mint(&base_mint)
            {
                let base_reserve = base_r as u128;
                let quote_reserve = quote_r as u128;

                let (in_reserve, out_reserve) = if is_buy {
                    (quote_reserve, base_reserve)
                } else {
                    (base_reserve, quote_reserve)
                };

                let (amount_out, price_impact_bps) =
                    self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
                if amount_out == 0 {
                    if self.allow_rpc_on_miss {
                        warn!(
                            base_mint = %base_mint_str,
                            base_reserve = base_r,
                            quote_reserve = quote_r,
                            "pump_amm: cache reserves degenerate (one side=0), Cold Path falling through to RPC"
                        );
                    } else {
                        return Ok(None);
                    }
                } else {
                    debug!(
                        base_mint = %base_mint_str,
                        pool = %pool_market,
                        base_reserve = base_r,
                        quote_reserve = quote_r,
                        amount_out,
                        "pump_amm: quote from LivePoolCache (ZERO RPC)"
                    );

                    return Ok(Some(Quote {
                        amount_out,
                        price_impact_bps,
                        route: vec![pool_market.to_string()],
                        fee_bps: DEFAULT_TOTAL_FEE_BPS,
                        in_reserve,
                        out_reserve,
                        input_mint: input_mint.to_string(),
                        output_mint: output_mint.to_string(),
                        tick_spacing: None,
                    }));
                }
            }
            // Cache miss: Hot Path (allow_rpc_on_miss=false) → None. Cold Path (true) → RPC fallback. P3 #12.
            if !self.allow_rpc_on_miss {
                debug!(base_mint = %base_mint_str, "pump_amm: quote cache miss, returning None (no RPC)");
                return Ok(None);
            }
        }

        // RPC FALLBACK (Cold Path only): No LivePoolCache or allow_rpc_on_miss — discover pool and fetch vault reserves via RPC.
        warn!(
            base_mint = %base_mint_str,
            "pump_amm: no LivePoolCache, using RPC fallback for quote"
        );

        let pool = match self.discover_pool_static(base_mint).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // Read pool vault reserves from the (fast) local RPC.
        let base_reserve = self.get_vault_amount(pool.pool_base_vault).await? as u128;
        let quote_reserve = self.get_vault_amount(pool.pool_quote_vault).await? as u128;

        let (in_reserve, out_reserve) = if is_buy {
            (quote_reserve, base_reserve)
        } else {
            (base_reserve, quote_reserve)
        };

        let (amount_out, price_impact_bps) =
            self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
        if amount_out == 0 {
            return Ok(None);
        }

        Ok(Some(Quote {
            amount_out,
            price_impact_bps,
            route: vec![pool.pool_market.to_string()],
            fee_bps: DEFAULT_TOTAL_FEE_BPS,
            in_reserve,
            out_reserve,
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: None,
        }))
    }

    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let user = self
            .user_authority
            .ok_or_else(|| anyhow!("pump_amm user_authority not set"))?;

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

        let (base_mint_str, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Err(anyhow!("pump_amm only supports WSOL pairs"));
        };

        let base_mint = Pubkey::from_str(base_mint_str)?;

        // Blocking in sync fn: we rely on caches being primed by quote path.
        // If not present, we fail fast with a clear error.
        let pool = self
            .pools_by_base
            .get(&base_mint)
            .map(|v| v.clone())
            .ok_or_else(|| {
                anyhow!("pump_amm pool not discovered/cached for base_mint={base_mint}")
            })?;

        // Prefer discovered user accounts if available; fallback to ATAs for token accounts.
        let user_acc = self
            .user_accounts
            .get(&(pool.pool_market, user))
            .map(|v| v.clone());
        // Get token program from cache for correct ATA derivation (Token-2022 support)
        let base_token_program = self
            .cached_data
            .get(&format!("token_program:{}", pool.base_mint))
            .and_then(|v| Pubkey::from_str(&v).ok())
            .unwrap_or_else(|| Pubkey::new_from_array(spl_token::id().to_bytes()));
        let quote_token_program = Pubkey::new_from_array(spl_token::id().to_bytes()); // WSOL always uses SPL Token

        let user_base_ta = user_acc
            .as_ref()
            .map(|u| u.user_base_ta)
            .unwrap_or_else(|| {
                Self::derive_ata_with_program(user, pool.base_mint, base_token_program)
            });
        let user_quote_ta = user_acc
            .as_ref()
            .map(|u| u.user_quote_ta)
            .unwrap_or_else(|| {
                Self::derive_ata_with_program(user, pool.quote_mint, quote_token_program)
            });
        let user_volume = user_acc
            .as_ref()
            .map(|u| u.user_volume_accumulator)
            .unwrap_or_else(|| {
                Self::derive_user_volume_accumulator(program_id, pool.pool_market, user)
            });

        let disc = if is_buy {
            anchor_disc("buy_exact_quote_in")
        } else {
            anchor_disc("sell")
        };
        let data = Self::build_ix_data(disc, amount_in, min_out);

        // Account ordering differs between BUY (23 accounts) and SELL (21 accounts).
        // Reference: observed on-chain Pump.fun AMM swap transactions.
        // BUY includes global_volume_accumulator (#16) and user_volume (#19), SELL does not.
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if pool.global_config != global_config {
            warn!(
                pool = %pool.global_config,
                expected = %global_config,
                "pump_amm build_swap_ix: cached pool.global_config != canonical; using canonical"
            );
        }
        let event_authority = pump_amm_canonical_event_authority(&program_id);
        if pool.event_authority != event_authority {
            warn!(
                pool = %pool.event_authority,
                expected = %event_authority,
                "pump_amm build_swap_ix: cached pool.event_authority != canonical; using canonical"
            );
        }
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_canonical_protocol_fee_accounts(pool.quote_mint, quote_token_program);
        if pool.protocol_fee_recipient != protocol_fee_recipient {
            warn!(
                pool = %pool.protocol_fee_recipient,
                expected = %protocol_fee_recipient,
                "pump_amm build_swap_ix: cached pool.protocol_fee_recipient != canonical; using canonical"
            );
        }
        if pool.protocol_fee_recipient_ta != protocol_fee_recipient_ta {
            warn!(
                pool = %pool.protocol_fee_recipient_ta,
                expected = %protocol_fee_recipient_ta,
                "pump_amm build_swap_ix: cached pool.protocol_fee_recipient_ta != derived canonical; using canonical"
            );
        }
        let mut metas = vec![
            AccountMeta::new(pool.pool_market, false),         // 0
            AccountMeta::new(user, true),                      // 1
            AccountMeta::new_readonly(global_config, false),   // 2
            AccountMeta::new_readonly(pool.base_mint, false),  // 3
            AccountMeta::new_readonly(pool.quote_mint, false), // 4
            AccountMeta::new(user_base_ta, false),             // 5
            AccountMeta::new(user_quote_ta, false),            // 6
            AccountMeta::new(pool.pool_base_vault, false),     // 7
            AccountMeta::new(pool.pool_quote_vault, false),    // 8
            AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
            AccountMeta::new(protocol_fee_recipient_ta, false), // 10
            AccountMeta::new_readonly(base_token_program, false), // 11
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 12
            AccountMeta::new_readonly(
                Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                false,
            ), // 13
            AccountMeta::new_readonly(
                Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                false,
            ), // 14
            AccountMeta::new_readonly(event_authority, false), // 15
        ];

        if is_buy {
            // BUY: accounts 16-22 (23 total)
            metas.push(AccountMeta::new(pool.global_volume_accumulator, false)); // 16
            metas.push(AccountMeta::new(pool.coin_creator_vault_ata, false)); // 17
            metas.push(AccountMeta::new_readonly(
                pool.coin_creator_vault_authority,
                false,
            )); // 18
            metas.push(AccountMeta::new(user_volume, false)); // 19
            metas.push(AccountMeta::new_readonly(pool.fee_config, false)); // 20
            metas.push(AccountMeta::new_readonly(pool.fee_program, false)); // 21
            metas.push(AccountMeta::new_readonly(program_id, false)); // 22
        } else {
            // SELL: accounts 16-20 (21 total) - no volume accumulators
            metas.push(AccountMeta::new_readonly(program_id, false)); // 16
            metas.push(AccountMeta::new(pool.coin_creator_vault_ata, false)); // 17
            metas.push(AccountMeta::new_readonly(
                pool.coin_creator_vault_authority,
                false,
            )); // 18
            metas.push(AccountMeta::new_readonly(pool.fee_config, false)); // 19
            metas.push(AccountMeta::new_readonly(pool.fee_program, false)); // 20
        }

        Ok(vec![Instruction {
            program_id,
            accounts: metas,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        Vec::new()
    }

    fn cache_extra_data(&self, key: &str, value: &str) {
        self.cached_data.insert(key.to_string(), value.to_string());
    }
}

impl PumpFunAmmDex {
    /// Build a Pump.fun AMM (PumpSwap) swap instruction purely from static pool accounts.
    ///
    /// This is the intent-driven path: execution-engine can plan/simulate without any
    /// on-chain discovery (no tx-history scans, no Helius).
    ///
    /// `pool_accounts` must be the v1 ordered list produced by market-data
    /// (MarketEventKind::DexPoolAccounts):
    /// [0] pool_market
    /// [1] global_config
    /// [2] base_mint
    /// [3] quote_mint
    /// [4] pool_base_vault
    /// [5] pool_quote_vault
    /// [6] protocol_fee_recipient (cache slot; builder uses canonical mainnet recipient, see dex_parser)
    /// [7] protocol_fee_recipient_ta (cache slot; builder derives ATA from [6] canonical + quote mint + quote TP)
    /// [8] event_authority (cache slot; builders use canonical `__event_authority` PDA for ix metas)
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    ///
    /// `base_token_program` - Optional token program override for the base token.
    /// Use `Some(TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb)` for Token-2022 tokens.
    /// Defaults to SPL Token if None.
    pub fn build_swap_ix_from_pool_accounts(
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        user: Pubkey,
        pool_accounts: &[Pubkey],
        base_token_program: Option<Pubkey>,
    ) -> Result<Vec<Instruction>> {
        // Require 14 accounts (v1 format with global_volume_accumulator)
        if pool_accounts.len() < 14 {
            return Err(anyhow!(
                "pump_amm expected at least 14 pool_accounts (v1 format), got {}",
                pool_accounts.len()
            ));
        }

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_fee_program = Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID)?;

        let pool_market = pool_accounts[0];
        let global_config = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG)?;
        if pool_accounts.len() > 1 && pool_accounts[1] != global_config {
            warn!(
                from_pool_accounts = %pool_accounts[1],
                expected = %global_config,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[1] != canonical global_config; using canonical"
            );
        }
        let base_mint = pool_accounts[2];
        let quote_mint = pool_accounts[3];
        let pool_base_vault = pool_accounts[4];
        let pool_quote_vault = pool_accounts[5];
        let event_authority = pump_amm_canonical_event_authority(&program_id);
        if pool_accounts.len() > 8 && pool_accounts[8] != event_authority {
            warn!(
                from_pool_accounts = %pool_accounts[8],
                expected = %event_authority,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[8] != canonical event_authority; using canonical"
            );
        }
        let coin_creator_vault_ata = pool_accounts[9];
        let coin_creator_vault_authority = pool_accounts[10];
        let global_volume_accumulator = pool_accounts[11]; // REQUIRED for BUY!

        // CRITICAL FIX: Use the global fee_config constant instead of trusting pool_accounts.
        // The fee_config is the SAME for all pools - it's a global account owned by the Fee Program.
        // Observed from successful on-chain SELL (21 accounts) and BUY (23 accounts) transactions.
        let fee_config = Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG)?;
        let fee_program = expected_fee_program; // Always use expected, don't trust intent

        // fee_config and fee_program are now constants, no need for validation

        let (expected_base, is_buy) = if input_mint == WSOL_MINT {
            (output_mint, true)
        } else if output_mint == WSOL_MINT {
            (input_mint, false)
        } else {
            return Err(anyhow!("pump_amm only supports WSOL pairs"));
        };

        let expected_base = Pubkey::from_str(expected_base)?;
        if expected_base != base_mint {
            return Err(anyhow!(
                "pump_amm base_mint mismatch: intent expects {expected_base}, pool_accounts has {base_mint}"
            ));
        }

        // Resolve token programs: use override for base token (Token-2022 support),
        // WSOL always uses SPL Token.
        let base_tp = base_token_program
            .unwrap_or_else(|| Pubkey::new_from_array(spl_token::id().to_bytes()));
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes()); // WSOL always SPL Token

        // Canonical protocol fee recipient + quote ATA — same as `dex_parser.rs`
        // `build_pool_accounts_from_create_pool` / mainnet SELL references. Do not trust
        // `pool_accounts[6]` / `[7]` alone: misaligned or stale cache slots caused
        // `InvalidProtocolFeeRecipient` (6013) while ix account order was already correct.
        let (protocol_fee_recipient, protocol_fee_recipient_ta) =
            pump_amm_canonical_protocol_fee_accounts(quote_mint, quote_tp);
        if pool_accounts.len() > 6 && pool_accounts[6] != protocol_fee_recipient {
            warn!(
                from_pool_accounts = %pool_accounts[6],
                expected = %protocol_fee_recipient,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[6] != canonical protocol_fee_recipient; using canonical"
            );
        }
        if pool_accounts.len() > 7 && pool_accounts[7] != protocol_fee_recipient_ta {
            warn!(
                from_pool_accounts = %pool_accounts[7],
                expected = %protocol_fee_recipient_ta,
                "pump_amm build_swap_ix_from_pool_accounts: pool_accounts[7] != derived protocol_fee_recipient_ta; using canonical derivation"
            );
        }

        // User token accounts are deterministic ATAs with correct token program.
        let user_base_ta = Self::derive_ata_with_program(user, base_mint, base_tp);
        let user_quote_ta = Self::derive_ata_with_program(user, quote_mint, quote_tp);

        // User volume accumulator is a PDA; needed for BUY transactions.
        let user_vol = Self::derive_user_volume_accumulator(program_id, pool_market, user);

        let disc = if is_buy {
            anchor_disc("buy_exact_quote_in")
        } else {
            anchor_disc("sell")
        };
        let data = Self::build_ix_data(disc, amount_in, min_out);

        // Account ordering differs between BUY (23 accounts) and SELL (21 accounts).
        // BUY requires global_volume_accumulator and user_volume_accumulator.
        // See dex_parser.rs for reference account ordering from on-chain transactions.
        let metas = if is_buy {
            // BUY: 23 accounts
            vec![
                AccountMeta::new(pool_market, false),                     // 0
                AccountMeta::new(user, true),                             // 1
                AccountMeta::new_readonly(global_config, false),          // 2
                AccountMeta::new_readonly(base_mint, false),              // 3
                AccountMeta::new_readonly(quote_mint, false),             // 4
                AccountMeta::new(user_base_ta, false),                    // 5
                AccountMeta::new(user_quote_ta, false),                   // 6
                AccountMeta::new(pool_base_vault, false),                 // 7
                AccountMeta::new(pool_quote_vault, false),                // 8
                AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
                AccountMeta::new(protocol_fee_recipient_ta, false),       // 10
                AccountMeta::new_readonly(base_tp, false), // 11 - base token program (Token-2022 aware)
                AccountMeta::new_readonly(quote_tp, false), // 12 - quote token program (always SPL)
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                    false,
                ), // 13
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                    false,
                ), // 14
                AccountMeta::new_readonly(event_authority, false), // 15
                AccountMeta::new(global_volume_accumulator, false), // 16 - REQUIRED for BUY!
                AccountMeta::new(coin_creator_vault_ata, false), // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
                AccountMeta::new(user_vol, false),         // 19 - user volume accumulator
                AccountMeta::new_readonly(fee_config, false), // 20
                AccountMeta::new_readonly(fee_program, false), // 21
                AccountMeta::new_readonly(program_id, false), // 22
            ]
        } else {
            // SELL: 21 accounts (no volume accumulators)
            vec![
                AccountMeta::new(pool_market, false),                     // 0
                AccountMeta::new(user, true),                             // 1
                AccountMeta::new_readonly(global_config, false),          // 2
                AccountMeta::new_readonly(base_mint, false),              // 3
                AccountMeta::new_readonly(quote_mint, false),             // 4
                AccountMeta::new(user_base_ta, false),                    // 5
                AccountMeta::new(user_quote_ta, false),                   // 6
                AccountMeta::new(pool_base_vault, false),                 // 7
                AccountMeta::new(pool_quote_vault, false),                // 8
                AccountMeta::new_readonly(protocol_fee_recipient, false), // 9
                AccountMeta::new(protocol_fee_recipient_ta, false),       // 10
                AccountMeta::new_readonly(base_tp, false), // 11 - base token program (Token-2022 aware)
                AccountMeta::new_readonly(quote_tp, false), // 12 - quote token program (always SPL)
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                    false,
                ), // 13
                AccountMeta::new_readonly(
                    Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                    false,
                ), // 14
                AccountMeta::new_readonly(event_authority, false), // 15
                AccountMeta::new_readonly(program_id, false), // 16
                AccountMeta::new(coin_creator_vault_ata, false), // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
                AccountMeta::new_readonly(fee_config, false), // 19
                AccountMeta::new_readonly(fee_program, false), // 20
            ]
        };

        Ok(vec![Instruction {
            program_id,
            accounts: metas,
            data,
        }])
    }

    /// Prime discovery caches for a base mint (static pool) and a user (user-specific accounts).
    pub async fn ensure_discovered_for_user(&self, base_mint: Pubkey, user: Pubkey) -> Result<()> {
        let pool = self
            .discover_pool_static(base_mint)
            .await?
            .ok_or_else(|| anyhow!("pump_amm: no pool found for base_mint={base_mint}"))?;

        let _ua = self
            .discover_user_accounts(pool.pool_market, base_mint, user)
            .await?
            .ok_or_else(|| {
                anyhow!("pump_amm: no user accounts found for user={user} base_mint={base_mint}")
            })?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::live_pool_cache::{CachedPoolState, LivePoolCache, PumpAmmState};
    use crate::solana::dex::Dex;
    use crate::solana::rpc::SolanaRpc;
    use std::str::FromStr;
    use std::sync::Arc;

    fn make_pump_amm_cache_with_reserves(
        pool_market: Pubkey,
        base_mint: Pubkey,
        base_reserve: u64,
        quote_reserve: u64,
    ) -> Arc<LivePoolCache> {
        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(base_reserve),
                quote_reserve: Some(quote_reserve),
                pool_accounts: vec![],
                creator: None,
            }),
            100,
        );
        Arc::new(cache)
    }

    fn make_pump_amm_cache_with_pool_accounts(
        pool_market: Pubkey,
        base_mint: Pubkey,
        pool_accounts: Vec<Pubkey>,
    ) -> Arc<LivePoolCache> {
        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint: Pubkey::from_str(WSOL_MINT).unwrap(),
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1),
                quote_reserve: Some(1),
                pool_accounts,
                creator: None,
            }),
            100,
        );
        Arc::new(cache)
    }

    fn make_empty_cache() -> Arc<LivePoolCache> {
        Arc::new(LivePoolCache::new())
    }

    #[tokio::test]
    async fn test_quote_exact_in_cache_hit_no_rpc() {
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let cache = make_pump_amm_cache_with_reserves(
            pool_market,
            base_mint,
            1_000_000_000_000,
            50_000_000_000,
        );
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let base_mint_str = base_mint.to_string();
        let result = dex
            .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000_000)
            .await;

        let quote = result.expect("quote should succeed");
        assert!(quote.is_some(), "expected Some(Quote) on cache hit");
        let quote = quote.unwrap();
        assert!(quote.amount_out > 0);
        assert!(quote.route.contains(&pool_market.to_string()));
        assert_eq!(quote.fee_bps, 125);
    }

    #[tokio::test]
    async fn fetch_pump_amm_vault_reserves_rejects_short_accounts() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, make_empty_cache(), false);
        let short = vec![Pubkey::new_unique(); 5];
        let err = dex
            .fetch_pump_amm_vault_reserves(&short)
            .await
            .expect_err("expected err");
        assert!(
            err.to_string().contains("pool_accounts len"),
            "unexpected err: {err}"
        );
    }

    #[tokio::test]
    async fn test_quote_exact_in_cache_miss_returns_none() {
        let base_mint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let base_mint_str = base_mint.to_string();
        let result = dex
            .quote_exact_in(WSOL_MINT, &base_mint_str, 1_000_000)
            .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_pool_accounts_v1_for_base_mint_cache_hit() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let pool_market = Pubkey::new_unique();
        let pool_accounts: Vec<Pubkey> = vec![
            pool_market,
            Pubkey::new_unique(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let cache =
            make_pump_amm_cache_with_pool_accounts(pool_market, base_mint, pool_accounts.clone());
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let result = dex.pool_accounts_v1_for_base_mint(base_mint).await;

        assert!(result.is_ok());
        let accounts = result.unwrap();
        assert!(accounts.is_some());
        let accounts = accounts.unwrap();
        assert_eq!(accounts.len(), 14);
        assert_eq!(accounts, pool_accounts);
    }

    #[tokio::test]
    async fn test_pool_accounts_v1_for_base_mint_cache_miss() {
        let base_mint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, false);

        let result = dex.pool_accounts_v1_for_base_mint(base_mint).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    /// I-24d: A bad `pool_address_hint` must not fall through to `getProgramAccounts` (~20s+).
    #[tokio::test]
    async fn test_pool_address_hint_parse_fail_errors_without_global_scan() {
        let base_mint = Pubkey::new_unique();
        // Missing account → try_parse returns Ok(None) after one cheap RPC attempt.
        let bad_hint = Pubkey::new_unique();
        let cache = make_empty_cache();
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let dex = PumpFunAmmDex::new_with_cache(rpc, cache, true);

        let fut = dex.pool_accounts_v1_for_base_mint_with_hint(base_mint, Some(bad_hint));
        let completed = tokio::time::timeout(std::time::Duration::from_secs(3), fut)
            .await
            .expect("must not block on getProgramAccounts global scan");

        assert!(
            completed.is_err(),
            "expected Err for failed hint parse; got {completed:?}"
        );
        let msg = format!("{:#}", completed.unwrap_err());
        assert!(
            msg.contains("I-24d") || msg.contains("pool_address hint"),
            "unexpected error message: {msg}"
        );
    }

    #[test]
    fn test_build_swap_ix_from_pool_accounts() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let base_mint_str = base_mint.to_string();
        let user = Pubkey::new_unique();

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let result = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            WSOL_MINT,
            &base_mint_str,
            1_000_000_000,
            100_000,
            user,
            &pool_accounts,
            None,
        );

        assert!(result.is_ok());
        let ixs = result.unwrap();
        assert!(!ixs.is_empty());
        assert_eq!(
            ixs[0].program_id,
            Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap()
        );
        assert!(!ixs[0].data.is_empty());
    }

    /// SELL path: instruction account #2 must be the canonical global_config (not pool_accounts[1]).
    #[test]
    fn test_pumpswap_sell_global_config_meta_is_canonical() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let canonical_gc = Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap();

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(), // pool
            Pubkey::new_unique(), // wrong on purpose — must not appear at ix[2]
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);
        assert_ne!(pool_accounts[1], canonical_gc);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[2].pubkey, canonical_gc);
    }

    /// SELL path: instruction account #15 must be the canonical `__event_authority` PDA (not pool_accounts[8]).
    #[test]
    fn test_pumpswap_sell_event_authority_meta_is_canonical() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap();
        let canonical_ea = pump_amm_canonical_event_authority(&program_id);

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(), // wrong on purpose — must not appear at ix[15]
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);
        assert_ne!(pool_accounts[8], canonical_ea);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[15].pubkey, canonical_ea);
    }

    /// SELL path: ix[9]/[10] must use canonical protocol_fee_recipient + derived ATA (not pool_accounts[6]/[7]).
    #[test]
    fn test_pumpswap_sell_protocol_fee_recipient_metas_are_canonical() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let canonical_pfr = Pubkey::from_str(PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT).unwrap();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let expected_pfr_ta = PumpFunAmmDex::derive_ata_with_program(canonical_pfr, wsol, quote_tp);

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(), // [6] wrong on purpose
            Pubkey::new_unique(), // [7] wrong on purpose
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);
        assert_ne!(pool_accounts[6], canonical_pfr);
        assert_ne!(pool_accounts[7], expected_pfr_ta);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            None,
        )
        .expect("SELL build");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[9].pubkey, canonical_pfr);
        assert_eq!(ixs[0].accounts[10].pubkey, expected_pfr_ta);
    }

    /// SELL path: Token-2022 base mint — ix[11] must be Token-2022 program; user base ATA (ix[5]) must match derivation.
    /// Wrong SPL program → wrong ATA → Custom(6023) NotEnoughTokensToSell on-chain.
    #[test]
    fn test_pumpswap_sell_token2022_base_program_and_user_ata() {
        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let user = Pubkey::new_unique();
        let token_2022 = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

        let pool_accounts: Vec<Pubkey> = vec![
            Pubkey::new_unique(),
            Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            wsol,
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
            Pubkey::new_unique(),
        ];
        assert_eq!(pool_accounts.len(), 14);

        let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &base_mint.to_string(),
            WSOL_MINT,
            1_000_000,
            1,
            user,
            &pool_accounts,
            Some(token_2022),
        )
        .expect("SELL build Token-2022");

        assert_eq!(ixs[0].accounts[11].pubkey, token_2022);
        let owner_spl = SplProgramPubkey::new_from_array(user.to_bytes());
        let mint_spl = SplProgramPubkey::new_from_array(base_mint.to_bytes());
        let token_program_spl = SplProgramPubkey::new_from_array(token_2022.to_bytes());
        let expected_user_base =
            spl_associated_token_account::get_associated_token_address_with_program_id(
                &owner_spl,
                &mint_spl,
                &token_program_spl,
            );
        let expected_user_base = Pubkey::new_from_array(expected_user_base.to_bytes());
        assert_eq!(ixs[0].accounts[5].pubkey, expected_user_base);
    }

    /// Cached/sync `build_swap_ix`: stale `PumpAmmPoolStatic.protocol_fee_*` must not reach ix #9/#10.
    #[test]
    fn test_build_swap_ix_protocol_fee_metas_canonical_despite_stale_cached_pool() {
        let rpc = Arc::new(SolanaRpc::new("http://127.0.0.1:0"));
        let mut dex = PumpFunAmmDex::new(rpc);
        let user = Pubkey::new_unique();
        dex.set_user_authority(user);

        let wsol = Pubkey::from_str(WSOL_MINT).unwrap();
        let base_mint = Pubkey::new_unique();
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).unwrap();
        let canonical_pfr = Pubkey::from_str(PUMPFUN_AMM_FALLBACK_PROTOCOL_FEE_RECIPIENT).unwrap();
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let expected_pfr_ta = PumpFunAmmDex::derive_ata_with_program(canonical_pfr, wsol, quote_tp);

        let pool = PumpAmmPoolStatic {
            pool_market: Pubkey::new_unique(),
            global_config: Pubkey::from_str(PUMPFUN_AMM_GLOBAL_CONFIG).unwrap(),
            base_mint,
            quote_mint: wsol,
            pool_base_vault: Pubkey::new_unique(),
            pool_quote_vault: Pubkey::new_unique(),
            protocol_fee_recipient: Pubkey::new_unique(),
            protocol_fee_recipient_ta: Pubkey::new_unique(),
            event_authority: pump_amm_canonical_event_authority(&program_id),
            coin_creator_vault_ata: Pubkey::new_unique(),
            coin_creator_vault_authority: Pubkey::new_unique(),
            global_volume_accumulator: Pubkey::new_unique(),
            fee_config: Pubkey::from_str(PUMPFUN_AMM_FEE_CONFIG).unwrap(),
            fee_program: Pubkey::from_str(PUMPFUN_AMM_FEE_PROGRAM_ID).unwrap(),
        };
        dex.pools_by_base.insert(base_mint, pool);

        let ixs = dex
            .build_swap_ix(&base_mint.to_string(), WSOL_MINT, 1_000_000, 1)
            .expect("SELL build_swap_ix");

        assert_eq!(ixs[0].accounts.len(), 21);
        assert_eq!(ixs[0].accounts[9].pubkey, canonical_pfr);
        assert_eq!(ixs[0].accounts[10].pubkey, expected_pfr_ta);
    }
}
