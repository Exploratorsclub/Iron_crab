use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD as BASE64_STD;
use base64::Engine;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use reqwest::Client;
use reqwest::StatusCode;
use serde_json::{json, Value};
use solana_sdk::hash::hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use tokio::sync::Mutex;
use tokio::sync::Semaphore;
use tokio::time::sleep;
use tracing::{debug, info, warn};

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMPFUN_AMM_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
// Observed on-chain in PumpSwap/Pump.fun AMM swaps: `fee_program` is this program id.
const PUMPFUN_AMM_FEE_PROGRAM_ID: &str = "pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ";
// Global fee_config account - owned by Fee Program, same for ALL pools.
// Observed in successful on-chain SELL and BUY transactions.
const PUMPFUN_AMM_FEE_CONFIG: &str = "5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx";

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

// Observed on-chain for at least one PumpSwap market account:
// global_config appears to be a Pubkey at offset 11, followed by base/quote mints.
const PUMPFUN_AMM_MARKET_GLOBAL_CONFIG_OFFSET: usize = 11;

// Observed on-chain: buy_exact_quote_in fee fields sum to 125 bps (lp 2 + protocol 93 + creator 30).
// We use that as a conservative default for quoting.
const DEFAULT_TOTAL_FEE_BPS: u32 = 125;

// Helius can aggressively rate limit across the entire API key. Keep JSON-RPC calls serialized
// and spaced out process-wide (not per `PumpFunAmmDex` instance).
static HELIUS_THROTTLES: Lazy<DashMap<String, Arc<HeliusThrottle>>> = Lazy::new(DashMap::new);

#[derive(Debug)]
struct HeliusThrottle {
    permits: Arc<Semaphore>,
    last_request: Arc<Mutex<Option<Instant>>>,
}

impl HeliusThrottle {
    fn new() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(1)),
            last_request: Arc::new(Mutex::new(None)),
        }
    }
}

fn normalize_rpc_url(u: &str) -> String {
    u.trim().trim_end_matches('/').to_string()
}

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
    rpc_url: String,
    helius_rpc_url: Option<String>,
    http: Client,
    user_authority: Option<Pubkey>,

    // Optional, process-wide throttle for the configured Helius endpoint.
    helius_throttle: Option<Arc<HeliusThrottle>>,

    // Prevent concurrent pool discovery storms (e.g. parallel exits) from hammering RPC.
    discovery_lock: Arc<Mutex<()>>,

    // Cache by base mint (WSOL quote only for now)
    pools_by_base: DashMap<Pubkey, PumpAmmPoolStatic>,
    // Index by pool_market address (for load_pool_by_address)
    pools_by_market: DashMap<Pubkey, Pubkey>, // pool_market -> base_mint
    user_accounts: DashMap<(Pubkey, Pubkey), PumpAmmUserAccounts>, // (pool_market, user)
    // Extra cached data (e.g., token_program:<mint> → program_id)
    cached_data: DashMap<String, String>,
}

impl PumpFunAmmDex {
    pub fn new(rpc: Arc<SolanaRpc>, rpc_url: String, helius_rpc_url: Option<String>) -> Self {
        let helius_throttle = helius_rpc_url.as_deref().map(|u| {
            let key = normalize_rpc_url(u);
            HELIUS_THROTTLES
                .entry(key)
                .or_insert_with(|| Arc::new(HeliusThrottle::new()))
                .clone()
        });

        Self {
            rpc,
            rpc_url,
            helius_rpc_url,
            http: Client::new(),
            user_authority: None,
            helius_throttle,
            discovery_lock: Arc::new(Mutex::new(())),
            pools_by_base: DashMap::new(),
            pools_by_market: DashMap::new(),
            user_accounts: DashMap::new(),
            cached_data: DashMap::new(),
        }
    }

    fn is_helius_endpoint(&self, endpoint: &str) -> bool {
        let Some(h) = self.helius_rpc_url.as_deref() else {
            return false;
        };
        normalize_rpc_url(h) == normalize_rpc_url(endpoint)
    }

    async fn helius_throttle_guard(
        &self,
        endpoint: &str,
    ) -> Option<tokio::sync::OwnedSemaphorePermit> {
        if !self.is_helius_endpoint(endpoint) {
            return None;
        }

        let throttle = self.helius_throttle.as_ref()?.clone();

        // Serialize Helius calls.
        let permit = throttle.permits.clone().acquire_owned().await.ok()?;

        // Space out calls to reduce 429/-32429.
        // Keep this conservative; kill-switch correctness > speed.
        const MIN_GAP_MS: u64 = 600;
        let min_gap = Duration::from_millis(MIN_GAP_MS);

        let mut last = throttle.last_request.lock().await;
        if let Some(prev) = *last {
            let since = prev.elapsed();
            if since < min_gap {
                sleep(min_gap - since).await;
            }
        }
        *last = Some(Instant::now());

        Some(permit)
    }

    fn parse_retry_after_ms(resp: &reqwest::Response) -> Option<u64> {
        // Retry-After can be seconds or an HTTP date. We only handle seconds.
        let v = resp
            .headers()
            .get(reqwest::header::RETRY_AFTER)?
            .to_str()
            .ok()?;
        let secs: u64 = v.trim().parse().ok()?;
        Some(secs.saturating_mul(1000))
    }

    pub fn set_user_authority(&mut self, user: Pubkey) {
        self.user_authority = Some(user);
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
    pub async fn pool_accounts_v1_for_base_mint(
        &self,
        base_mint: Pubkey,
    ) -> Result<Option<Vec<Pubkey>>> {
        let pool = match self.discover_pool_static(base_mint).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        // CRITICAL: global_volume_accumulator is required for BUY (BuyExactQuoteIn).
        // The PumpSwap program validates it exists and is initialized.
        // Without it: "AccountNotInitialized" error (Custom(3012)).
        Ok(Some(vec![
            pool.pool_market,              // [0]
            pool.global_config,            // [1]
            pool.base_mint,                // [2]
            pool.quote_mint,               // [3]
            pool.pool_base_vault,          // [4]
            pool.pool_quote_vault,         // [5]
            pool.protocol_fee_recipient,   // [6]
            pool.protocol_fee_recipient_ta, // [7]
            pool.event_authority,          // [8]
            pool.coin_creator_vault_ata,   // [9]
            pool.coin_creator_vault_authority, // [10]
            pool.global_volume_accumulator, // [11] - REQUIRED for BUY!
            pool.fee_config,               // [12]
            pool.fee_program,              // [13]
        ]))
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

    fn discovery_endpoints(&self) -> Vec<&str> {
        let mut endpoints: Vec<&str> = Vec::with_capacity(2);
        endpoints.push(self.rpc_url.as_str());
        if let Some(h) = self.helius_rpc_url.as_deref() {
            endpoints.push(h);
        }
        endpoints.dedup();
        endpoints
    }

    fn tx_history_endpoint(&self) -> &str {
        // Tx-history discovery is the highest RPC load in this module; prefer the full-index
        // endpoint (Helius) when available. Local validators are often pruned and will return
        // empty signature pages.
        self.helius_rpc_url
            .as_deref()
            .unwrap_or(self.rpc_url.as_str())
    }

    async fn rpc_call_tx_history(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let endpoint = self.tx_history_endpoint();
        let mut backoff_ms = 500u64;

        for attempt in 0..8usize {
            let _helius_guard = self.helius_throttle_guard(endpoint).await;
            let resp = self
                .http
                .post(endpoint)
                .json(&body)
                .send()
                .await
                .map_err(|e| {
                    anyhow!(
                        "pump_amm tx_history http error endpoint={endpoint} attempt={attempt}: {e}"
                    )
                })?;

            let status = resp.status();
            let retry_after_ms = Self::parse_retry_after_ms(&resp);
            let text = resp.text().await.map_err(|e| {
                anyhow!(
                    "pump_amm tx_history read body error endpoint={endpoint} attempt={attempt}: {e}"
                )
            })?;
            let v: Value = serde_json::from_str(&text).map_err(|e| {
                anyhow!(
                    "pump_amm tx_history json decode error endpoint={endpoint} attempt={attempt}: {e} body={text}"
                )
            })?;

            let is_rate_limited = status == StatusCode::TOO_MANY_REQUESTS
                || v.get("error")
                    .and_then(|e| e.get("code"))
                    .and_then(|c| c.as_i64())
                    == Some(-32429)
                || v.get("error")
                    .and_then(|e| e.get("message"))
                    .and_then(|m| m.as_str())
                    .map(|m| m.to_ascii_lowercase().contains("rate"))
                    .unwrap_or(false);

            if is_rate_limited {
                let wait_ms = retry_after_ms.unwrap_or(backoff_ms);
                sleep(Duration::from_millis(wait_ms)).await;
                backoff_ms = (backoff_ms * 2).min(8000);
                continue;
            }

            if !status.is_success() {
                return Err(anyhow!(
                    "pump_amm tx_history http status {status} endpoint={endpoint}: {v}"
                ));
            }
            if v.get("error").is_some() {
                return Err(anyhow!(
                    "pump_amm tx_history rpc error endpoint={endpoint}: {v}"
                ));
            }
            return Ok(v);
        }

        Err(anyhow!(
            "pump_amm tx_history rate-limited endpoint={endpoint} method={method}"
        ))
    }

    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let mut last_err: Option<anyhow::Error> = None;
        for endpoint in self.discovery_endpoints() {
            // Retry on rate-limits; fall back to the next endpoint if still blocked.
            let mut backoff_ms = 250u64;
            let max_attempts = if self.is_helius_endpoint(endpoint) {
                10usize
            } else {
                2usize
            };
            for attempt in 0..max_attempts {
                let _helius_guard = self.helius_throttle_guard(endpoint).await;
                let resp = match self.http.post(endpoint).json(&body).send().await {
                    Ok(r) => r,
                    Err(e) => {
                        last_err = Some(anyhow!(
                            "pump_amm rpc http error endpoint={endpoint} attempt={attempt}: {e}"
                        ));
                        break;
                    }
                };

                let status = resp.status();
                let retry_after_ms = Self::parse_retry_after_ms(&resp);
                let text = match resp.text().await {
                    Ok(t) => t,
                    Err(e) => {
                        last_err = Some(anyhow!(
                            "pump_amm rpc read body error endpoint={endpoint} attempt={attempt}: {e}"
                        ));
                        break;
                    }
                };
                let v: Value = match serde_json::from_str(&text) {
                    Ok(v) => v,
                    Err(e) => {
                        last_err = Some(anyhow!(
                            "pump_amm rpc json decode error endpoint={endpoint} attempt={attempt}: {e} body={text}"
                        ));
                        break;
                    }
                };

                let is_rate_limited = status == StatusCode::TOO_MANY_REQUESTS
                    || v.get("error")
                        .and_then(|e| e.get("code"))
                        .and_then(|c| c.as_i64())
                        == Some(-32429)
                    || v.get("error")
                        .and_then(|e| e.get("message"))
                        .and_then(|m| m.as_str())
                        .map(|m| m.to_ascii_lowercase().contains("rate"))
                        .unwrap_or(false);

                if is_rate_limited {
                    last_err = Some(anyhow!(
                        "pump_amm rpc http status {status} endpoint={endpoint}: {v}"
                    ));
                    let wait_ms = retry_after_ms.unwrap_or(backoff_ms);
                    sleep(Duration::from_millis(wait_ms)).await;
                    backoff_ms = (backoff_ms * 2).min(10_000);
                    continue;
                }

                if !status.is_success() {
                    last_err = Some(anyhow!(
                        "pump_amm rpc http status {status} endpoint={endpoint}: {v}"
                    ));
                    break;
                }
                if v.get("error").is_some() {
                    last_err = Some(anyhow!("pump_amm rpc error endpoint={endpoint}: {v}"));
                    break;
                }
                return Ok(v);
            }
        }

        Err(last_err
            .unwrap_or_else(|| anyhow!("pump_amm rpc call failed method={method} (no endpoints)")))
    }

    async fn rpc_get_account_owner_and_executable(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool)>> {
        let v = self
            .rpc_call(
                "getAccountInfo",
                json!([
                    address.to_string(),
                    {"encoding": "base64", "commitment": "confirmed"}
                ]),
            )
            .await?;

        let value = match v.get("result").and_then(|r| r.get("value")) {
            Some(v) => v,
            None => return Ok(None),
        };
        if value.is_null() {
            return Ok(None);
        }

        let owner = value
            .get("owner")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("getAccountInfo missing owner"))?;
        let executable = value
            .get("executable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        Ok(Some((Pubkey::from_str(owner)?, executable)))
    }

    async fn rpc_get_account_owner_executable_and_data(
        &self,
        address: Pubkey,
    ) -> Result<Option<(Pubkey, bool, Vec<u8>)>> {
        let v = self
            .rpc_call(
                "getAccountInfo",
                json!([
                    address.to_string(),
                    {"encoding": "base64", "commitment": "confirmed"}
                ]),
            )
            .await?;

        let value = match v.get("result").and_then(|r| r.get("value")) {
            Some(v) => v,
            None => return Ok(None),
        };
        if value.is_null() {
            return Ok(None);
        }

        let owner = value
            .get("owner")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("getAccountInfo missing owner"))?;
        let executable = value
            .get("executable")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let data_b64 = value
            .get("data")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("getAccountInfo missing data"))?;

        let data = BASE64_STD
            .decode(data_b64)
            .map_err(|e| anyhow!("base64 decode getAccountInfo data: {e}"))?;

        Ok(Some((Pubkey::from_str(owner)?, executable, data)))
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

    async fn find_token_account_by_owner_and_mint(
        &self,
        token_program: Pubkey,
        token_owner: Pubkey,
        mint: Pubkey,
    ) -> Result<Option<Pubkey>> {
        // Fallback for cases where the recipient token account is not an ATA.
        // Avoid `getProgramAccounts` over the token program (can be huge / Helius may reject).
        // Prefer `getTokenAccountsByOwner` with a mint filter.

        let params_mint_filtered = json!([
            token_owner.to_string(),
            {"mint": mint.to_string()},
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "dataSlice": {"offset": 0, "length": 0}
            }
        ]);

        let v = self
            .rpc_call("getTokenAccountsByOwner", params_mint_filtered)
            .await?;

        if let Some(arr) = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
        {
            for item in arr {
                if let Some(pk) = item.get("pubkey").and_then(|p| p.as_str()) {
                    if let Ok(parsed) = Pubkey::from_str(pk) {
                        return Ok(Some(parsed));
                    }
                }
            }
        }

        // Some RPCs may not return Token-2022 accounts for the mint-filtered variant.
        // As a fallback, query by programId and filter client-side using a small dataSlice.
        let params_programid_filtered = json!([
            token_owner.to_string(),
            {"programId": token_program.to_string()},
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "dataSlice": {"offset": 0, "length": 72}
            }
        ]);

        let v = self
            .rpc_call("getTokenAccountsByOwner", params_programid_filtered)
            .await?;

        let Some(arr) = v
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
        else {
            return Ok(None);
        };

        for item in arr {
            let Some(pk) = item.get("pubkey").and_then(|p| p.as_str()) else {
                continue;
            };
            let Some(data_arr) = item
                .get("account")
                .and_then(|a| a.get("data"))
                .and_then(|d| d.as_array())
            else {
                continue;
            };
            let Some(b64) = data_arr.first().and_then(|v| v.as_str()) else {
                continue;
            };
            let Ok(acc_data) = BASE64_STD.decode(b64) else {
                continue;
            };
            let Some((acc_mint, acc_owner)) =
                Self::parse_spl_token_account_mint_and_owner(&acc_data)
            else {
                continue;
            };
            if acc_mint == mint && acc_owner == token_owner {
                if let Ok(parsed) = Pubkey::from_str(pk) {
                    return Ok(Some(parsed));
                }
            }
        }

        Ok(None)
    }

    async fn find_any_token_account_for_owner_and_mint(
        &self,
        token_owner: Pubkey,
        mint: Pubkey,
        token_program: Pubkey,
        token_2022_program: Pubkey,
    ) -> Result<Option<Pubkey>> {
        // Prefer the legacy SPL Token program first.
        if let Some(ta) = self
            .find_token_account_by_owner_and_mint(token_program, token_owner, mint)
            .await?
        {
            return Ok(Some(ta));
        }
        self.find_token_account_by_owner_and_mint(token_2022_program, token_owner, mint)
            .await
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
        let pump_amm_program = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;
        let expected_quote_mint = Pubkey::from_str(WSOL_MINT)?;

        let Some((owner, executable, data)) = self
            .rpc_get_account_owner_executable_and_data(pool_market)
            .await?
        else {
            return Ok(None);
        };
        if executable || owner != pump_amm_program {
            return Ok(None);
        }

        // Require at least the global_config + base + quote mints.
        let min_len = PUMPFUN_AMM_MARKET_GLOBAL_CONFIG_OFFSET + (32 * 3);
        if data.len() < min_len {
            return Ok(None);
        }

        let global_config = Pubkey::new_from_array(
            data[PUMPFUN_AMM_MARKET_GLOBAL_CONFIG_OFFSET
                ..PUMPFUN_AMM_MARKET_GLOBAL_CONFIG_OFFSET + 32]
                .try_into()
                .map_err(|_| anyhow!("market global_config slice"))?,
        );
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

                // 2) Slow path: find any token account owned by cand for mint (non-ATA).
                if let Some(ta) = self
                    .find_any_token_account_for_owner_and_mint(
                        cand,
                        mint,
                        token_program,
                        token_2022_program,
                    )
                    .await?
                {
                    return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((cand, ta)));
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
                        let combined_list = combined
                            .iter()
                            .map(|p| p.to_string())
                            .collect::<Vec<_>>()
                            .join(",");
                        // Cannot construct swap instruction without protocol_fee_recipient_ta.
                        // Skip this pool rather than failing hard.
                        eprintln!(
                            "pump_amm market parse: no protocol fee recipient token account, skipping pool. \
                             market={pool_market} global_config={global_config} tried_mints=[{quote_mint},{base_mint}] \
                             authority_candidates_count={} authority_candidates=[{}] fallback={}",
                            combined.len(),
                            combined_list,
                            fallback_recipient,
                        );
                        return Ok(None);
                    }
                }
            }
        };

        // Creator vault ATA: prefer an embedded base token account; otherwise derive ATA.
        let (coin_creator_vault_authority, coin_creator_vault_ata) = if let Some(t) =
            base_token_accounts
                .iter()
                .find(|t| t.address != pool_base_vault)
        {
            (t.token_owner, t.address)
        } else if let Some((auth, ta)) =
            find_authority_with_existing_token_account(authority_candidates.clone(), base_mint)
                .await?
        {
            (auth, ta)
        } else {
            // Try deriving creator vault authority as PDA from AMM program with common seed patterns
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
                    // Derive ATA for creator vault
                    let derived_ata = Self::derive_ata(derived_authority, base_mint);

                    // Verify ATA exists
                    match self
                        .rpc_get_account_owner_and_executable(derived_ata)
                        .await?
                    {
                        Some(_) => (derived_authority, derived_ata),
                        None => {
                            eprintln!(
                                "pump_amm market parse: no creator vault token account (no embedded creator ATA; no ATA found; PDA derivation failed). \
                                 market={pool_market} base_mint={base_mint} authority_candidates_count={}",
                                authority_candidates.len()
                            );
                            return Ok(None);
                        }
                    }
                }
                None => {
                    eprintln!(
                        "pump_amm market parse: no creator vault token account (no embedded creator ATA; no ATA found; no valid PDA). \
                         market={pool_market} base_mint={base_mint} authority_candidates_count={}",
                        authority_candidates.len()
                    );
                    return Ok(None);
                }
            }
        };

        // Derive remaining PDAs with a small set of common seed patterns and validate existence.
        let event_authority = {
            // Prefer the canonical Anchor seed "__event_authority".
            let candidate =
                Pubkey::find_program_address(&[b"__event_authority"], &pump_amm_program).0;
            if non_token_pubkeys.contains(&candidate) {
                candidate
            } else {
                // Fallback to the common seed without leading underscores.
                Pubkey::find_program_address(&[b"event_authority"], &pump_amm_program).0
            }
        };

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
            // Fallback: try deriving from AMM program
            self.derive_existing_pda(
                pump_amm_program,
                &[
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

        if fee_config == Pubkey::default() || global_volume_accumulator == Pubkey::default() {
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
                        eprintln!(
                            "pump_amm market parse: could not derive protocol_fee_recipient PDA, skipping pool. \
                             market={pool_market} fee_program={fee_program} fee_config={fee_config}",
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
                        eprintln!(
                            "pump_amm market parse: derived protocol_fee_recipient_ta does not exist, skipping pool. \
                             recipient={derived_recipient} ta={derived_ta} market={pool_market}",
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

    async fn discover_pool_markets_via_program_accounts(
        &self,
        base_mint: Pubkey,
    ) -> Result<Vec<Pubkey>> {
        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

        let params = json!([
            program_id.to_string(),
            {
                "encoding": "base64",
                "commitment": "confirmed",
                "dataSlice": {"offset": 0, "length": 0},
                "filters": [
                    {"memcmp": {"offset": PUMPFUN_AMM_MARKET_BASE_MINT_OFFSET, "bytes": base_mint.to_string()}},
                    {"memcmp": {"offset": PUMPFUN_AMM_MARKET_QUOTE_MINT_OFFSET, "bytes": WSOL_MINT}},
                ]
            }
        ]);

        // NOTE: On our local validator RPC, getProgramAccounts can be disabled for large programs
        // ("excluded from account secondary indexes"). When Helius is configured, always use it
        // for program-account discovery.
        let v = if self.helius_rpc_url.is_some() {
            self.rpc_call_tx_history("getProgramAccounts", params)
                .await?
        } else {
            self.rpc_call("getProgramAccounts", params).await?
        };

        let arr = match v.get("result").and_then(|r| r.as_array()) {
            Some(v) => v,
            None => return Ok(Vec::new()),
        };

        // Some mints can have multiple matching market accounts (re-created markets,
        // migrations, etc). We return all matches and let tx-history scanning pick
        // the first one that yields a valid swap account set.
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            let Some(pk) = item.get("pubkey").and_then(|v| v.as_str()) else {
                continue;
            };
            if let Ok(p) = Pubkey::from_str(pk) {
                out.push(p);
            }
        }
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

        let addr = pool_market.to_string();

        info!(
            "pump_amm TX-history: starting getSignaturesForAddress for market={} base_mint={} limit=200",
            pool_market, base_mint
        );

        let sigs_v = self
            .rpc_call_tx_history("getSignaturesForAddress", json!([addr, {"limit": 200}]))
            .await?;

        info!(
            "pump_amm TX-history: received getSignaturesForAddress response for market={}",
            pool_market
        );

        // Check for RPC errors - returnOk(None) triggers generic error message upstream
        if let Some(err) = sigs_v.get("error") {
            // RPC error (e.g., method not found) - return as anyhow error with details
            return Err(anyhow!(
                "pump_amm tx_history RPC error market={} error={}",
                pool_market,
                serde_json::to_string(err).unwrap_or_else(|_| "unknown".to_string())
            ));
        }

        let sigs = match sigs_v.get("result").and_then(|v| v.as_array()) {
            Some(v) => v,
            None => {
                // Unexpected response structure
                warn!(
                    "pump_amm TX-history: unexpected response from getSignaturesForAddress market={} response={}",
                    pool_market,
                    serde_json::to_string(&sigs_v).unwrap_or_else(|_| "unknown".to_string())
                );
                return Err(anyhow!(
                    "pump_amm tx_history unexpected response market={} response={}",
                    pool_market,
                    serde_json::to_string(&sigs_v).unwrap_or_else(|_| "unknown".to_string())
                ));
            }
        };

        info!(
            "pump_amm TX-history: found {} signatures for market={} base_mint={}, starting transaction scan...",
            sigs.len(), pool_market, base_mint
        );

        if sigs.is_empty() {
            // No transactions found - this is expected for brand-new pools, return None
            info!(
                "pump_amm TX-history: no signatures found for market={}, returning None",
                pool_market
            );
            return Ok(None);
        }

        // Cap transaction fetches (sequential scan is more reliable than sampling for thin history).
        // Increased from 60 to 200 to handle markets with sparse/old swap history.
        const MAX_TX_FETCHES: usize = 200;

        let mut fetched = 0usize;
        let mut scanned_tx_count = 0usize;
        const DEBUG_REF_TX: &str = "3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY";

        // Log first few signatures for debugging
        info!(
            "pump_amm TX-history: first 10 signatures: {:?}",
            sigs.iter()
                .take(10)
                .filter_map(|s| s.get("signature").and_then(|v| v.as_str()))
                .collect::<Vec<_>>()
        );

        for s in sigs.iter() {
            if fetched >= MAX_TX_FETCHES {
                break;
            }
            if let Some(err) = s.get("err") {
                if !err.is_null() {
                    continue;
                }
            }
            let sig = match s.get("signature").and_then(|v| v.as_str()) {
                Some(v) => v,
                None => continue,
            };

            // Debug: Check if reference TX is in signature list
            if sig == DEBUG_REF_TX {
                info!(
                    "pump_amm TX-history: FOUND reference TX in signature list! sig={}",
                    sig
                );
            }

            fetched += 1;

            // Log progress every 20 transactions
            if fetched % 20 == 0 {
                info!(
                    "pump_amm TX-history: scanned {}/{} transactions for market={}...",
                    fetched,
                    sigs.len(),
                    pool_market
                );
            }

            let tx_v = self
                .rpc_call_tx_history(
                    "getTransaction",
                    json!([sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]),
                )
                .await?;

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

    async fn discover_pool_static(&self, base_mint: Pubkey) -> Result<Option<PumpAmmPoolStatic>> {
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

        // Avoid concurrent discovery attempts for the same base mint.
        // This significantly reduces RPC rate-limits when `parallel_exits` is enabled.
        let _guard = self.discovery_lock.lock().await;
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

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
            const SIG_PAGE_SIZE: u32 = 200;
            const SIG_MAX_PAGES: usize = 100; // up to ~20k signatures
            const SIG_TX_PER_PAGE: usize = 40; // cap getTransaction calls per page
            let mut before: Option<String> = None;

            for _page in 0..SIG_MAX_PAGES {
                let mut cfg = json!({"limit": SIG_PAGE_SIZE});
                if let Some(b) = &before {
                    cfg["before"] = json!(b);
                }

                let sigs_v = match self
                    .rpc_call_tx_history("getSignaturesForAddress", json!([addr, cfg]))
                    .await
                {
                    Ok(v) => v,
                    Err(e) => {
                        discovery_err = Some(e);
                        break;
                    }
                };

                let sigs = match sigs_v.get("result").and_then(|v| v.as_array()) {
                    Some(v) => v,
                    None => break,
                };
                if sigs.is_empty() {
                    break;
                }

                // Update pagination cursor (last signature in the page)
                before = sigs
                    .last()
                    .and_then(|v| v.get("signature"))
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string());

                // Sample across the whole page (not just the newest N), so we don't miss the
                // relevant swap when the address is busy.
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
                    // Avoid poisoning discovery with failed transactions (e.g., our own earlier
                    // liquidation attempts that used wrong accounts and therefore failed).
                    if let Some(err) = s.get("err") {
                        if !err.is_null() {
                            continue;
                        }
                    }
                    let sig = match s.get("signature").and_then(|v| v.as_str()) {
                        Some(v) => v,
                        None => continue,
                    };

                    let tx_v = match self
                        .rpc_call_tx_history(
                            "getTransaction",
                            json!([sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]),
                        )
                        .await
                    {
                        Ok(v) => v,
                        Err(e) => {
                            discovery_err = Some(e);
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
        let sigs_v = self
            .rpc_call(
                "getSignaturesForAddress",
                json!([user.to_string(), {"limit": 500}]),
            )
            .await?;
        let sigs = match sigs_v.get("result").and_then(|v| v.as_array()) {
            Some(v) => v,
            None => return Ok(None),
        };

        for s in sigs.iter() {
            // Prefer successful transactions; failed ones can contain partial/invalid account sets.
            if let Some(err) = s.get("err") {
                if !err.is_null() {
                    continue;
                }
            }
            let sig = match s.get("signature").and_then(|v| v.as_str()) {
                Some(v) => v,
                None => continue,
            };

            let tx_v = match self
                .rpc_call(
                    "getTransaction",
                    json!([sig, {"encoding": "json", "maxSupportedTransactionVersion": 0}]),
                )
                .await
            {
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
        let bal = self
            .rpc
            .rpc
            .get_token_account_balance(&ta)
            .await
            .map_err(|e| anyhow!("get_token_account_balance failed: {e}"))?;
        let amount_str = bal
            .amount
            .parse::<u64>()
            .map_err(|e| anyhow!("invalid token balance amount: {e}"))?;
        Ok(amount_str)
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
    /// **ARCHITECTURE COMPLIANCE (TARGET_ARCHITECTURE.md Section 4.2):**
    /// This is a single getAccount call (acceptable) to pre-load a pool
    /// that arb-strategy discovered and passed via Intent metadata.
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
        let min_len = PUMPFUN_AMM_MARKET_GLOBAL_CONFIG_OFFSET + (32 * 3);
        if account.data.len() < min_len {
            return Err(anyhow!(
                "pump_amm pool {} data too short: {} < {}",
                pool_address,
                account.data.len(),
                min_len
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

        // Use existing method to parse full pool structure
        match self
            .try_parse_pool_static_from_market_account(*pool_address, base_mint)
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
            Ok(None) => Err(anyhow!(
                "pump_amm pool {} could not be parsed (returned None)",
                pool_address
            )),
            Err(e) => Err(anyhow!(
                "pump_amm pool {} parse failed: {}",
                pool_address,
                e
            )),
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
    /// [6] protocol_fee_recipient
    /// [7] protocol_fee_recipient_ta
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
        let global_config = parse_pubkey(&accounts[1], "global_config")?;
        let base_mint = parse_pubkey(&accounts[2], "base_mint")?;
        let quote_mint = parse_pubkey(&accounts[3], "quote_mint")?;
        let pool_base_vault = parse_pubkey(&accounts[4], "pool_base_vault")?;
        let pool_quote_vault = parse_pubkey(&accounts[5], "pool_quote_vault")?;
        let protocol_fee_recipient = parse_pubkey(&accounts[6], "protocol_fee_recipient")?;
        let protocol_fee_recipient_ta = parse_pubkey(&accounts[7], "protocol_fee_recipient_ta")?;
        let event_authority = parse_pubkey(&accounts[8], "event_authority")?;
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
        let mut metas = vec![
            AccountMeta::new(pool.pool_market, false),            // 0
            AccountMeta::new(user, true),                         // 1
            AccountMeta::new_readonly(pool.global_config, false), // 2
            AccountMeta::new_readonly(pool.base_mint, false),     // 3
            AccountMeta::new_readonly(pool.quote_mint, false),    // 4
            AccountMeta::new(user_base_ta, false),                // 5
            AccountMeta::new(user_quote_ta, false),               // 6
            AccountMeta::new(pool.pool_base_vault, false),        // 7
            AccountMeta::new(pool.pool_quote_vault, false),       // 8
            AccountMeta::new_readonly(pool.protocol_fee_recipient, false), // 9
            AccountMeta::new(pool.protocol_fee_recipient_ta, false), // 10
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 11
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 12
            AccountMeta::new_readonly(
                Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                false,
            ), // 13
            AccountMeta::new_readonly(
                Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                false,
            ), // 14
            AccountMeta::new_readonly(pool.event_authority, false), // 15
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
    /// [6] protocol_fee_recipient
    /// [7] protocol_fee_recipient_ta
    /// [8] event_authority
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    pub fn build_swap_ix_from_pool_accounts(
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
        user: Pubkey,
        pool_accounts: &[Pubkey],
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
        let global_config = pool_accounts[1];
        let base_mint = pool_accounts[2];
        let quote_mint = pool_accounts[3];
        let pool_base_vault = pool_accounts[4];
        let pool_quote_vault = pool_accounts[5];
        let protocol_fee_recipient = pool_accounts[6];
        let protocol_fee_recipient_ta = pool_accounts[7];
        let event_authority = pool_accounts[8];
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

        // User token accounts are deterministic ATAs.
        let user_base_ta = Self::derive_ata(user, base_mint);
        let user_quote_ta = Self::derive_ata(user, quote_mint);

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
                AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 11
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
                AccountMeta::new(global_volume_accumulator, false), // 16 - REQUIRED for BUY!
                AccountMeta::new(coin_creator_vault_ata, false),   // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
                AccountMeta::new(user_vol, false),                 // 19 - user volume accumulator
                AccountMeta::new_readonly(fee_config, false),      // 20
                AccountMeta::new_readonly(fee_program, false),     // 21
                AccountMeta::new_readonly(program_id, false),      // 22
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
                AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 11
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
                AccountMeta::new_readonly(program_id, false),      // 16
                AccountMeta::new(coin_creator_vault_ata, false),   // 17
                AccountMeta::new_readonly(coin_creator_vault_authority, false), // 18
                AccountMeta::new_readonly(fee_config, false),      // 19
                AccountMeta::new_readonly(fee_program, false),     // 20
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
