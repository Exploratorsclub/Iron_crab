use crate::solana::dex::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;
use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dashmap::DashMap;
use reqwest::Client;
use serde_json::{json, Value};
use solana_sdk::hash::hash;
use solana_sdk::instruction::{AccountMeta, Instruction};
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::str::FromStr;
use std::sync::Arc;

const WSOL_MINT: &str = "So11111111111111111111111111111111111111112";
const PUMPFUN_AMM_PROGRAM_ID: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";

// Observed on-chain: buy_exact_quote_in fee fields sum to 125 bps (lp 2 + protocol 93 + creator 30).
// We use that as a conservative default for quoting.
const DEFAULT_TOTAL_FEE_BPS: u32 = 125;

fn anchor_disc(ix_name: &str) -> [u8; 8] {
    let out = hash(format!("global:{ix_name}").as_bytes());
    let mut disc = [0u8; 8];
    disc.copy_from_slice(&out.as_ref()[..8]);
    disc
}

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

#[derive(Clone)]
pub struct PumpFunAmmDex {
    rpc: Arc<SolanaRpc>,
    rpc_url: String,
    helius_rpc_url: Option<String>,
    http: Client,
    user_authority: Option<Pubkey>,

    // Cache by base mint (WSOL quote only for now)
    pools_by_base: DashMap<Pubkey, PumpAmmPoolStatic>,
    user_accounts: DashMap<(Pubkey, Pubkey), PumpAmmUserAccounts>, // (pool_market, user)
}

impl PumpFunAmmDex {
    pub fn new(rpc: Arc<SolanaRpc>, rpc_url: String, helius_rpc_url: Option<String>) -> Self {
        Self {
            rpc,
            rpc_url,
            helius_rpc_url,
            http: Client::new(),
            user_authority: None,
            pools_by_base: DashMap::new(),
            user_accounts: DashMap::new(),
        }
    }

    pub fn set_user_authority(&mut self, user: Pubkey) {
        self.user_authority = Some(user);
    }

    /// Return the deterministic v1 pool-accounts list for a Pump.fun AMM pool.
    ///
    /// Ordering matches `MarketEventKind::DexPoolAccounts` (PumpSwap v1) and
    /// `PumpFunAmmDex::build_swap_ix_from_pool_accounts`.
    pub async fn pool_accounts_v1_for_base_mint(
        &self,
        base_mint: Pubkey,
    ) -> Result<Option<Vec<Pubkey>>> {
        let pool = match self.discover_pool_static(base_mint).await? {
            Some(p) => p,
            None => return Ok(None),
        };

        Ok(Some(vec![
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

    fn rpc_endpoint_for_discovery(&self) -> &str {
        self.helius_rpc_url
            .as_deref()
            .unwrap_or(self.rpc_url.as_str())
    }

    async fn rpc_call(&self, method: &str, params: Value) -> Result<Value> {
        let body = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });
        let resp = self
            .http
            .post(self.rpc_endpoint_for_discovery())
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("pump_amm rpc http error: {e}"))?;
        let status = resp.status();
        let v: Value = resp
            .json()
            .await
            .map_err(|e| anyhow!("pump_amm rpc json decode error: {e}"))?;
        if !status.is_success() {
            return Err(anyhow!("pump_amm rpc http status {status}: {v}"));
        }
        if v.get("error").is_some() {
            return Err(anyhow!("pump_amm rpc error: {v}"));
        }
        Ok(v)
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

    async fn discover_pool_static(&self, base_mint: Pubkey) -> Result<Option<PumpAmmPoolStatic>> {
        if let Some(v) = self.pools_by_base.get(&base_mint) {
            return Ok(Some(v.clone()));
        }

        let pump_amm_program_id =
            Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID).context("invalid PUMPFUN_AMM_PROGRAM_ID")?;

        // TX-based discovery: scan transactions touching the mint.
        // We scan deeper than the default 50 because the newest signatures can be dominated
        // by failed liquidation attempts (which we intentionally skip).
        let sigs_v = self
            .rpc_call(
                "getSignaturesForAddress",
                json!([base_mint.to_string(), {"limit": 500}]),
            )
            .await?;
        let sigs = match sigs_v.get("result").and_then(|v| v.as_array()) {
            Some(v) => v,
            None => return Ok(None),
        };

        for s in sigs.iter() {
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

            let account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let instructions = match msg.get("instructions").and_then(|v| v.as_array()) {
                Some(v) => v,
                None => continue,
            };

            for ix in instructions {
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
                if accounts.len() != 23 {
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
                    protocol_fee_recipient_ta: Pubkey::from_str(&account_keys[accounts[10]])?,
                    event_authority: Pubkey::from_str(&account_keys[accounts[15]])?,
                    coin_creator_vault_ata: Pubkey::from_str(&account_keys[accounts[17]])?,
                    coin_creator_vault_authority: Pubkey::from_str(&account_keys[accounts[18]])?,
                    global_volume_accumulator: Pubkey::from_str(&account_keys[accounts[19]])?,
                    // These last two accounts have shown variants across tx layouts.
                    // We resolve them below by checking which one is executable.
                    fee_config: Pubkey::default(),
                    fee_program: Pubkey::default(),
                };

                // Robustly resolve (fee_config, fee_program) from the final two accounts.
                // `fee_program` must be executable; `fee_config` must be owned by the pump_amm program.
                let a = Pubkey::from_str(&account_keys[accounts[21]])?;
                let b = Pubkey::from_str(&account_keys[accounts[22]])?;

                let (a_owner, a_exec) = match self.rpc_get_account_owner_and_executable(a).await? {
                    Some(v) => v,
                    None => continue,
                };
                let (b_owner, b_exec) = match self.rpc_get_account_owner_and_executable(b).await? {
                    Some(v) => v,
                    None => continue,
                };

                let (fee_program, fee_config, fee_config_owner) = match (a_exec, b_exec) {
                    (true, false) => (a, b, b_owner),
                    (false, true) => (b, a, a_owner),
                    _ => continue,
                };
                if fee_config_owner != pump_amm_program_id {
                    continue;
                }

                let mut pool = pool;
                pool.fee_program = fee_program;
                pool.fee_config = fee_config;

                self.pools_by_base.insert(base_mint, pool.clone());
                return Ok(Some(pool));
            }
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

            let account_keys = match Self::parse_account_keys(msg) {
                Ok(v) => v,
                Err(_) => continue,
            };
            let instructions = match msg.get("instructions").and_then(|v| v.as_array()) {
                Some(v) => v,
                None => continue,
            };

            for ix in instructions {
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
                if accounts.len() != 23 {
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
        let owner_spl = SplProgramPubkey::new_from_array(owner.to_bytes());
        let mint_spl = SplProgramPubkey::new_from_array(mint.to_bytes());
        let token_program = spl_token::id();
        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &owner_spl,
            &mint_spl,
            &token_program,
        );
        Pubkey::new_from_array(ata_spl.to_bytes())
    }
}

#[async_trait]
impl Dex for PumpFunAmmDex {
    async fn refresh_pools(&self) -> Result<()> {
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
        let user_base_ta = user_acc
            .as_ref()
            .map(|u| u.user_base_ta)
            .unwrap_or_else(|| Self::derive_ata(user, pool.base_mint));
        let user_quote_ta = user_acc
            .as_ref()
            .map(|u| u.user_quote_ta)
            .unwrap_or_else(|| Self::derive_ata(user, pool.quote_mint));
        let user_vol = user_acc
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

        // Account ordering is taken from an observed on-chain Pump.fun AMM swap transaction.
        let metas = vec![
            AccountMeta::new(pool.pool_market, false),
            AccountMeta::new(user, true),
            AccountMeta::new_readonly(pool.global_config, false),
            AccountMeta::new_readonly(pool.base_mint, false),
            AccountMeta::new_readonly(pool.quote_mint, false),
            AccountMeta::new(user_base_ta, false),
            AccountMeta::new(user_quote_ta, false),
            AccountMeta::new(pool.pool_base_vault, false),
            AccountMeta::new(pool.pool_quote_vault, false),
            AccountMeta::new_readonly(pool.protocol_fee_recipient, false),
            AccountMeta::new(pool.protocol_fee_recipient_ta, false),
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
            AccountMeta::new_readonly(
                Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                false,
            ),
            AccountMeta::new_readonly(pool.event_authority, false),
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new(pool.coin_creator_vault_ata, false),
            AccountMeta::new_readonly(pool.coin_creator_vault_authority, false),
            AccountMeta::new(pool.global_volume_accumulator, false),
            AccountMeta::new(user_vol, false),
            AccountMeta::new_readonly(pool.fee_config, false),
            AccountMeta::new_readonly(pool.fee_program, false),
        ];

        Ok(vec![Instruction {
            program_id,
            accounts: metas,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        Vec::new()
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
        if pool_accounts.len() != 14 {
            return Err(anyhow!(
                "pump_amm expected 14 pool_accounts (v1), got {}",
                pool_accounts.len()
            ));
        }

        let program_id = Pubkey::from_str(PUMPFUN_AMM_PROGRAM_ID)?;

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
        let global_volume_accumulator = pool_accounts[11];
        let fee_config = pool_accounts[12];
        let fee_program = pool_accounts[13];

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

        // User volume accumulator is a PDA; derive deterministically.
        let user_vol = Self::derive_user_volume_accumulator(program_id, pool_market, user);

        let disc = if is_buy {
            anchor_disc("buy_exact_quote_in")
        } else {
            anchor_disc("sell")
        };
        let data = Self::build_ix_data(disc, amount_in, min_out);

        // Account ordering is taken from an observed on-chain Pump.fun AMM swap transaction.
        let metas = vec![
            AccountMeta::new(pool_market, false),
            AccountMeta::new(user, true),
            AccountMeta::new_readonly(global_config, false),
            AccountMeta::new_readonly(base_mint, false),
            AccountMeta::new_readonly(quote_mint, false),
            AccountMeta::new(user_base_ta, false),
            AccountMeta::new(user_quote_ta, false),
            AccountMeta::new(pool_base_vault, false),
            AccountMeta::new(pool_quote_vault, false),
            AccountMeta::new_readonly(protocol_fee_recipient, false),
            AccountMeta::new(protocol_fee_recipient_ta, false),
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
            AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false),
            AccountMeta::new_readonly(
                Pubkey::new_from_array(solana_system_program::id().to_bytes()),
                false,
            ),
            AccountMeta::new_readonly(
                Pubkey::new_from_array(spl_associated_token_account::id().to_bytes()),
                false,
            ),
            AccountMeta::new_readonly(event_authority, false),
            AccountMeta::new_readonly(program_id, false),
            AccountMeta::new(coin_creator_vault_ata, false),
            AccountMeta::new_readonly(coin_creator_vault_authority, false),
            AccountMeta::new(global_volume_accumulator, false),
            AccountMeta::new(user_vol, false),
            AccountMeta::new_readonly(fee_config, false),
            AccountMeta::new_readonly(fee_program, false),
        ];

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
