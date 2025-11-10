//! Orca Connector – Skeleton (Whirlpool/Classic)

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use std::str::FromStr;
use std::sync::Arc;

use super::{Dex, Quote};
use crate::solana::rpc::SolanaRpc;
use solana_sdk::instruction::Instruction;

pub const ORCA_WHIRLPOOL_PROGRAM: &str = "whirLbMiicV3QDeqAD9nukHf8stYwh5GozfX6rS3SAm"; // verify
                                                                                        // Approximate Whirlpool account size (may vary by version). Adjust if mismatch logged repeatedly.
pub const WHIRLPOOL_ACCOUNT_MIN_SIZE: usize = 400; // conservative lower bound
pub const WHIRLPOOL_ACCOUNT_MAX_SIZE: usize = 800; // safety upper bound

use super::orca_whirlpool_layout as layout;
use dashmap::DashMap;
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Debug)]
struct OrcaPool {
    base_mint: Pubkey,
    quote_mint: Pubkey,
    reserve_base: u128,
    reserve_quote: u128,
    fee_bps: u32,
    fee_tier: Option<Pubkey>,
    tick_spacing: Option<u16>,
    vault_a: Pubkey,
    vault_b: Pubkey,
    tick_current_index: Option<i32>,
}

#[derive(Clone, Debug)]
pub struct OrcaPoolSnapshot {
    pub address: Pubkey,
    pub base_mint: Pubkey,
    pub quote_mint: Pubkey,
    pub reserve_base: u128,
    pub reserve_quote: u128,
    pub tick_spacing: Option<u16>,
    pub vault_a: Pubkey,
    pub vault_b: Pubkey,
}

pub struct Orca {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, OrcaPool>>, // keyed by a pseudo pool id (mint xor) for now
    fee_tiers: Arc<DashMap<Pubkey, (u32, u16)>>,
    user_authority: Arc<std::sync::RwLock<Option<Pubkey>>>,
    user_token_accounts: Arc<DashMap<Pubkey, Pubkey>>, // mint -> user token account (ATA)
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>,     // mint -> pools containing it
}

impl Orca {
    pub fn new(rpc: Arc<SolanaRpc>) -> Self {
        Self {
            rpc,
            pools: Arc::new(DashMap::new()),
            fee_tiers: Arc::new(DashMap::new()),
            user_authority: Arc::new(std::sync::RwLock::new(None)),
            user_token_accounts: Arc::new(DashMap::new()),
            mint_index: Arc::new(DashMap::new()),
        }
    }

    /// Set the global user authority (signer) used for swaps.
    pub fn set_user_authority(&self, auth: Pubkey) {
        *self.user_authority.write().unwrap() = Some(auth);
    }

    /// Register (or override) a user token account (ATA) for a given mint.
    pub fn set_user_token_account(&self, mint: Pubkey, ata: Pubkey) {
        self.user_token_accounts.insert(mint, ata);
    }

    fn find_pool(&self, input: &Pubkey, output: &Pubkey) -> Option<(Pubkey, bool, OrcaPool)> {
        for p in self.pools.iter() {
            let forward = p.base_mint == *input && p.quote_mint == *output;
            let reverse = p.base_mint == *output && p.quote_mint == *input;
            if forward || reverse {
                return Some((*p.key(), forward, p.clone()));
            }
        }
        None
    }

    pub fn insert_mock_pool(
        &self,
        base: Pubkey,
        quote: Pubkey,
        reserve_base: u128,
        reserve_quote: u128,
        fee_bps: u32,
    ) {
        self.pools.insert(
            base,
            OrcaPool {
                base_mint: base,
                quote_mint: quote,
                reserve_base,
                reserve_quote,
                fee_bps,
                fee_tier: None,
                tick_spacing: None,
                vault_a: Pubkey::new_unique(),
                vault_b: Pubkey::new_unique(),
                tick_current_index: None,
            },
        );
    }

    /// Pools that reference this mint.
    pub fn pools_for_mint(&self, mint: &Pubkey) -> Vec<Pubkey> {
        self.mint_index
            .get(mint)
            .map(|v| v.clone())
            .unwrap_or_default()
    }

    /// Return a lightweight snapshot of current pools (for read-only aggregation like liquidity indexing).
    pub fn pools_snapshot(&self) -> Vec<OrcaPoolSnapshot> {
        self.pools
            .iter()
            .map(|p| OrcaPoolSnapshot {
                address: *p.key(),
                base_mint: p.base_mint,
                quote_mint: p.quote_mint,
                reserve_base: p.reserve_base,
                reserve_quote: p.reserve_quote,
                tick_spacing: p.tick_spacing,
                vault_a: p.vault_a,
                vault_b: p.vault_b,
            })
            .collect()
    }

    /// Replay: refresh Orca (Whirlpool) pools from a ReplayRpc store.
    /// Scans latest accounts, decodes Whirlpool states, optionally reads vault balances if present,
    /// and populates in-memory snapshots similar to live refresh.
    pub fn refresh_pools_replay(
        &self,
        replay: &crate::backtest::replay_rpc::ReplayRpc,
    ) -> anyhow::Result<()> {
        // Clear current index but keep pools map; we'll upsert
        self.mint_index.clear();
        let all = replay.all_latest();
        let mut fee_tier_keys: Vec<Pubkey> = Vec::new();
        let mut added = 0u32;
        for (addr_str, bytes) in all.into_iter() {
            // Fast size gate to reduce decode attempts
            if bytes.len() < WHIRLPOOL_ACCOUNT_MIN_SIZE || bytes.len() > WHIRLPOOL_ACCOUNT_MAX_SIZE
            {
                continue;
            }
            if let Some(parsed) = layout::parse_whirlpool_strict(&bytes) {
                let address = Pubkey::from_str(&addr_str).unwrap_or_else(|_| Pubkey::new_unique());
                // Try to fetch vault balances from replay if vault accounts are present in trace
                let mut reserves = (0u128, 0u128);
                let vaults =
                    replay.get_multiple_accounts(&[parsed.token_vault_a, parsed.token_vault_b]);
                if let Some(Some(v1)) = vaults.first() {
                    if v1.data.len() >= 72 {
                        reserves.0 = Self::parse_token_amount(&v1.data) as u128;
                    }
                }
                if let Some(Some(v2)) = vaults.get(1) {
                    if v2.data.len() >= 72 {
                        reserves.1 = Self::parse_token_amount(&v2.data) as u128;
                    }
                }
                // Record fee tier to refine fee/tick spacing if available in trace
                fee_tier_keys.push(parsed.fee_tier);
                // Insert/overwrite pool
                self.pools.insert(
                    address,
                    OrcaPool {
                        base_mint: parsed.token_mint_a,
                        quote_mint: parsed.token_mint_b,
                        reserve_base: reserves.0,
                        reserve_quote: reserves.1,
                        fee_bps: parsed.fee_rate as u32, // may be overridden by fee tier account below
                        fee_tier: Some(parsed.fee_tier),
                        tick_spacing: Some(parsed.tick_spacing),
                        vault_a: parsed.token_vault_a,
                        vault_b: parsed.token_vault_b,
                        tick_current_index: Some(parsed.tick_current_index),
                    },
                );
                for m in [parsed.token_mint_a, parsed.token_mint_b] {
                    self.mint_index
                        .entry(m)
                        .or_insert_with(|| Vec::with_capacity(2))
                        .push(address);
                }
                added += 1;
            }
        }
        // Deduplicate and fetch fee tier accounts via replay to set authoritative fee_bps & tick_spacing
        fee_tier_keys.sort_unstable();
        fee_tier_keys.dedup();
        if !fee_tier_keys.is_empty() {
            let accts = replay.get_multiple_accounts(&fee_tier_keys);
            for (i, acc_opt) in accts.into_iter().enumerate() {
                if let Some(acc) = acc_opt {
                    if acc.data.len() >= 4 {
                        let tick = u16::from_le_bytes([acc.data[0], acc.data[1]]);
                        let fee = u16::from_le_bytes([acc.data[2], acc.data[3]]) as u32;
                        if (1..=1000).contains(&fee) {
                            self.fee_tiers.insert(fee_tier_keys[i], (fee, tick));
                        }
                    }
                }
            }
            // Patch pools with authoritative values
            for mut p in self.pools.iter_mut() {
                if let Some(key) = p.fee_tier {
                    if let Some((fee, tick)) = self.fee_tiers.get(&key).map(|v| *v) {
                        p.fee_bps = fee;
                        p.tick_spacing = Some(tick);
                    }
                }
            }
        }
        tracing::info!(
            added,
            total = self.pools.len(),
            "orca.refresh_pools_replay() done"
        );
        Ok(())
    }
}

#[async_trait]
impl Dex for Orca {
    async fn refresh_pools(&self) -> Result<()> {
        use crate::metrics::ORCA_POOLS_SKIPPED_ZERO_RESERVE;
        use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
        use solana_client::rpc_filter::RpcFilterType;
        use solana_sdk::pubkey::Pubkey;
        use std::str::FromStr;
        tracing::trace!("orca.refresh_pools() whirlpool fetch");
        let program_id = Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM)
            .map_err(|_| anyhow!("invalid whirlpool program id"))?;
        // DataSize filter (broad window) to reduce traffic
        let size_filter = RpcFilterType::DataSize(WHIRLPOOL_ACCOUNT_MAX_SIZE as u64);
        let filters = Some(vec![size_filter]);
        let acc_cfg = RpcAccountInfoConfig {
            encoding: None,
            data_slice: None,
            commitment: None,
            min_context_slot: None,
        };
        let cfg = RpcProgramAccountsConfig {
            filters,
            account_config: acc_cfg,
            with_context: None,
            sort_results: None,
        };
        let accounts = self
            .rpc
            .get_program_accounts_with_config_retry(&program_id, cfg)
            .await?;
        let mut added = 0u32;
        let mut fee_tier_keys: Vec<Pubkey> = Vec::new();
        self.mint_index.clear();
        for (addr, acc) in accounts.into_iter().take(5000) {
            // safety limit
            if let Some(parsed) = layout::parse_whirlpool_strict(&acc.data) {
                // Fetch vault balances (SPL token accounts) to approximate reserves
                let mut reserves = (0u128, 0u128);
                if let Ok(vaults) = self
                    .rpc
                    .rpc
                    .get_multiple_accounts(&[parsed.token_vault_a, parsed.token_vault_b])
                    .await
                {
                    if let Some(Some(v1)) = vaults.first().map(|o| o.as_ref()) {
                        if v1.data.len() >= 72 {
                            reserves.0 = Self::parse_token_amount(&v1.data) as u128;
                        }
                    }
                    if let Some(Some(v2)) = vaults.get(1).map(|o| o.as_ref()) {
                        if v2.data.len() >= 72 {
                            reserves.1 = Self::parse_token_amount(&v2.data) as u128;
                        }
                    }
                }
                if reserves.0 == 0 || reserves.1 == 0 {
                    ORCA_POOLS_SKIPPED_ZERO_RESERVE
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    continue;
                }
                fee_tier_keys.push(parsed.fee_tier);
                let id = addr;
                self.pools.insert(
                    id,
                    OrcaPool {
                        base_mint: parsed.token_mint_a,
                        quote_mint: parsed.token_mint_b,
                        reserve_base: reserves.0,
                        reserve_quote: reserves.1,
                        fee_bps: parsed.fee_rate as u32,
                        fee_tier: Some(parsed.fee_tier),
                        tick_spacing: Some(parsed.tick_spacing),
                        vault_a: parsed.token_vault_a,
                        vault_b: parsed.token_vault_b,
                        tick_current_index: Some(parsed.tick_current_index),
                    },
                );
                for m in [parsed.token_mint_a, parsed.token_mint_b] {
                    self.mint_index
                        .entry(m)
                        .or_insert_with(|| Vec::with_capacity(2))
                        .push(id);
                }
                added += 1;
            }
        }
        // Deduplicate & fetch fee tier accounts
        fee_tier_keys.sort_unstable();
        fee_tier_keys.dedup();
        if !fee_tier_keys.is_empty() {
            if let Ok(accts) = self.rpc.rpc.get_multiple_accounts(&fee_tier_keys).await {
                for (i, acc_opt) in accts.into_iter().enumerate() {
                    if let Some(acc) = acc_opt {
                        if acc.data.len() >= 4 {
                            let tick = u16::from_le_bytes([acc.data[0], acc.data[1]]);
                            let fee = u16::from_le_bytes([acc.data[2], acc.data[3]]) as u32;
                            if (1..=1000).contains(&fee) {
                                self.fee_tiers.insert(fee_tier_keys[i], (fee, tick));
                            }
                        }
                    }
                }
            }
            // Patch pools with authoritative values
            for mut p in self.pools.iter_mut() {
                if let Some(key) = p.fee_tier {
                    if let Some((fee, tick)) = self.fee_tiers.get(&key).map(|v| *v) {
                        p.fee_bps = fee;
                        p.tick_spacing = Some(tick);
                    }
                }
            }
        }
        tracing::info!(added, total = self.pools.len(), "orca.refresh_pools() done");
        Ok(())
    }

    async fn quote_exact_in(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
    ) -> Result<Option<Quote>> {
        use std::str::FromStr;
        let input = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let output = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let pool = match self.find_pool(&input, &output) {
            Some(p) => p,
            None => return Ok(None),
        };
        let (pid, forward, p) = pool;
        let (rin, rout) = if forward {
            (p.reserve_base, p.reserve_quote)
        } else {
            (p.reserve_quote, p.reserve_base)
        };
        if rin == 0 || rout == 0 {
            return Ok(None);
        }
        let amount_in_u = _amount_in as u128;
        let fee_bps_u = p.fee_bps as u128;
        let amount_less_fee = amount_in_u * (10_000 - fee_bps_u) / 10_000;
        let out = (amount_less_fee * rout) / (rin + amount_less_fee);
        if out == 0 {
            return Ok(None);
        }
        let impact_bps = ((amount_less_fee * 10_000) / (rin + amount_less_fee)) as u32;
        Ok(Some(Quote {
            amount_out: out as u64,
            price_impact_bps: impact_bps,
            route: vec![pid.to_string()],
            fee_bps: p.fee_bps,
            in_reserve: rin,
            out_reserve: rout,
            input_mint: (if forward { input } else { output }).to_string(),
            output_mint: (if forward { output } else { input }).to_string(),
            tick_spacing: p.tick_spacing,
        }))
    }

    fn build_swap_ix(
        &self,
        _input_mint: &str,
        _output_mint: &str,
        _amount_in: u64,
        _min_out: u64,
    ) -> Result<Vec<Instruction>> {
        use solana_sdk::instruction::AccountMeta as AM;
        use std::str::FromStr;
        let in_pk = Pubkey::from_str(_input_mint).map_err(|_| anyhow!("bad input mint"))?;
        let out_pk = Pubkey::from_str(_output_mint).map_err(|_| anyhow!("bad output mint"))?;
        let (pool_id, forward, pool) = self
            .find_pool(&in_pk, &out_pk)
            .ok_or_else(|| anyhow!("no orca pool for pair"))?;
        let spacing = pool.tick_spacing.unwrap_or(64) as i32;
        let tick_now = pool.tick_current_index.unwrap_or(0);
        let start0 = align_to_start(tick_now, spacing);
        let start1 = align_to_start(tick_now + spacing * 88, spacing);
        let start_1 = align_to_start(tick_now - spacing * 88, spacing);
        let tick_arrays = [start_1, start0, start1].map(|s| derive_tick_array_pda(&pool_id, s));
        let oracle = derive_oracle_pda(&pool_id);
        let mut data = Vec::with_capacity(1 + 8 + 8 + 1 + 16);
        data.push(0u8);
        data.extend_from_slice(&_amount_in.to_le_bytes());
        data.extend_from_slice(&_min_out.to_le_bytes());
        data.push(if forward { 1 } else { 0 });
        data.extend_from_slice(&0u128.to_le_bytes());
        let program_id =
            Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).map_err(|_| anyhow!("orca program id"))?;
        let mut accounts = vec![
            AM {
                pubkey: pool_id,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_a,
                is_signer: false,
                is_writable: true,
            },
            AM {
                pubkey: pool.vault_b,
                is_signer: false,
                is_writable: true,
            },
        ];
        if let Some(ft) = pool.fee_tier {
            accounts.push(AM {
                pubkey: ft,
                is_signer: false,
                is_writable: false,
            });
        }
        for t in &tick_arrays {
            accounts.push(AM {
                pubkey: *t,
                is_signer: false,
                is_writable: true,
            });
        }
        accounts.push(AM {
            pubkey: oracle,
            is_signer: false,
            is_writable: false,
        });
        // Real user authority & token accounts
        let authority = self
            .user_authority
            .read()
            .unwrap()
            .ok_or_else(|| anyhow!("orca user authority not set"))?;
        let (input_mint, output_mint) = if forward {
            (in_pk, out_pk)
        } else {
            (out_pk, in_pk)
        };
        let user_source = *self
            .user_token_accounts
            .get(&input_mint)
            .ok_or_else(|| anyhow!("missing user source token account for input mint"))?;
        let user_destination = *self
            .user_token_accounts
            .get(&output_mint)
            .ok_or_else(|| anyhow!("missing user destination token account for output mint"))?;
        accounts.push(AM {
            pubkey: authority,
            is_signer: true,
            is_writable: false,
        });
        accounts.push(AM {
            pubkey: user_source,
            is_signer: false,
            is_writable: true,
        });
        accounts.push(AM {
            pubkey: user_destination,
            is_signer: false,
            is_writable: true,
        });
        Ok(vec![Instruction {
            program_id,
            accounts,
            data,
        }])
    }

    fn list_pairs(&self) -> Vec<(String, String)> {
        self.pools
            .iter()
            .map(|p| (p.base_mint.to_string(), p.quote_mint.to_string()))
            .collect()
    }
}

impl Orca {
    fn parse_token_amount(data: &[u8]) -> u64 {
        if data.len() < 72 {
            return 0;
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&data[64..72]);
        u64::from_le_bytes(arr)
    }
}

// (Removed) WhirlpoolMeta replaced by canonical parser struct in layout module.

fn derive_tick_array_pda(pool: &Pubkey, start_tick: i32) -> Pubkey {
    let seeds: &[&[u8]] = &[b"tick_array", pool.as_ref(), &start_tick.to_le_bytes()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn derive_oracle_pda(pool: &Pubkey) -> Pubkey {
    let seeds: &[&[u8]] = &[b"oracle", pool.as_ref()];
    Pubkey::find_program_address(seeds, &Pubkey::from_str(ORCA_WHIRLPOOL_PROGRAM).unwrap()).0
}

fn align_to_start(tick: i32, spacing: i32) -> i32 {
    tick - (tick.rem_euclid(spacing))
}
