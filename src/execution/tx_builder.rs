use crate::execution::live_pool_cache::{CachedPoolState, SharedLivePoolCache};
use crate::ipc::{RejectReason, SwapHop, TradeIntent, TradeSide};
use crate::solana::dex::meteora_dlmm::MeteoraDlmm;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::orca_whirlpool_layout;
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::dex::pumpfun_amm::PumpFunAmmDex;
use crate::solana::dex::raydium::Raydium;
use crate::solana::dex::Dex;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::hash::hash;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::str::FromStr;
use std::sync::Arc;
use tracing::warn;

/// System program ID
const SYSTEM_PROGRAM_ID: &str = "11111111111111111111111111111111";

fn prog_ix_to_sdk(ix: spl_token::solana_program::instruction::Instruction) -> Instruction {
    Instruction {
        program_id: Pubkey::new_from_array(ix.program_id.to_bytes()),
        accounts: ix
            .accounts
            .into_iter()
            .map(|a| AccountMeta {
                pubkey: Pubkey::new_from_array(a.pubkey.to_bytes()),
                is_signer: a.is_signer,
                is_writable: a.is_writable,
            })
            .collect(),
        data: ix.data,
    }
}

/// Build wrap SOL instructions: CreateATA (idempotent) + Transfer + SyncNative
///
/// Returns instructions to:
/// 1. Create WSOL ATA if needed (idempotent)
/// 2. Transfer native SOL to the WSOL ATA
/// 3. Sync native balance to token balance
///
/// NOTE: This function is currently unused because WsolManager maintains
/// WSOL buffer outside of trades. Keeping it for potential future use
/// (e.g., manual wrap scenarios or fallback mode).
#[allow(dead_code)]
fn build_wrap_sol_instructions(wallet_pubkey: Pubkey, amount_lamports: u64) -> Vec<Instruction> {
    let wsol_mint = Pubkey::from_str(SOL_MINT).expect("valid SOL_MINT");
    let wsol_mint_spl = SplProgramPubkey::new_from_array(wsol_mint.to_bytes());
    let wallet_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
    let token_program_spl = spl_token::id();

    // Derive WSOL ATA
    let wsol_ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
        &wallet_spl,
        &wsol_mint_spl,
        &token_program_spl,
    );
    let wsol_ata = Pubkey::new_from_array(wsol_ata_spl.to_bytes());

    let mut ixs = Vec::with_capacity(3);

    // 1. Create WSOL ATA (idempotent - won't fail if exists)
    let create_ata_ix = prog_ix_to_sdk(
        spl_associated_token_account::instruction::create_associated_token_account_idempotent(
            &wallet_spl,
            &wallet_spl,
            &wsol_mint_spl,
            &token_program_spl,
        ),
    );
    ixs.push(create_ata_ix);

    // 2. Transfer native SOL to WSOL ATA
    // System program transfer: 4-byte discriminator (2u32 LE) + 8-byte lamports (u64 LE)
    let system_program_id = Pubkey::from_str(SYSTEM_PROGRAM_ID).expect("valid system program id");
    let transfer_ix = Instruction {
        program_id: system_program_id,
        accounts: vec![
            AccountMeta {
                pubkey: wallet_pubkey,
                is_signer: true,
                is_writable: true,
            },
            AccountMeta {
                pubkey: wsol_ata,
                is_signer: false,
                is_writable: true,
            },
        ],
        data: {
            let mut d = Vec::with_capacity(12);
            d.extend_from_slice(&2u32.to_le_bytes()); // Transfer discriminator (4 bytes)
            d.extend_from_slice(&amount_lamports.to_le_bytes());
            d
        },
    };
    ixs.push(transfer_ix);

    // 3. Sync native balance to WSOL token balance
    let sync_ix = prog_ix_to_sdk(
        spl_token::instruction::sync_native(&token_program_spl, &wsol_ata_spl)
            .expect("valid sync_native instruction"),
    );
    ixs.push(sync_ix);

    ixs
}

#[derive(Debug, Clone)]
pub struct TxPlan {
    pub instructions: Vec<Instruction>,
}

impl TxPlan {
    pub fn hash_string(&self) -> String {
        let mut bytes = Vec::new();
        for ix in &self.instructions {
            bytes.extend_from_slice(ix.program_id.as_ref());
            bytes.extend_from_slice(&(ix.accounts.len() as u32).to_le_bytes());
            for meta in &ix.accounts {
                bytes.extend_from_slice(meta.pubkey.as_ref());
                bytes.push(meta.is_signer as u8);
                bytes.push(meta.is_writable as u8);
            }
            bytes.extend_from_slice(&(ix.data.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&ix.data);
        }
        hash(&bytes).to_string()
    }
}

#[derive(Debug, Clone)]
pub struct UnsupportedTxPlan {
    pub reason: RejectReason,
    pub details: String,
}

#[derive(Debug, Clone)]
pub enum TxPlanOutcome {
    Planned(TxPlan),
    Unsupported(UnsupportedTxPlan),
}

const SOL_MINT: &str = "So11111111111111111111111111111111111111112";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DexHint {
    Raydium,
    Orca,
    Pumpfun,
    PumpAmm,
    MeteoraDlmm,
}

fn dex_hint_from_intent(intent: &TradeIntent) -> Result<DexHint, UnsupportedTxPlan> {
    match intent.metadata.get("dex").map(|s| s.as_str()) {
        Some("raydium") => Ok(DexHint::Raydium),
        Some("orca") => Ok(DexHint::Orca),
        Some("pumpfun") => Ok(DexHint::Pumpfun),
        Some("pump_amm") => Ok(DexHint::PumpAmm),
        Some("meteora_dlmm") => Ok(DexHint::MeteoraDlmm),
        Some(other) => Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("unsupported metadata.dex={other}"),
        }),
        None => {
            // Back-compat behavior: if producer didn't provide a dex hint but *did* provide a
            // Pump.fun creator, treat it as pumpfun. Otherwise we must not guess (misroutes are
            // worse than rejects).
            let has_creator = intent
                .metadata
                .get("creator")
                .is_some_and(|v| !v.trim().is_empty());
            if has_creator {
                Ok(DexHint::Pumpfun)
            } else {
                Err(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "missing metadata.dex (required for tx build routing)".to_string(),
                })
            }
        }
    }
}

/// Try to get min_out from intent. Returns None if not provided.
fn min_out_raw_from_intent(intent: &TradeIntent) -> Option<u64> {
    // First check typed execution field
    if let Some(execution) = intent.execution.as_ref() {
        if let Some(min_out) = execution.min_out.as_ref() {
            if min_out.raw > 0 {
                return Some(min_out.raw);
            }
        }
    }

    // Legacy fallback (stringly-typed metadata)
    if let Some(min_out_str) = intent.metadata.get("min_out_raw") {
        if let Ok(v) = min_out_str.parse::<u64>() {
            if v > 0 {
                return Some(v);
            }
        }
    }

    None
}

/// Fetch Orca Whirlpool state from RPC (fallback when cache miss)
async fn fetch_orca_from_rpc(
    rpc: &Arc<SolanaRpc>,
    pool_id: &Pubkey,
) -> Result<orca_whirlpool_layout::WhirlpoolParsed, UnsupportedTxPlan> {
    let acct = match rpc.rpc.get_account(pool_id).await {
        Ok(a) => a,
        Err(e) => {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("orca whirlpool account fetch failed: {e}"),
            })
        }
    };

    match orca_whirlpool_layout::parse_whirlpool(&acct.data) {
        Some(p) => Ok(p),
        None => Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: "orca whirlpool parse failed (invalid layout/size)".to_string(),
        }),
    }
}

pub async fn build_tx_plan(
    intent: &TradeIntent,
    wallet_pubkey: Pubkey,
    rpc: Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> TxPlanOutcome {
    // === Multi-hop detection (takes priority over single-hop) ===
    // Multi-hop intents have swap_path with multiple hops for atomic arbitrage
    if let Some(ref swap_path) = intent.swap_path {
        if !swap_path.is_empty() {
            tracing::info!(
                intent_id = %intent.intent_id,
                hops = swap_path.len(),
                "Building multi-hop tx plan"
            );
            return build_multi_hop_tx_plan(intent, wallet_pubkey, rpc, cache, swap_path).await;
        }
    }

    // === Single-hop path (original logic) ===
    let dex_hint = match dex_hint_from_intent(intent) {
        Ok(h) => h,
        Err(e) => return TxPlanOutcome::Unsupported(e),
    };

    match intent.side {
        TradeSide::Buy => {
            if intent.resources.input_mint != SOL_MINT {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!(
                        "input_mint={} (only SOL mint supported for Buy)",
                        intent.resources.input_mint
                    ),
                });
            }
        }
        TradeSide::Sell => {
            if intent.resources.output_mint != SOL_MINT {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!(
                        "output_mint={} (only SOL mint supported for Sell)",
                        intent.resources.output_mint
                    ),
                });
            }

            if intent.resources.input_mint == SOL_MINT {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "input_mint=SOL (Sell expects token->SOL)".to_string(),
                });
            }
        }
    }

    if intent.resources.pools.len() != 1 {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!(
                "pools_len={} (expected exactly 1)",
                intent.resources.pools.len()
            ),
        });
    }

    // Get min_out from intent, or calculate fresh from cache if not provided
    // This is the core of Option C: execution-engine calculates min_out from live cache
    let min_out: u64 = match min_out_raw_from_intent(intent) {
        Some(v) => {
            tracing::debug!(min_out = v, "tx_plan: using min_out from intent");
            v
        }
        None => {
            // No min_out in intent - try to calculate from cache
            if let Some(cache) = cache {
                match super::quote_calculator::calculate_fresh_min_out(cache, intent) {
                    Ok(Some(fresh_min_out)) => {
                        tracing::info!(
                            fresh_min_out,
                            amount_in = intent.required_capital.raw,
                            slippage_bps = intent.max_slippage_bps,
                            "tx_plan: calculated fresh min_out from cache"
                        );
                        fresh_min_out
                    }
                    Ok(None) => {
                        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                            reason: RejectReason::UnsupportedIntent,
                            details: "no min_out in intent and cache calculation returned None (pool not cached or zero output)".to_string(),
                        });
                    }
                    Err(e) => {
                        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                            reason: RejectReason::UnsupportedIntent,
                            details: format!(
                                "no min_out in intent and cache calculation failed: {e}"
                            ),
                        });
                    }
                }
            } else {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details:
                        "missing execution.min_out and no cache available for fresh calculation"
                            .to_string(),
                });
            }
        }
    };

    if dex_hint == DexHint::Orca {
        // Orca planning requires a whirlpool pubkey in `resources.pools[0]`.
        let pool_id_str = &intent.resources.pools[0];
        let pool_id = match Pubkey::from_str(pool_id_str) {
            Ok(pk) => pk,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid resources.pools[0] pubkey for orca: {e}"),
                })
            }
        };

        // Try cache first, fallback to RPC
        let parsed = if let Some(cache) = cache {
            if let Some((state, slot, age_ms)) = cache.get_with_metadata(&pool_id) {
                match state {
                    CachedPoolState::Orca(orca_state) => {
                        // Convert cached state to WhirlpoolParsed
                        tracing::debug!(
                            pool = %pool_id,
                            slot,
                            age_ms,
                            "orca: using cached pool state"
                        );
                        orca_whirlpool_layout::WhirlpoolParsed {
                            token_mint_a: orca_state.token_mint_a,
                            token_mint_b: orca_state.token_mint_b,
                            token_vault_a: orca_state.token_vault_a,
                            token_vault_b: orca_state.token_vault_b,
                            tick_current_index: orca_state.tick_current_index,
                            sqrt_price: orca_state.sqrt_price,
                            liquidity: orca_state.liquidity,
                            fee_rate: orca_state.fee_rate,
                            protocol_fee_rate: orca_state.protocol_fee_rate,
                            tick_spacing: orca_state.tick_spacing,
                        }
                    }
                    _ => {
                        warn!(pool = %pool_id, "orca: cache hit but wrong DEX type, falling back to RPC");
                        match fetch_orca_from_rpc(&rpc, &pool_id).await {
                            Ok(p) => p,
                            Err(e) => return TxPlanOutcome::Unsupported(e),
                        }
                    }
                }
            } else {
                warn!(pool = %pool_id, "orca: cache miss, falling back to RPC");
                match fetch_orca_from_rpc(&rpc, &pool_id).await {
                    Ok(p) => p,
                    Err(e) => return TxPlanOutcome::Unsupported(e),
                }
            }
        } else {
            // No cache provided, use RPC directly
            match fetch_orca_from_rpc(&rpc, &pool_id).await {
                Ok(p) => p,
                Err(e) => return TxPlanOutcome::Unsupported(e),
            }
        };

        let orca = Orca::new(Arc::clone(&rpc));
        orca.set_user_authority(wallet_pubkey);

        // Register ATAs for both mints (Orca build_swap_ix requires these mappings).
        let owner_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
        let token_program_spl = spl_token::id();
        for mint in [
            intent.resources.input_mint.as_str(),
            intent.resources.output_mint.as_str(),
        ] {
            let mint_sdk = match Pubkey::from_str(mint) {
                Ok(m) => m,
                Err(e) => {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::InvalidIntent,
                        details: format!("invalid mint pubkey: {e}"),
                    })
                }
            };
            let mint_spl = SplProgramPubkey::new_from_array(mint_sdk.to_bytes());
            let ata_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner_spl,
                    &mint_spl,
                    &token_program_spl,
                );
            let ata_sdk = Pubkey::new_from_array(ata_spl.to_bytes());
            orca.set_user_token_account(mint_sdk, ata_sdk);
        }

        orca.insert_whirlpool_parsed(pool_id, parsed);

        let ixs = match orca.build_swap_ix(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
        ) {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("orca build failed: {e}"),
                })
            }
        };

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.

        return TxPlanOutcome::Planned(TxPlan {
            instructions: ixs,
        });
    }

    if dex_hint == DexHint::Raydium {
        // Raydium planning requires an AMM pool pubkey in `resources.pools[0]`.
        let pool_id_str = &intent.resources.pools[0];
        let pool_id = match Pubkey::from_str(pool_id_str) {
            Ok(pk) => pk,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid resources.pools[0] pubkey for raydium: {e}"),
                })
            }
        };

        let mut raydium = Raydium::new(Arc::clone(&rpc));
        raydium.set_user_authority(wallet_pubkey);

        // Try cache first for pool state, fallback to RPC
        let mut used_cache = false;
        if let Some(cache) = cache {
            if let Some((state, slot, age_ms)) = cache.get_with_metadata(&pool_id) {
                match state {
                    CachedPoolState::RaydiumAmm(amm_state) => {
                        tracing::debug!(
                            pool = %pool_id,
                            slot,
                            age_ms,
                            "raydium: using cached pool state"
                        );
                        // Inject cached state into Raydium adapter
                        raydium.inject_cached_amm_state(
                            pool_id,
                            amm_state.base_mint,
                            amm_state.quote_mint,
                            amm_state.coin_vault,
                            amm_state.pc_vault,
                            amm_state.base_decimals,
                            amm_state.quote_decimals,
                            amm_state.market_id,
                            amm_state.serum_bids,
                            amm_state.serum_asks,
                            amm_state.serum_event_queue,
                        );
                        used_cache = true;
                    }
                    _ => {
                        warn!(pool = %pool_id, "raydium: cache hit but wrong DEX type, falling back to RPC");
                    }
                }
            } else {
                warn!(pool = %pool_id, "raydium: cache miss, falling back to RPC");
            }
        }

        // If cache didn't provide state, load from RPC
        if !used_cache {
            if let Err(e) = raydium.load_pool_from_geyser(&pool_id).await {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("raydium load_pool_from_geyser failed: {e}"),
                });
            }
        }

        // Raydium tx building needs Serum/OpenBook market accounts.
        // Note: These are static and could be cached, but for now we still fetch if not in cache
        if let Err(e) = raydium.fetch_and_populate_serum_accounts(&pool_id).await {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("raydium fetch_serum_accounts failed: {e}"),
            });
        }

        let ixs = match raydium.build_swap_ix(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
        ) {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("raydium build failed: {e}"),
                })
            }
        };

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.

        return TxPlanOutcome::Planned(TxPlan {
            instructions: ixs,
        });
    }

    if dex_hint == DexHint::PumpAmm {
        // Pump.fun AMM (PumpSwap) planning uses the pool/market pubkey in `resources.pools[0]`.
        // This path is intent-driven and does not perform any on-chain discovery.
        let pool_id_str = &intent.resources.pools[0];
        let pool_id = match Pubkey::from_str(pool_id_str) {
            Ok(pk) => pk,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid resources.pools[0] pubkey for pump_amm: {e}"),
                })
            }
        };

        // Accept both formats:
        // - 12 accounts: SELL format (no volume accumulators)
        // - 14 accounts: BUY format (with global_volume_accumulator + user_volume_accumulator)
        let accounts_len = intent.resources.accounts.len();
        if accounts_len != 12 && accounts_len != 14 {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "pump_amm requires resources.accounts (len=12 for SELL or len=14 for BUY), got len={}",
                    accounts_len
                ),
            });
        }

        let mut pool_accounts: Vec<Pubkey> = Vec::with_capacity(accounts_len);
        for (idx, a) in intent.resources.accounts.iter().enumerate() {
            match Pubkey::from_str(a) {
                Ok(pk) => pool_accounts.push(pk),
                Err(e) => {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::InvalidIntent,
                        details: format!(
                            "invalid resources.accounts[{idx}] pubkey for pump_amm: {e}"
                        ),
                    })
                }
            }
        }

        if pool_accounts[0] != pool_id {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::InvalidIntent,
                details: format!(
                    "pump_amm pool mismatch: resources.pools[0]={pool_id} but resources.accounts[0]={}",
                    pool_accounts[0]
                ),
            });
        }

        let mut ixs = match PumpFunAmmDex::build_swap_ix_from_pool_accounts(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
            wallet_pubkey,
            &pool_accounts,
        ) {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("pump_amm build failed: {e}"),
                })
            }
        };

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.
        // For SELL trades, ensure WSOL ATA exists (for receiving output).
        if intent.side == TradeSide::Sell {
            let wsol_mint = Pubkey::from_str(SOL_MINT).expect("valid SOL_MINT");
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let wsol_mint_spl = SplProgramPubkey::new_from_array(wsol_mint.to_bytes());
            let token_program_spl = spl_token::id();
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &wsol_mint_spl,
                    &token_program_spl,
                ),
            );
            ixs.insert(0, ata_ix);
        }

        return TxPlanOutcome::Planned(TxPlan { instructions: ixs });
    }

    // Meteora DLMM planning
    if dex_hint == DexHint::MeteoraDlmm {
        let pool_id_str = &intent.resources.pools[0];
        let _pool_id = match Pubkey::from_str(pool_id_str) {
            Ok(pk) => pk,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid resources.pools[0] pubkey for meteora_dlmm: {e}"),
                })
            }
        };

        // Meteora DLMM requires DexPoolAccounts with pool data
        // Format: [lb_pair, token_x_mint, token_y_mint, reserve_x?, reserve_y?, active_id:N?, bin_step:N?]
        if intent.resources.accounts.len() < 3 {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "meteora_dlmm requires resources.accounts (DexPoolAccounts), got len={}",
                    intent.resources.accounts.len()
                ),
            });
        }

        let mut meteora = MeteoraDlmm::new(Arc::clone(&rpc));
        meteora.set_user_authority(wallet_pubkey);

        // Parse accounts and set pool from intent data
        if let Err(e) = meteora.set_pool_from_accounts(pool_id_str, &intent.resources.accounts) {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("meteora_dlmm set_pool_from_accounts failed: {e}"),
            });
        }

        // Use async version - Meteora DLMM requires bin array derivation
        let ixs = match meteora
            .build_swap_ix_async(
                &intent.resources.input_mint,
                &intent.resources.output_mint,
                intent.required_capital.raw,
                min_out,
            )
            .await
        {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("meteora_dlmm build failed: {e}"),
                })
            }
        };

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.
        // We only create the token ATA for receiving bought tokens.
        let final_ixs = if intent.side == TradeSide::Buy {
            // Create token ATA (for receiving bought tokens)
            let token_mint = Pubkey::from_str(&intent.resources.output_mint).unwrap_or_default();
            let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let token_program_spl = spl_token::id();
            let token_ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &token_mint_spl,
                    &token_program_spl,
                ),
            );
            let mut all_ixs = vec![token_ata_ix];
            all_ixs.extend(ixs);
            all_ixs
        } else {
            // SELL: ensure WSOL ATA exists
            let wsol_mint = Pubkey::from_str(SOL_MINT).expect("valid SOL_MINT");
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let wsol_mint_spl = SplProgramPubkey::new_from_array(wsol_mint.to_bytes());
            let token_program_spl = spl_token::id();
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &wsol_mint_spl,
                    &token_program_spl,
                ),
            );
            let mut all_ixs = vec![ata_ix];
            all_ixs.extend(ixs);
            all_ixs
        };

        return TxPlanOutcome::Planned(TxPlan {
            instructions: final_ixs,
        });
    }

    debug_assert_eq!(dex_hint, DexHint::Pumpfun);

    // For Pump.fun, creator MUST be provided by the strategy (typically from a Geyser event).
    // We intentionally do not attempt to parse it from the bonding curve state.
    let creator_str = match intent.metadata.get("creator") {
        Some(v) if !v.trim().is_empty() => v,
        _ => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: "missing metadata.creator (required for pump.fun tx build)".to_string(),
            })
        }
    };

    let creator = match Pubkey::from_str(creator_str) {
        Ok(pk) => pk,
        Err(e) => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::InvalidIntent,
                details: format!("invalid metadata.creator pubkey: {e}"),
            })
        }
    };

    // Currently we implement Pump.fun BUY (SOL->token) and SELL (token->SOL) plans.
    let mut pumpfun = match PumpFunDex::new(rpc) {
        Ok(d) => d,
        Err(e) => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::InternalError,
                details: format!("pumpfun init failed: {e}"),
            })
        }
    };
    pumpfun.set_user_authority(wallet_pubkey);

    // Parse token_program from intent (for Token-2022 support)
    let token_program_override = intent
        .resources
        .token_program
        .as_ref()
        .and_then(|s| Pubkey::from_str(s).ok());

    let ixs = match pumpfun
        .build_swap_ix_async_with_slippage(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
            Some(creator),
            intent.max_slippage_bps,
            token_program_override,
        )
        .await
    {
        Ok(ixs) => ixs,
        Err(e) => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("pumpfun build failed: {e}"),
            })
        }
    };

    // NOTE: No in-TX wrap for BUYs!
    // WsolManager maintains WSOL buffer outside of trades.
    // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.
    // Pump.fun BC swap instructions handle the SOL transfer internally.

    TxPlanOutcome::Planned(TxPlan {
        instructions: ixs,
    })
}

// ============================================================================
// Multi-hop TX Plan Builder (for atomic arbitrage)
// ============================================================================

/// Build transaction plan for multi-hop arbitrage (atomic bundle).
///
/// Multi-hop intents contain a `swap_path` with multiple SwapHop entries,
/// each specifying: pool_address, dex, input_mint, output_mint.
///
/// For a 2-hop arb cycle WSOL → Token → WSOL:
/// - Hop 0: Buy token (WSOL → Token) on DEX A
/// - Hop 1: Sell token (Token → WSOL) on DEX B
///
/// All swaps are combined into a single atomic transaction (Jito bundle).
async fn build_multi_hop_tx_plan(
    intent: &TradeIntent,
    wallet_pubkey: Pubkey,
    rpc: Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
    swap_path: &[SwapHop],
) -> TxPlanOutcome {
    // Validate: must start and end with SOL for arbitrage
    if swap_path.is_empty() {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: "multi-hop: swap_path is empty".to_string(),
        });
    }

    let first_hop = &swap_path[0];
    let last_hop = &swap_path[swap_path.len() - 1];

    // Arb cycle: WSOL → ... → WSOL
    if first_hop.input_mint != SOL_MINT {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!(
                "multi-hop: first hop input_mint={} (expected WSOL for arb)",
                first_hop.input_mint
            ),
        });
    }
    if last_hop.output_mint != SOL_MINT {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!(
                "multi-hop: last hop output_mint={} (expected WSOL for arb)",
                last_hop.output_mint
            ),
        });
    }

    let mut all_instructions: Vec<Instruction> = Vec::new();

    // NOTE: No in-TX wrap for arbitrage!
    // WsolManager maintains WSOL buffer outside of trades.
    // This saves ~2000-3000 CU per trade and avoids lamport noise.
    // If WSOL balance is insufficient, the swap will fail in simulation.

    // Build swap instructions for each hop
    let mut current_amount = intent.required_capital.raw;

    for (hop_idx, hop) in swap_path.iter().enumerate() {
        tracing::debug!(
            intent_id = %intent.intent_id,
            hop_idx,
            dex = %hop.dex,
            pool = %hop.pool_address,
            input_mint = %hop.input_mint,
            output_mint = %hop.output_mint,
            amount_in = current_amount,
            "Building hop instruction"
        );

        let hop_ixs = match build_hop_instructions(
            wallet_pubkey,
            hop,
            current_amount,
            &rpc,
            cache,
        )
        .await
        {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: e.reason,
                    details: format!("multi-hop: hop {} failed: {}", hop_idx, e.details),
                });
            }
        };

        all_instructions.extend(hop_ixs);

        // For subsequent hops, use expected_output as input amount.
        // Note: This is an estimate; simulation will validate the actual flow.
        // For atomic arb, we use min_out=1 and let simulation validate profitability.
        if hop.expected_output > 0 {
            current_amount = hop.expected_output;
        }
        // If expected_output is 0, keep current_amount (simulation will handle)
    }

    tracing::info!(
        intent_id = %intent.intent_id,
        total_instructions = all_instructions.len(),
        hops = swap_path.len(),
        "Multi-hop tx plan built successfully"
    );

    TxPlanOutcome::Planned(TxPlan {
        instructions: all_instructions,
    })
}

/// Build swap instructions for a single hop.
///
/// Routes to the appropriate DEX based on hop.dex field.
async fn build_hop_instructions(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    amount_in: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let dex = hop.dex.to_lowercase();
    let pool_address = match Pubkey::from_str(&hop.pool_address) {
        Ok(p) => p,
        Err(e) => {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::InvalidIntent,
                details: format!("invalid pool_address {}: {}", hop.pool_address, e),
            });
        }
    };

    // For atomic arb, we use min_out=1 (simulation validates profitability)
    let min_out: u64 = 1;

    match dex.as_str() {
        "pumpfun" => {
            build_hop_pumpfun(wallet_pubkey, hop, amount_in, min_out, rpc, cache).await
        }
        "pump_amm" => {
            build_hop_pump_amm(wallet_pubkey, hop, &pool_address, amount_in, min_out, rpc, cache)
                .await
        }
        "raydium" | "raydium_amm" | "raydium_amm_v4" => {
            build_hop_raydium(wallet_pubkey, hop, &pool_address, amount_in, min_out, rpc, cache)
                .await
        }
        "raydium_cpmm" => {
            // TODO: Implement Raydium CPMM support for multi-hop
            Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("raydium_cpmm multi-hop not yet implemented (pool={})", hop.pool_address),
            })
        }
        "orca" | "orca_whirlpool" => {
            build_hop_orca(wallet_pubkey, hop, &pool_address, amount_in, min_out, rpc, cache).await
        }
        "meteora_dlmm" | "meteora" => {
            build_hop_meteora_dlmm(wallet_pubkey, hop, &pool_address, amount_in, min_out, rpc, cache)
                .await
        }
        other => Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("multi-hop: unsupported DEX '{}' in hop", other),
        }),
    }
}

// --- DEX-specific hop builders ---

async fn build_hop_pumpfun(
    _wallet_pubkey: Pubkey,
    hop: &SwapHop,
    _amount_in: u64,
    _min_out: u64,
    _rpc: &Arc<SolanaRpc>,
    _cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    // PumpFun Bonding Curve does NOT support arbitrage!
    // The bonding curve price changes with each trade, making arb impossible.
    // Only PumpSwap AMM (post-graduation pools) can be used for arbitrage.
    //
    // If you see this error, the arb-strategy should use "pump_amm" DEX type
    // for graduated tokens, not "pumpfun".
    Err(UnsupportedTxPlan {
        reason: RejectReason::UnsupportedIntent,
        details: format!(
            "pumpfun bonding curve does not support multi-hop arbitrage (pool={}). Use pump_amm for graduated tokens.",
            hop.pool_address
        ),
    })
}

async fn build_hop_pump_amm(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    _rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    // PumpSwap AMM (graduated tokens) - get pool_accounts from cache
    let pool_accounts = if let Some(cache) = cache {
        if let Some(state) = cache.get(pool_address) {
            if let CachedPoolState::PumpAmm(amm_state) = state {
                if amm_state.pool_accounts.len() >= 14 {
                    tracing::debug!(
                        pool = %pool_address,
                        accounts_len = amm_state.pool_accounts.len(),
                        "multi-hop pump_amm: using cached pool_accounts"
                    );
                    amm_state.pool_accounts.clone()
                } else {
                    return Err(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: format!(
                            "pump_amm: cached pool_accounts too short (got {}, need 14)",
                            amm_state.pool_accounts.len()
                        ),
                    });
                }
            } else {
                return Err(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("pump_amm: cache hit but wrong DEX type for {}", pool_address),
                });
            }
        } else {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("pump_amm: pool {} not in cache", pool_address),
            });
        }
    } else {
        return Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: "pump_amm: no cache available for multi-hop".to_string(),
        });
    };

    // Use static method with pool_accounts from cache
    let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts(
        &hop.input_mint,
        &hop.output_mint,
        amount_in,
        min_out,
        wallet_pubkey,
        &pool_accounts,
    )
    .map_err(|e| UnsupportedTxPlan {
        reason: RejectReason::UnsupportedIntent,
        details: format!("pump_amm build_swap_ix_from_pool_accounts failed: {}", e),
    })?;

    Ok(ixs)
}

async fn build_hop_raydium(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let mut raydium = Raydium::new(Arc::clone(rpc));

    // Try to inject pool state from cache
    let mut used_cache = false;
    if let Some(cache) = cache {
        if let Some(CachedPoolState::RaydiumAmm(amm_state)) = cache.get(pool_address) {
            tracing::debug!(
                pool = %pool_address,
                "multi-hop raydium: using cached pool state"
            );
            raydium.inject_cached_amm_state(
                *pool_address,
                amm_state.base_mint,
                amm_state.quote_mint,
                amm_state.coin_vault,
                amm_state.pc_vault,
                amm_state.base_decimals,
                amm_state.quote_decimals,
                amm_state.market_id,
                amm_state.serum_bids,
                amm_state.serum_asks,
                amm_state.serum_event_queue,
            );
            used_cache = true;
        }
    }

    // If cache didn't provide state, load from RPC
    if !used_cache {
        if let Err(e) = raydium.load_pool_from_geyser(pool_address).await {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("raydium load_pool failed: {}", e),
            });
        }
    }

    raydium.set_user_authority(wallet_pubkey);

    // Raydium needs Serum accounts if not already populated
    if let Err(e) = raydium.fetch_and_populate_serum_accounts(pool_address).await {
        tracing::warn!(
            pool = %pool_address,
            error = %e,
            "raydium: serum account fetch failed (non-fatal, may already be populated)"
        );
    }

    let ixs = raydium
        .build_swap_ix(&hop.input_mint, &hop.output_mint, amount_in, min_out)
        .map_err(|e| UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("raydium build_swap_ix failed: {}", e),
        })?;

    Ok(ixs)
}

async fn build_hop_orca(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let orca = Orca::new(Arc::clone(rpc));
    orca.set_user_authority(wallet_pubkey);

    // Get pool state from cache or RPC
    let parsed = if let Some(cache) = cache {
        if let Some(state) = cache.get(pool_address) {
            match state {
                CachedPoolState::Orca(orca_state) => {
                    tracing::debug!(
                        pool = %pool_address,
                        "multi-hop orca: using cached pool state"
                    );
                    orca_whirlpool_layout::WhirlpoolParsed {
                        token_mint_a: orca_state.token_mint_a,
                        token_mint_b: orca_state.token_mint_b,
                        token_vault_a: orca_state.token_vault_a,
                        token_vault_b: orca_state.token_vault_b,
                        tick_current_index: orca_state.tick_current_index,
                        sqrt_price: orca_state.sqrt_price,
                        liquidity: orca_state.liquidity,
                        fee_rate: orca_state.fee_rate,
                        protocol_fee_rate: orca_state.protocol_fee_rate,
                        tick_spacing: orca_state.tick_spacing,
                    }
                }
                _ => {
                    // Cache hit but wrong type - fallback to RPC
                    fetch_orca_from_rpc(rpc, pool_address).await?
                }
            }
        } else {
            // Cache miss - fallback to RPC
            fetch_orca_from_rpc(rpc, pool_address).await?
        }
    } else {
        fetch_orca_from_rpc(rpc, pool_address).await?
    };

    // Register ATAs for both mints
    let owner_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
    let token_program_spl = spl_token::id();

    for mint_str in [&hop.input_mint, &hop.output_mint] {
        let mint_sdk = Pubkey::from_str(mint_str).map_err(|e| UnsupportedTxPlan {
            reason: RejectReason::InvalidIntent,
            details: format!("invalid mint {}: {}", mint_str, e),
        })?;
        let mint_spl = SplProgramPubkey::new_from_array(mint_sdk.to_bytes());
        let ata_spl = spl_associated_token_account::get_associated_token_address_with_program_id(
            &owner_spl,
            &mint_spl,
            &token_program_spl,
        );
        let ata_sdk = Pubkey::new_from_array(ata_spl.to_bytes());
        orca.set_user_token_account(mint_sdk, ata_sdk);
    }

    orca.insert_whirlpool_parsed(*pool_address, parsed);

    let ixs = orca
        .build_swap_ix(&hop.input_mint, &hop.output_mint, amount_in, min_out)
        .map_err(|e| UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("orca build_swap_ix failed: {}", e),
        })?;

    Ok(ixs)
}

async fn build_hop_meteora_dlmm(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let mut meteora = MeteoraDlmm::new(Arc::clone(rpc));
    meteora.set_user_authority(wallet_pubkey);

    // Try to inject state from cache
    let mut used_cache = false;
    if let Some(cache) = cache {
        if let Some(CachedPoolState::Meteora(dlmm_state)) = cache.get(pool_address) {
            // Only use if active_id != 0 (real Geyser data, not default)
            if dlmm_state.active_id != 0 {
                if let Ok(true) = meteora.inject_cached_meteora_state(pool_address, &dlmm_state) {
                    tracing::debug!(
                        pool = %pool_address,
                        active_id = dlmm_state.active_id,
                        "multi-hop meteora: using cached pool state"
                    );
                    used_cache = true;
                }
            } else {
                tracing::warn!(
                    pool = %pool_address,
                    "multi-hop meteora: active_id=0 (stale cache), skipping injection"
                );
            }
        }
    }

    // If cache didn't help, load from RPC
    if !used_cache {
        // Try to load pool state via single RPC getAccount call
        if let Err(e) = meteora.load_pool_by_address(pool_address).await {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("meteora_dlmm load_pool_by_address failed: {}", e),
            });
        }
    }

    // Meteora DLMM needs async bin array derivation
    let ixs = meteora
        .build_swap_ix_async(&hop.input_mint, &hop.output_mint, amount_in, min_out)
        .await
        .map_err(|e| UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("meteora_dlmm build_swap_ix failed: {}", e),
        })?;

    Ok(ixs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{
        ExplicitAmount, IntentOrigin, IntentTier, TradeExecutionConstraints, TradeResources,
        TradingRegime,
    };

    fn base_intent() -> TradeIntent {
        TradeIntent::new(
            "test",
            "build",
            "run",
            "intent-1".to_string(),
            "test",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(1, 9),
            TradeResources {
                input_mint: "in".to_string(),
                output_mint: "out".to_string(),
                pools: vec!["pool".to_string()],
                accounts: vec![],
                token_program: None,
            },
            0,
            0,
            TradeSide::Sell,
            TradingRegime::Early,
        )
    }

    #[test]
    fn dex_hint_routing_is_explicit_and_safe() {
        // Regression: previously any non-Orca intent fell through to Pump.fun and
        // would fail with "missing metadata.creator" even when dex=raydium.

        let mut ray = base_intent();
        ray.metadata
            .insert("dex".to_string(), "raydium".to_string());
        assert_eq!(
            super::dex_hint_from_intent(&ray).unwrap(),
            super::DexHint::Raydium
        );

        let mut orca = base_intent();
        orca.metadata.insert("dex".to_string(), "orca".to_string());
        assert_eq!(
            super::dex_hint_from_intent(&orca).unwrap(),
            super::DexHint::Orca
        );

        let mut pump = base_intent();
        pump.metadata
            .insert("dex".to_string(), "pumpfun".to_string());
        assert_eq!(
            super::dex_hint_from_intent(&pump).unwrap(),
            super::DexHint::Pumpfun
        );

        let mut legacy = base_intent();
        legacy.metadata.insert(
            "creator".to_string(),
            "11111111111111111111111111111111".to_string(),
        );
        assert_eq!(
            super::dex_hint_from_intent(&legacy).unwrap(),
            super::DexHint::Pumpfun
        );

        let missing = base_intent();
        let err = super::dex_hint_from_intent(&missing).unwrap_err();
        assert_eq!(err.reason, RejectReason::UnsupportedIntent);
        assert!(err.details.contains("missing metadata.dex"));
    }

    #[test]
    fn min_out_prefers_typed_execution_field() {
        let mut intent = base_intent();

        intent
            .metadata
            .insert("min_out_raw".to_string(), "123".to_string());
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(999, 9)),
        });

        let parsed = super::min_out_raw_from_intent(&intent);

        assert_eq!(parsed, Some(999));
    }

    #[test]
    fn min_out_falls_back_to_legacy_metadata() {
        let mut intent = base_intent();

        intent
            .metadata
            .insert("min_out_raw".to_string(), "456".to_string());

        let parsed = super::min_out_raw_from_intent(&intent);

        assert_eq!(parsed, Some(456));
    }

    #[test]
    fn min_out_returns_none_when_missing() {
        let intent = base_intent();
        let parsed = super::min_out_raw_from_intent(&intent);
        assert_eq!(parsed, None);
    }
}
