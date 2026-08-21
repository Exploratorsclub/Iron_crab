use crate::execution::live_pool_cache::{CachedPoolState, MeteoraState, SharedLivePoolCache};
use crate::ipc::{RejectReason, SwapHop, TradeIntent, TradeSide, NATIVE_SOL_MINT};
use crate::solana::dex::meteora_dlmm::MeteoraDlmm;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::orca_whirlpool_layout;
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::dex::pumpfun_amm::{
    pump_amm_resolve_sell_pre_fee_meta_1_for_build, pump_amm_sell_ix_uses_global_fee_at,
    pump_amm_sell_trailing_is_post_upgrade_pool_v2, PumpAmmPoolAccountsDiagnostic, PumpFunAmmDex,
    PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR, PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR,
    PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS, PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS,
    PUMPFUN_AMM_SELL_EXT_TAIL_0_IX, PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2,
    PUMPFUN_AMM_SELL_EXT_TAIL_1_IX, PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2,
    PUMPFUN_AMM_SELL_EXT_THIRD_META_IX, PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2,
    PUMPFUN_AMM_SELL_FEE_TAIL_0_IX, PUMPFUN_AMM_SELL_FEE_TAIL_1_IX,
};
use crate::solana::dex::raydium::Raydium;
use crate::solana::dex::Dex;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::hash::hash;
use solana_sdk::instruction::AccountMeta;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use spl_token_2022;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

/// Whether a [`MeteoraState`] row from LivePoolCache is structurally usable for DLMM planning.
///
/// `active_id` and `bin_step` may legitimately be zero on-chain; rejecting cache injection on
/// `active_id != 0` breaks JetStream/SLAVE rows that preserve real zeros (see pool_cache_sync).
/// Default vault pubkeys indicate a degenerate/minimal cache row, not “stale active_id”.
#[must_use]
fn meteora_dlmm_cache_state_injectable(state: &MeteoraState) -> bool {
    state.reserve_x != Pubkey::default() && state.reserve_y != Pubkey::default()
}

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

/// Optional hint for SELL: (available_balance_raw, required_amount_raw).
/// When provided and available != required, CloseAccount is NOT added (partial sell safety).
///
/// Returns true when `sell_balance_hint` confirms a full sell of the highest known balance.
///
/// Hint tuple is `(known_balance_raw, required_raw)` from execution-engine preflight.
pub fn sell_close_account_verified(sell_balance_hint: Option<(u64, u64)>) -> bool {
    sell_balance_hint
        .map(|(known_balance, required)| known_balance > 0 && required == known_balance)
        .unwrap_or(false)
}

/// `allow_rpc_fallback`: When true (Cold Path, e.g. Liquidation ExecutionMevB), Raydium may use
/// RPC on cache miss. When false (Hot Path), reject on cache miss (GEYSER-ONLY).
pub async fn build_tx_plan(
    intent: &TradeIntent,
    wallet_pubkey: Pubkey,
    rpc: Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
    sell_balance_hint: Option<(u64, u64)>,
    allow_rpc_fallback: bool,
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
            return build_multi_hop_tx_plan(
                intent,
                wallet_pubkey,
                rpc,
                cache,
                swap_path,
                allow_rpc_fallback,
            )
            .await;
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

    // FIX-28: Get min_out from intent, calculate fresh from cache, and cap with
    // the more conservative (lower) value. This prevents Error 6002 when the
    // bonding curve shifts between intent creation and TX build.
    let intent_min_out = min_out_raw_from_intent(intent);
    let cache_min_out = cache.and_then(|c| match super::quote_calculator::calculate_fresh_min_out(
        c, intent,
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::debug!(error = %e, "tx_plan: cache min_out calculation failed");
            None
        }
    });

    let min_out: u64 = match (intent_min_out, cache_min_out) {
        (Some(from_intent), Some(from_cache)) => {
            let capped = from_intent.min(from_cache);
            if capped < from_intent {
                tracing::info!(
                    intent_min_out = from_intent,
                    cache_min_out = from_cache,
                    capped,
                    delta_pct =
                        format_args!("{:.1}", (1.0 - capped as f64 / from_intent as f64) * 100.0),
                    "tx_plan: capped intent min_out with fresh cache quote"
                );
            } else {
                tracing::debug!(
                    intent_min_out = from_intent,
                    cache_min_out = from_cache,
                    "tx_plan: intent min_out already conservative (cache agrees or higher)"
                );
            }
            capped
        }
        (Some(v), None) => {
            tracing::debug!(
                min_out = v,
                "tx_plan: using min_out from intent (no cache quote)"
            );
            v
        }
        (None, Some(v)) => {
            tracing::info!(
                fresh_min_out = v,
                amount_in = intent.required_capital.raw,
                slippage_bps = intent.max_slippage_bps,
                "tx_plan: calculated fresh min_out from cache (no intent min_out)"
            );
            v
        }
        (None, None) => {
            if cache.is_some() {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "no min_out in intent and cache calculation returned None (pool not cached or zero output)".to_string(),
                });
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
                        if !allow_rpc_fallback {
                            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                                reason: RejectReason::UnsupportedIntent,
                                details: "orca pool not in LivePoolCache (GEYSER-ONLY hot path)"
                                    .to_string(),
                            });
                        }
                        warn!(pool = %pool_id, "orca: cache hit but wrong DEX type, falling back to RPC");
                        match fetch_orca_from_rpc(&rpc, &pool_id).await {
                            Ok(p) => p,
                            Err(e) => return TxPlanOutcome::Unsupported(e),
                        }
                    }
                }
            } else {
                if !allow_rpc_fallback {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: "orca pool not in LivePoolCache (GEYSER-ONLY hot path)"
                            .to_string(),
                    });
                }
                warn!(pool = %pool_id, "orca: cache miss, falling back to RPC");
                match fetch_orca_from_rpc(&rpc, &pool_id).await {
                    Ok(p) => p,
                    Err(e) => return TxPlanOutcome::Unsupported(e),
                }
            }
        } else {
            if !allow_rpc_fallback {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "orca pool not in LivePoolCache (GEYSER-ONLY hot path)".to_string(),
                });
            }
            // No cache provided, use RPC directly
            match fetch_orca_from_rpc(&rpc, &pool_id).await {
                Ok(p) => p,
                Err(e) => return TxPlanOutcome::Unsupported(e),
            }
        };

        let orca = Orca::new_with_cache_ext(
            Arc::clone(&rpc),
            None,
            cache.map(Arc::clone),
            allow_rpc_fallback,
        );
        orca.set_user_authority(wallet_pubkey);
        orca.set_skip_tick_array_rpc_validation(!allow_rpc_fallback);

        // Register ATAs for both mints (Orca build_swap_ix requires these mappings).
        // Use token_program from intent for Token-2022 support.
        let owner_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
        let token_program_spl = intent
            .resources
            .token_program
            .as_ref()
            .and_then(|tp| Pubkey::from_str(tp).ok())
            .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
            .unwrap_or_else(spl_token::id);
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
            let tp = if mint == SOL_MINT {
                spl_token::id()
            } else {
                token_program_spl
            };
            let ata_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &owner_spl, &mint_spl, &tp,
                );
            let ata_sdk = Pubkey::new_from_array(ata_spl.to_bytes());
            orca.set_user_token_account(mint_sdk, ata_sdk);
        }

        orca.insert_whirlpool_parsed(pool_id, parsed);

        let ixs = match orca
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
                    details: format!("orca build failed: {e}"),
                })
            }
        };

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        let mut final_ixs = ixs;
        if intent.side == TradeSide::Buy {
            // BUY: create token ATA for receiving bought tokens (Token-2022 aware)
            let token_mint = Pubkey::from_str(&intent.resources.output_mint).unwrap_or_default();
            let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &token_mint_spl,
                    &token_program_spl,
                ),
            );
            final_ixs.insert(0, ata_ix);
        } else {
            // SELL: ensure WSOL ATA exists
            let wsol_mint = Pubkey::from_str(SOL_MINT).expect("valid SOL_MINT");
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let wsol_mint_spl = SplProgramPubkey::new_from_array(wsol_mint.to_bytes());
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &wsol_mint_spl,
                    &spl_token::id(),
                ),
            );
            final_ixs.insert(0, ata_ix);
        }

        return TxPlanOutcome::Planned(TxPlan {
            instructions: final_ixs,
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

        let mut raydium = Raydium::new_with_live_cache(
            Arc::clone(&rpc),
            cache.map(Arc::clone),
            allow_rpc_fallback,
        );
        raydium.set_user_authority(wallet_pubkey);

        // Try cache first for pool state, fallback to RPC
        let mut used_cache = false;
        let mut has_serum_from_cache = false;
        if let Some(cache) = cache {
            if let Some((state, slot, age_ms)) = cache.get_with_metadata(&pool_id) {
                match state {
                    CachedPoolState::RaydiumAmm(amm_state) => {
                        has_serum_from_cache = amm_state.serum_bids.is_some();
                        tracing::debug!(
                            pool = %pool_id,
                            slot,
                            age_ms,
                            has_serum = has_serum_from_cache,
                            "raydium: using cached pool state"
                        );
                        raydium.inject_raydium_amm_from_live_cache(pool_id, &amm_state);
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

        // If cache didn't provide state: Cold Path may use RPC, Hot Path rejects
        if !used_cache {
            if !allow_rpc_fallback {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "raydium pool not in LivePoolCache (GEYSER-ONLY hot path)".to_string(),
                });
            }
            if let Err(e) = raydium.load_pool_from_rpc_fallback(&pool_id).await {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("raydium load_pool_from_rpc_fallback failed: {e}"),
                });
            }
        }

        // FIX-29: Serum accounts are static — skip RPC if already in cache
        if !has_serum_from_cache {
            if !allow_rpc_fallback {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "raydium serum accounts not in LivePoolCache (GEYSER-ONLY hot path)"
                        .to_string(),
                });
            }
            if let Err(e) = raydium.fetch_and_populate_serum_accounts(&pool_id).await {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("raydium fetch_serum_accounts failed: {e}"),
                });
            }
            // Write back to SLAVE LivePoolCache for subsequent trades
            if let Some(cache) = cache {
                if let Some((b, a, eq, bv, qv)) = raydium.get_serum_accounts(&pool_id) {
                    cache.set_raydium_serum_accounts(&pool_id, b, a, eq, bv, qv);
                }
            }
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
        let mut final_ixs = ixs;
        if intent.side == TradeSide::Buy {
            // BUY: create token ATA for receiving bought tokens (Token-2022 aware)
            let token_mint = Pubkey::from_str(&intent.resources.output_mint).unwrap_or_default();
            let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let token_program_spl = intent
                .resources
                .token_program
                .as_ref()
                .and_then(|tp| Pubkey::from_str(tp).ok())
                .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
                .unwrap_or_else(spl_token::id);
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &token_mint_spl,
                    &token_program_spl,
                ),
            );
            final_ixs.insert(0, ata_ix);
        } else {
            // SELL: ensure WSOL ATA exists
            let wsol_mint = Pubkey::from_str(SOL_MINT).expect("valid SOL_MINT");
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let wsol_mint_spl = SplProgramPubkey::new_from_array(wsol_mint.to_bytes());
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &wsol_mint_spl,
                    &spl_token::id(),
                ),
            );
            final_ixs.insert(0, ata_ix);
        }

        return TxPlanOutcome::Planned(TxPlan {
            instructions: final_ixs,
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
        let (
            sell_requires_cashback_remaining,
            sell_cashback_third_meta,
            sell_extended_tail_0,
            sell_extended_tail_1,
        ) = cache
            .map(|c| c.pump_amm_sell_extended_layout(&pool_id))
            .unwrap_or((false, None, None, None));
        let (sell_extended_fee_tail_0, sell_extended_fee_tail_1) = cache
            .map(|c| c.pump_amm_sell_fee_tail_layout(&pool_id))
            .unwrap_or((None, None));
        let sell_requires_pre_fee_metas = cache
            .map(|c| c.pump_amm_sell_requires_pre_fee_metas(&pool_id))
            .unwrap_or(false);
        let sell_requires_fee_tail = cache
            .map(|c| c.pump_amm_sell_requires_fee_tail(&pool_id))
            .unwrap_or(false);
        let sell_layout_ready = cache
            .map(|c| c.pump_amm_sell_layout_ready(&pool_id))
            .unwrap_or(false);
        let cached_sell_pre_fee_meta_1 =
            cache.and_then(|c| c.pump_amm_sell_pre_fee_meta_1(&pool_id));

        let mut pool_accounts_build_source = if accounts_len == 0 {
            "slave_livepoolcache"
        } else {
            "intent_resources"
        };

        let mut pool_accounts: Vec<Pubkey> = if accounts_len == 0 {
            if let Some(cache) = cache {
                if let Some(CachedPoolState::PumpAmm(amm_state)) = cache.get(&pool_id) {
                    if amm_state.pool_accounts.len() >= 12 {
                        let (sell_ext, sell_third, sell_t0, sell_t1) =
                            cache.pump_amm_sell_extended_layout(&pool_id);
                        tracing::warn!(
                            pool = %pool_id,
                            accounts_len = amm_state.pool_accounts.len(),
                            sell_cashback_remaining = sell_ext,
                            sell_cashback_third_meta = ?sell_third,
                            sell_extended_tail_0 = ?sell_t0,
                            sell_extended_tail_1 = ?sell_t1,
                            "pump_amm: using cached pool_accounts (intent missing accounts)"
                        );
                        amm_state.pool_accounts.clone()
                    } else {
                        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                            reason: RejectReason::UnsupportedIntent,
                            details: format!(
                                "pump_amm cached pool_accounts too short: len={}",
                                amm_state.pool_accounts.len()
                            ),
                        });
                    }
                } else {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: format!("pump_amm: pool {} not in cache", pool_id),
                    });
                }
            } else {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "pump_amm requires resources.accounts or cache".to_string(),
                });
            }
        } else {
            if accounts_len != 12 && accounts_len != 14 {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!(
                        "pump_amm requires resources.accounts (len=12 for SELL or len=14 for BUY), got len={}",
                        accounts_len
                    ),
                });
            }

            let mut parsed = Vec::with_capacity(accounts_len);
            for (idx, a) in intent.resources.accounts.iter().enumerate() {
                match Pubkey::from_str(a) {
                    Ok(pk) => parsed.push(pk),
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

            if parsed[0] != pool_id {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!(
                        "pump_amm pool mismatch: resources.pools[0]={pool_id} but resources.accounts[0]={}",
                        parsed[0]
                    ),
                });
            }

            parsed
        };

        // Scope 51 / Bug #34: After market-data `force_refresh`, SLAVE holds authoritative v14 +
        // extended SELL layout (`third_meta`). Cold-path intents often still carry stale
        // `resources.accounts` (same 14er); the builder must prefer **this pool’s** JetStream-
        // merged explicit `Ready` row — not mint-only heuristics (multi-pool) and not legacy
        // effective-ready fallback (mislabels observability). I-24d: no local discovery / no RPC.
        if allow_rpc_fallback && intent.side == TradeSide::Sell && accounts_len > 0 {
            if let Some(c) = cache {
                if let Some(v14) = c
                    .get_explicit_jetstream_ready_pump_amm_pool_accounts_v14_for_pool_market(
                        &pool_id,
                    )
                {
                    pool_accounts = v14;
                    pool_accounts_build_source = "slave_explicit_jetstream_ready_v14";
                }
            }
        }

        if pool_accounts.len() != 12 && pool_accounts.len() != 14 {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "pump_amm requires 12 or 14 accounts, got len={}",
                    pool_accounts.len()
                ),
            });
        }

        let global_volume_accumulator = pool_accounts
            .get(11)
            .copied()
            .filter(|p| *p != Pubkey::default());
        let (sell_pre_fee_meta_1, pre_fee_source) =
            if intent.side == TradeSide::Sell && sell_requires_pre_fee_metas {
                if let Some(gva) = global_volume_accumulator {
                    pump_amm_resolve_sell_pre_fee_meta_1_for_build(
                        &pool_id,
                        gva,
                        cached_sell_pre_fee_meta_1,
                    )
                } else {
                    (cached_sell_pre_fee_meta_1, "cache_unvalidated")
                }
            } else {
                (cached_sell_pre_fee_meta_1, "cache")
            };

        let quote_tp_for_tail = Pubkey::new_from_array(spl_token::id().to_bytes());
        let quote_mint_for_tail = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
        let (derived_tail_21_plan, derived_tail_22_plan) =
            if sell_requires_cashback_remaining && intent.side == TradeSide::Sell {
                PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(
                    wallet_pubkey,
                    quote_mint_for_tail,
                    quote_tp_for_tail,
                )
            } else {
                (Pubkey::default(), Pubkey::default())
            };
        let input_mint_pk = Pubkey::from_str(&intent.resources.input_mint).ok();
        let sell_post_upgrade_pool_v2_trailing = intent.side == TradeSide::Sell
            && sell_requires_cashback_remaining
            && input_mint_pk.is_some_and(|m| {
                sell_extended_tail_0
                    .filter(|p| *p != Pubkey::default())
                    .is_some_and(|t0| pump_amm_sell_trailing_is_post_upgrade_pool_v2(&m, t0))
            });
        let cached_tail_mismatch_plan = if sell_post_upgrade_pool_v2_trailing {
            false
        } else {
            sell_extended_tail_0
                .zip(sell_extended_tail_1)
                .is_some_and(|(c0, c1)| {
                    c0 != Pubkey::default()
                        && c1 != Pubkey::default()
                        && (c0 != derived_tail_21_plan || c1 != derived_tail_22_plan)
                })
        };
        let sell_extended_tail_0_for_build =
            if intent.side == TradeSide::Sell && cached_tail_mismatch_plan {
                None
            } else {
                sell_extended_tail_0
            };
        let sell_extended_tail_1_for_build =
            if intent.side == TradeSide::Sell && cached_tail_mismatch_plan {
                None
            } else {
                sell_extended_tail_1
            };

        // Parse token_program from intent for Token-2022 support (same as pumpfun path).
        // TradeResources documents token_program as "output_mint" — on SELL, output is WSOL (SPL Token).
        // Producers may therefore set SPL Token program here even when the *input* (base) mint uses
        // Token-2022: wrong base ATA derivation → on-chain Custom(6023) NotEnoughTokensToSell.
        // Geyser-cached mint owner (LivePoolCache) is authoritative for the base mint's program.
        let token_2022_pk = Pubkey::new_from_array(spl_token_2022::id().to_bytes());

        let mut token_program_override = intent
            .resources
            .token_program
            .as_ref()
            .and_then(|s| Pubkey::from_str(s.trim()).ok());

        if let (Ok(input_mint_pk), Some(c)) =
            (Pubkey::from_str(&intent.resources.input_mint), cache)
        {
            if let Some(cached_tp) = c.get_mint_program(&input_mint_pk) {
                if cached_tp == token_2022_pk {
                    token_program_override = Some(token_2022_pk);
                } else if token_program_override.is_none() {
                    token_program_override = Some(cached_tp);
                }
            }
        }

        let mut ixs = match PumpFunAmmDex::build_swap_ix_from_pool_accounts_with_extended_tail(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
            wallet_pubkey,
            &pool_accounts,
            token_program_override,
            sell_requires_cashback_remaining && intent.side == TradeSide::Sell,
            if intent.side == TradeSide::Sell {
                sell_cashback_third_meta
            } else {
                None
            },
            sell_requires_pre_fee_metas && intent.side == TradeSide::Sell,
            if intent.side == TradeSide::Sell {
                sell_pre_fee_meta_1
            } else {
                None
            },
            if intent.side == TradeSide::Sell {
                sell_extended_tail_0_for_build
            } else {
                None
            },
            if intent.side == TradeSide::Sell {
                sell_extended_tail_1_for_build
            } else {
                None
            },
            if intent.side == TradeSide::Sell {
                sell_extended_fee_tail_0
            } else {
                None
            },
            if intent.side == TradeSide::Sell {
                sell_extended_fee_tail_1
            } else {
                None
            },
            sell_requires_fee_tail && intent.side == TradeSide::Sell,
            cached_tail_mismatch_plan,
        ) {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("pump_amm build failed: {e}"),
                })
            }
        };

        if intent.side == TradeSide::Sell
            && sell_requires_pre_fee_metas
            && !ixs.is_empty()
            && ixs[0].accounts.len() != PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
        {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "pump_amm SELL: sell_requires_pre_fee_metas=true but built ix has {} accounts (expected {})",
                    ixs[0].accounts.len(),
                    PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS
                ),
            });
        }

        // Scope 44: exact v14 from intent/cache vs final SELL ix metas (fee fields overwritten in builder).
        if intent.side == TradeSide::Sell && pool_accounts.len() >= 14 && !ixs.is_empty() {
            if let Some((fee_config_ix, fee_program_ix)) =
                pump_amm_sell_ix_uses_global_fee_at(ixs[0].accounts.len())
            {
                let sell_ix_accounts = &ixs[0].accounts;
                let canonical_fee_cfg =
                    Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_CONFIG_STR).unwrap_or_default();
                let canonical_fee_prog =
                    Pubkey::from_str(PUMPFUN_AMM_BUILD_SWAP_FEE_PROGRAM_STR).unwrap_or_default();
                let v12_fc = pool_accounts[12];
                let v12_fp = pool_accounts[13];
                let ix_fc = sell_ix_accounts
                    .get(fee_config_ix)
                    .map(|m| m.pubkey)
                    .unwrap_or_default();
                let ix_fp = sell_ix_accounts
                    .get(fee_program_ix)
                    .map(|m| m.pubkey)
                    .unwrap_or_default();
                let fee_config_uses_global_constant = ix_fc == canonical_fee_cfg;
                let fee_program_uses_expected = ix_fp == canonical_fee_prog;
                let fee_config_differs_from_v14_row = v12_fc != ix_fc;
                let fee_program_differs_from_v14_row = v12_fp != ix_fp;
                let pfr_preserved = ixs[0].accounts[9].pubkey == pool_accounts[6];
                let pfr_ta_preserved = ixs[0].accounts[10].pubkey == pool_accounts[7];
                let sell_csv: String = ixs[0]
                    .accounts
                    .iter()
                    .map(|m| m.pubkey.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
                let quote_mint = Pubkey::from_str(NATIVE_SOL_MINT).unwrap_or_default();
                let (derived_tail_21, derived_tail_22) =
                    if sell_requires_cashback_remaining && intent.side == TradeSide::Sell {
                        PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(
                            wallet_pubkey,
                            quote_mint,
                            quote_tp,
                        )
                    } else {
                        (Pubkey::default(), Pubkey::default())
                    };
                let cached_tail_mismatch = cached_tail_mismatch_plan;
                let sell_ext_tail_src =
                    if !sell_requires_cashback_remaining || intent.side != TradeSide::Sell {
                        "n/a"
                    } else if sell_post_upgrade_pool_v2_trailing {
                        "validated_cache"
                    } else if sell_requires_pre_fee_metas {
                        match (
                            sell_extended_tail_0.filter(|p| *p != Pubkey::default()),
                            sell_extended_tail_1.filter(|p| *p != Pubkey::default()),
                        ) {
                            (Some(_), Some(_)) if cached_tail_mismatch => "derived_wallet_pool",
                            (Some(_), Some(_)) => "validated_cache",
                            _ => "derived_or_curated",
                        }
                    } else {
                        match (
                            sell_extended_tail_0.filter(|p| *p != Pubkey::default()),
                            sell_extended_tail_1.filter(|p| *p != Pubkey::default()),
                        ) {
                            (Some(_), Some(_)) if cached_tail_mismatch => "derived_for_intent_user",
                            (Some(_), Some(_)) => "validated_cache",
                            _ => "derived_for_intent_user",
                        }
                    };
                let layout_authoritative = if sell_requires_pre_fee_metas {
                    sell_pre_fee_meta_1.is_some() && sell_extended_fee_tail_0.is_some()
                } else if sell_post_upgrade_pool_v2_trailing {
                    sell_layout_ready
                        && sell_cashback_third_meta
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                        && sell_extended_tail_0
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                        && sell_extended_tail_1
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                } else {
                    sell_layout_ready
                        && sell_requires_cashback_remaining
                        && sell_cashback_third_meta
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                        && sell_extended_fee_tail_0
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                        && sell_extended_fee_tail_1
                            .filter(|p| *p != Pubkey::default())
                            .is_some()
                };
                let sell_ix_account_count = sell_ix_accounts.len();
                let (tail0_ix, tail1_ix, tail2_ix) =
                    if sell_ix_account_count == PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS {
                        (
                            PUMPFUN_AMM_SELL_EXT_TAIL_0_IX_V2,
                            PUMPFUN_AMM_SELL_EXT_TAIL_1_IX_V2,
                            PUMPFUN_AMM_SELL_EXT_THIRD_META_IX_V2,
                        )
                    } else {
                        (
                            PUMPFUN_AMM_SELL_EXT_TAIL_0_IX,
                            PUMPFUN_AMM_SELL_EXT_TAIL_1_IX,
                            PUMPFUN_AMM_SELL_EXT_THIRD_META_IX,
                        )
                    };
                let sell_ix_tail0 = sell_ix_accounts
                    .get(tail0_ix)
                    .map(|m| m.pubkey.to_string())
                    .unwrap_or_default();
                let sell_ix_tail1 = sell_ix_accounts
                    .get(tail1_ix)
                    .map(|m| m.pubkey.to_string())
                    .unwrap_or_default();
                let sell_ix_tail2 = sell_ix_accounts
                    .get(tail2_ix)
                    .map(|m| m.pubkey.to_string())
                    .unwrap_or_default();
                let sell_ix_tail2_writable = sell_ix_accounts
                    .get(tail2_ix)
                    .map(|m| m.is_writable)
                    .unwrap_or(false);
                let (fee_tail0_ix, fee_tail1_ix) =
                    if sell_ix_account_count == PUMPFUN_AMM_SELL_CASHBACK_TOTAL_ACCOUNTS {
                        (
                            PUMPFUN_AMM_SELL_FEE_TAIL_0_IX,
                            PUMPFUN_AMM_SELL_FEE_TAIL_1_IX,
                        )
                    } else {
                        (usize::MAX, usize::MAX)
                    };
                let sell_ix_meta_21_writable = sell_ix_accounts
                    .get(tail0_ix)
                    .map(|m| m.is_writable)
                    .unwrap_or(false);
                let sell_ix_meta_22_writable = sell_ix_accounts
                    .get(tail1_ix)
                    .map(|m| m.is_writable)
                    .unwrap_or(false);
                let sell_ix_meta_24_writable = sell_ix_accounts
                    .get(fee_tail0_ix)
                    .map(|m| m.is_writable)
                    .unwrap_or(false);
                let sell_ix_meta_25_writable = sell_ix_accounts
                    .get(fee_tail1_ix)
                    .map(|m| m.is_writable)
                    .unwrap_or(false);
                if sell_ix_account_count == PUMPFUN_AMM_SELL_EXTENDED_V2_TOTAL_ACCOUNTS {
                    info!(
                        intent_id = %intent.intent_id,
                        scope = "44",
                        dex = "pump_amm",
                        intent_user = %wallet_pubkey,
                        pool_accounts_source = pool_accounts_build_source,
                        sell_requires_pre_fee_metas,
                        sell_extended = sell_requires_cashback_remaining && intent.side == TradeSide::Sell,
                        sell_cashback_third_meta = ?sell_cashback_third_meta,
                        sell_extended_tail_source = sell_ext_tail_src,
                        pre_fee_source,
                        tail_source = sell_ext_tail_src,
                        layout_authoritative,
                        cached_tail_mismatch,
                        derived_tail_21 = %derived_tail_21,
                        derived_tail_22 = %derived_tail_22,
                        cached_tail_21 = ?sell_extended_tail_0,
                        cached_tail_22 = ?sell_extended_tail_1,
                        cached_tail_mismatch,
                        sell_extended_tail_2 = ?sell_cashback_third_meta,
                        sell_ix_meta_23 = %sell_ix_tail0,
                        sell_ix_meta_24 = %sell_ix_tail1,
                        sell_ix_meta_25 = %sell_ix_tail2,
                        sell_ix_meta_25_writable = sell_ix_tail2_writable,
                        sell_ix_account_count,
                        pool = %pool_id,
                        input_mint = %intent.resources.input_mint,
                        base_token_program_override = ?token_program_override,
                        v14_csv = %PumpAmmPoolAccountsDiagnostic::format_v14_csv(&pool_accounts[..14]),
                        sell_ix_accounts_csv = %sell_csv,
                        v14_fee_config_row = %v12_fc,
                        v14_fee_program_row = %v12_fp,
                        sell_ix_fee_config_meta = %ix_fc,
                        sell_ix_fee_program_meta = %ix_fp,
                        fee_config_uses_global_constant = fee_config_uses_global_constant,
                        fee_program_matches_expected_fee_program = fee_program_uses_expected,
                        protocol_fee_recipient_preserved_from_v14 = pfr_preserved,
                        protocol_fee_recipient_ta_preserved_from_v14 = pfr_ta_preserved,
                        v14_fee_config_differs_from_global_constant = (v12_fc != canonical_fee_cfg),
                        v14_fee_program_differs_from_expected = (v12_fp != canonical_fee_prog),
                        fee_config_differs_from_v14_row = fee_config_differs_from_v14_row,
                        fee_program_differs_from_v14_row = fee_program_differs_from_v14_row,
                        sell_ix_fee_config_ix = fee_config_ix,
                        sell_ix_fee_program_ix = fee_program_ix,
                        "Scope44: pump_amm SELL plan (27-account) — meta #21/#22 must be global FeeConfig + fee_program (v14[12] is informational; wrong type → Custom 3002)"
                    );
                } else {
                    info!(
                        intent_id = %intent.intent_id,
                        scope = "44",
                        dex = "pump_amm",
                        intent_user = %wallet_pubkey,
                        pool_accounts_source = pool_accounts_build_source,
                        sell_requires_pre_fee_metas,
                        sell_extended = sell_requires_cashback_remaining && intent.side == TradeSide::Sell,
                        sell_cashback_third_meta = ?sell_cashback_third_meta,
                        sell_extended_tail_source = sell_ext_tail_src,
                        pre_fee_source,
                        tail_source = sell_ext_tail_src,
                        layout_authoritative,
                        cached_tail_mismatch,
                        derived_tail_21 = %derived_tail_21,
                        derived_tail_22 = %derived_tail_22,
                        cached_tail_21 = ?sell_extended_tail_0,
                        cached_tail_22 = ?sell_extended_tail_1,
                        cached_tail_mismatch,
                        sell_extended_tail_2 = ?sell_cashback_third_meta,
                        sell_ix_meta_21 = %sell_ix_tail0,
                        sell_ix_meta_22 = %sell_ix_tail1,
                        sell_ix_meta_23 = %sell_ix_tail2,
                        sell_ix_meta_21_writable = sell_ix_meta_21_writable,
                        sell_ix_meta_22_writable = sell_ix_meta_22_writable,
                        sell_ix_meta_23_writable = sell_ix_tail2_writable,
                        sell_ix_meta_24_writable = sell_ix_meta_24_writable,
                        sell_ix_meta_25_writable = sell_ix_meta_25_writable,
                        sell_ix_account_count,
                        pool = %pool_id,
                        input_mint = %intent.resources.input_mint,
                        base_token_program_override = ?token_program_override,
                        v14_csv = %PumpAmmPoolAccountsDiagnostic::format_v14_csv(&pool_accounts[..14]),
                        sell_ix_accounts_csv = %sell_csv,
                        v14_fee_config_row = %v12_fc,
                        v14_fee_program_row = %v12_fp,
                        sell_ix_fee_config_meta = %ix_fc,
                        sell_ix_fee_program_meta = %ix_fp,
                        fee_config_uses_global_constant = fee_config_uses_global_constant,
                        fee_program_matches_expected_fee_program = fee_program_uses_expected,
                        protocol_fee_recipient_preserved_from_v14 = pfr_preserved,
                        protocol_fee_recipient_ta_preserved_from_v14 = pfr_ta_preserved,
                        v14_fee_config_differs_from_global_constant = (v12_fc != canonical_fee_cfg),
                        v14_fee_program_differs_from_expected = (v12_fp != canonical_fee_prog),
                        fee_config_differs_from_v14_row = fee_config_differs_from_v14_row,
                        fee_program_differs_from_v14_row = fee_program_differs_from_v14_row,
                        sell_ix_fee_config_ix = fee_config_ix,
                        sell_ix_fee_program_ix = fee_program_ix,
                        "Scope44: pump_amm SELL plan — meta #19/#20 must be global FeeConfig + fee_program (v14[12] is informational; wrong type → Custom 3002)"
                    );
                }
            }
        }

        // NOTE: No in-TX wrap for BUYs!
        // WsolManager maintains WSOL buffer outside of trades.
        // This saves ~2000-3000 CU and avoids lamport noise that breaks fill_in detection.
        if intent.side == TradeSide::Buy {
            // BUY: create token ATA for receiving bought tokens (Token-2022 aware)
            let token_mint = Pubkey::from_str(&intent.resources.output_mint).unwrap_or_default();
            let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());
            let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());
            let token_program_spl = intent
                .resources
                .token_program
                .as_ref()
                .and_then(|tp| Pubkey::from_str(tp).ok())
                .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
                .unwrap_or_else(spl_token::id);
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &token_mint_spl,
                    &token_program_spl,
                ),
            );
            ixs.insert(0, ata_ix);
        } else {
            // SELL: ensure WSOL ATA exists (for receiving output)
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

        let mut meteora = MeteoraDlmm::new_with_live_cache(
            Arc::clone(&rpc),
            cache.map(Arc::clone),
            allow_rpc_fallback,
        );
        meteora.set_user_authority(wallet_pubkey);

        // Meteora DLMM: If DexPoolAccounts missing, fallback to cache/RPC.
        // Format: [lb_pair, token_x_mint, token_y_mint, reserve_x?, reserve_y?, active_id:N?, bin_step:N?]
        if intent.resources.accounts.len() >= 3 {
            if let Err(e) = meteora.set_pool_from_accounts(pool_id_str, &intent.resources.accounts)
            {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("meteora_dlmm set_pool_from_accounts failed: {e}"),
                });
            }
        } else {
            let mut used_cache = false;
            if let Some(cache) = cache {
                if let Some(CachedPoolState::Meteora(dlmm_state)) = cache.get(&_pool_id) {
                    if meteora_dlmm_cache_state_injectable(&dlmm_state) {
                        if let Ok(true) =
                            meteora.inject_cached_meteora_state(&_pool_id, &dlmm_state)
                        {
                            tracing::warn!(
                                pool = %_pool_id,
                                active_id = dlmm_state.active_id,
                                "meteora_dlmm: using cached pool state (intent missing accounts)"
                            );
                            used_cache = true;
                        }
                    }
                }
            }

            if !used_cache {
                if !allow_rpc_fallback {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: "meteora pool not in LivePoolCache (GEYSER-ONLY hot path)"
                            .to_string(),
                    });
                }
                if let Err(e) = meteora.load_pool_by_address(&_pool_id).await {
                    return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: format!("meteora_dlmm load_pool_by_address failed: {e}"),
                    });
                }
            }
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

            // Use token_program from intent if provided, otherwise default to SPL Token.
            // Token-2022 tokens MUST have token_program set in TradeResources.
            let token_program_spl = intent
                .resources
                .token_program
                .as_ref()
                .and_then(|tp| Pubkey::from_str(tp).ok())
                .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
                .unwrap_or_else(spl_token::id);

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
            let token_program_spl_wsol = spl_token::id();
            let ata_ix = prog_ix_to_sdk(
                spl_associated_token_account::instruction::create_associated_token_account_idempotent(
                    &payer_spl,
                    &payer_spl,
                    &wsol_mint_spl,
                    &token_program_spl_wsol,
                ),
            );
            let mut all_ixs = vec![ata_ix];
            all_ixs.extend(ixs);

            // Close token ATA after full sell to recover rent (~0.002 SOL).
            // Only when sell_balance_hint confirms full sell of highest known balance.
            // Otherwise Custom(11) "Non-native account can only be closed if its balance is zero".
            let close_ata = intent
                .metadata
                .get("close_token_ata")
                .map(|v| v == "true")
                .unwrap_or(false);
            let full_sell_verified = sell_close_account_verified(sell_balance_hint);

            if close_ata && full_sell_verified {
                let token_mint = Pubkey::from_str(&intent.resources.input_mint).unwrap_or_default();
                let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());

                let sell_token_program_spl = intent
                    .resources
                    .token_program
                    .as_ref()
                    .and_then(|tp| Pubkey::from_str(tp).ok())
                    .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
                    .unwrap_or_else(spl_token::id);

                let token_ata_spl =
                    spl_associated_token_account::get_associated_token_address_with_program_id(
                        &payer_spl,
                        &token_mint_spl,
                        &sell_token_program_spl,
                    );

                let close_ix = if sell_token_program_spl == spl_token::id() {
                    prog_ix_to_sdk(
                        spl_token::instruction::close_account(
                            &sell_token_program_spl,
                            &token_ata_spl,
                            &payer_spl,
                            &payer_spl,
                            &[],
                        )
                        .expect("close_account instruction"),
                    )
                } else {
                    prog_ix_to_sdk(
                        spl_token_2022::instruction::close_account(
                            &sell_token_program_spl,
                            &token_ata_spl,
                            &payer_spl,
                            &payer_spl,
                            &[],
                        )
                        .expect("close_account instruction"),
                    )
                };
                all_ixs.push(close_ix);
            }

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
    let mut pumpfun = match PumpFunDex::new(rpc, cache.map(Arc::clone)) {
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

    let market_order = intent
        .metadata
        .get("market_order")
        .map(|v| v == "true")
        .unwrap_or(false);

    let ixs = match pumpfun
        .build_swap_ix_async_with_slippage(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
            Some(creator),
            intent.max_slippage_bps,
            token_program_override,
            market_order,
            allow_rpc_fallback,
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

    // SELL: Ensure the input token ATA exists (idempotent) for pure-derivation planning.
    // This matches BUY behavior (output token ATA creation) and keeps the tx-builder RPC-free.
    if intent.side == TradeSide::Sell {
        let token_mint = Pubkey::from_str(&intent.resources.input_mint).unwrap_or_default();
        let token_mint_spl = SplProgramPubkey::new_from_array(token_mint.to_bytes());
        let payer_spl = SplProgramPubkey::new_from_array(wallet_pubkey.to_bytes());

        // Use token_program from intent if provided, otherwise default to SPL Token.
        let token_program_spl = token_program_override
            .map(|pk| SplProgramPubkey::new_from_array(pk.to_bytes()))
            .unwrap_or_else(spl_token::id);

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

        // Close token ATA after full sell to recover rent (~0.002 SOL).
        // Only when sell_balance_hint confirms full sell of highest known balance.
        // Otherwise Token-2022 Custom(11): "Non-native account can only be closed if its balance is zero".
        let close_ata = intent
            .metadata
            .get("close_token_ata")
            .map(|v| v == "true")
            .unwrap_or(false);
        let full_sell_verified = sell_close_account_verified(sell_balance_hint);

        if close_ata && full_sell_verified {
            let token_ata_spl =
                spl_associated_token_account::get_associated_token_address_with_program_id(
                    &payer_spl,
                    &token_mint_spl,
                    &token_program_spl,
                );

            // Use the correct token program for close_account
            let close_ix = if token_program_spl == spl_token::id() {
                prog_ix_to_sdk(
                    spl_token::instruction::close_account(
                        &token_program_spl,
                        &token_ata_spl,
                        &payer_spl, // destination for rent
                        &payer_spl, // authority
                        &[],
                    )
                    .expect("close_account instruction"),
                )
            } else {
                // Token-2022
                prog_ix_to_sdk(
                    spl_token_2022::instruction::close_account(
                        &token_program_spl,
                        &token_ata_spl,
                        &payer_spl,
                        &payer_spl,
                        &[],
                    )
                    .expect("close_account instruction"),
                )
            };
            all_ixs.push(close_ix);
        }

        return TxPlanOutcome::Planned(TxPlan {
            instructions: all_ixs,
        });
    }

    TxPlanOutcome::Planned(TxPlan { instructions: ixs })
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
    allow_rpc_fallback: bool,
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
            allow_rpc_fallback,
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
    allow_rpc_fallback: bool,
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
        "pumpfun" => build_hop_pumpfun(wallet_pubkey, hop, amount_in, min_out, rpc, cache).await,
        "pump_amm" => {
            build_hop_pump_amm(
                wallet_pubkey,
                hop,
                &pool_address,
                amount_in,
                min_out,
                rpc,
                cache,
            )
            .await
        }
        "raydium" => {
            build_hop_raydium(
                wallet_pubkey,
                hop,
                &pool_address,
                amount_in,
                min_out,
                rpc,
                cache,
                allow_rpc_fallback,
            )
            .await
        }
        "raydium_cpmm" => {
            // TODO: Implement Raydium CPMM support for multi-hop
            Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "raydium_cpmm multi-hop not yet implemented (pool={})",
                    hop.pool_address
                ),
            })
        }
        "orca" => {
            build_hop_orca(
                wallet_pubkey,
                hop,
                &pool_address,
                amount_in,
                min_out,
                rpc,
                cache,
                allow_rpc_fallback,
            )
            .await
        }
        "meteora_dlmm" => {
            build_hop_meteora_dlmm(
                wallet_pubkey,
                hop,
                &pool_address,
                amount_in,
                min_out,
                rpc,
                cache,
                allow_rpc_fallback,
            )
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
    let (
        pool_accounts,
        sell_requires,
        sell_third,
        _sell_t0,
        _sell_t1,
        sell_fee_t0,
        sell_fee_t1,
        sell_requires_pre_fee,
        sell_pre_fee_meta_1,
        sell_requires_fee_tail,
    ) = if let Some(cache) = cache {
        match cache.get(pool_address) {
            Some(CachedPoolState::PumpAmm(amm_state)) => {
                let (sell_requires, sell_third, sell_t0, sell_t1) =
                    cache.pump_amm_sell_extended_layout(pool_address);
                let (sell_fee_t0, sell_fee_t1) = cache.pump_amm_sell_fee_tail_layout(pool_address);
                if amm_state.pool_accounts.len() >= 14 {
                    tracing::debug!(
                        pool = %pool_address,
                        accounts_len = amm_state.pool_accounts.len(),
                        "multi-hop pump_amm: using cached pool_accounts"
                    );
                    (
                        amm_state.pool_accounts.clone(),
                        sell_requires,
                        sell_third,
                        sell_t0,
                        sell_t1,
                        sell_fee_t0,
                        sell_fee_t1,
                        cache.pump_amm_sell_requires_pre_fee_metas(pool_address),
                        cache.pump_amm_sell_pre_fee_meta_1(pool_address),
                        cache.pump_amm_sell_requires_fee_tail(pool_address),
                    )
                } else {
                    return Err(UnsupportedTxPlan {
                        reason: RejectReason::UnsupportedIntent,
                        details: format!(
                            "pump_amm: cached pool_accounts too short (got {}, need 14)",
                            amm_state.pool_accounts.len()
                        ),
                    });
                }
            }
            Some(_) => {
                return Err(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!(
                        "pump_amm: cache hit but wrong DEX type for {}",
                        pool_address
                    ),
                });
            }
            None => {
                return Err(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("pump_amm: pool {} not in cache", pool_address),
                });
            }
        }
    } else {
        return Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: "pump_amm: no cache available for multi-hop".to_string(),
        });
    };

    // Use static method with pool_accounts from cache
    // Note: Multi-hop arb doesn't pass token_program yet; Token-2022 arb tokens are rare.
    // TODO: If needed, add token_program to SwapHop struct for full Token-2022 arb support.
    let is_sell_hop = hop.output_mint == NATIVE_SOL_MINT;
    let ixs = PumpFunAmmDex::build_swap_ix_from_pool_accounts_with_extended_tail(
        &hop.input_mint,
        &hop.output_mint,
        amount_in,
        min_out,
        wallet_pubkey,
        &pool_accounts,
        None, // Token-2022 not yet supported in multi-hop arb
        sell_requires && is_sell_hop,
        if is_sell_hop { sell_third } else { None },
        sell_requires_pre_fee && is_sell_hop,
        if is_sell_hop {
            sell_pre_fee_meta_1
        } else {
            None
        },
        None, // volume tails: derived for wallet in builder
        None,
        if is_sell_hop { sell_fee_t0 } else { None },
        if is_sell_hop { sell_fee_t1 } else { None },
        sell_requires_fee_tail && is_sell_hop,
        false,
    )
    .map_err(|e| UnsupportedTxPlan {
        reason: RejectReason::UnsupportedIntent,
        details: format!(
            "pump_amm build_swap_ix_from_pool_accounts_with_extended_tail failed: {}",
            e
        ),
    })?;

    Ok(ixs)
}

#[allow(clippy::too_many_arguments)]
async fn build_hop_raydium(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
    allow_rpc_fallback: bool,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let mut raydium = Raydium::new_with_live_cache(
        Arc::clone(rpc),
        cache.map(Arc::clone),
        false, // Multi-hop is Hot Path (arb) — no RPC on vault reserve miss
    );

    // Try to inject pool state from cache
    let mut used_cache = false;
    let mut has_serum_from_cache = false;
    if let Some(cache) = cache {
        if let Some(CachedPoolState::RaydiumAmm(amm_state)) = cache.get(pool_address) {
            has_serum_from_cache = amm_state.serum_bids.is_some();
            tracing::debug!(
                pool = %pool_address,
                has_serum = has_serum_from_cache,
                "multi-hop raydium: using cached pool state"
            );
            raydium.inject_raydium_amm_from_live_cache(*pool_address, &amm_state);
            used_cache = true;
        }
    }

    // Multi-hop is Hot Path only (arb) — reject on cache miss, no RPC
    if !used_cache {
        return Err(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!(
                "raydium pool {} not in LivePoolCache (multi-hop GEYSER-ONLY)",
                pool_address
            ),
        });
    }

    raydium.set_user_authority(wallet_pubkey);

    // FIX-29: Serum accounts are static — skip RPC if already in cache
    if !has_serum_from_cache {
        if !allow_rpc_fallback {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "raydium serum accounts for {} not in LivePoolCache (multi-hop GEYSER-ONLY)",
                    pool_address
                ),
            });
        }
        if let Err(e) = raydium
            .fetch_and_populate_serum_accounts(pool_address)
            .await
        {
            tracing::warn!(
                pool = %pool_address,
                error = %e,
                "raydium: serum account fetch failed (non-fatal, may already be populated)"
            );
        }
        // Write back to SLAVE LivePoolCache for subsequent trades
        if let Some(cache) = cache {
            if let Some((b, a, eq, bv, qv)) = raydium.get_serum_accounts(pool_address) {
                cache.set_raydium_serum_accounts(pool_address, b, a, eq, bv, qv);
            }
        }
    }

    let ixs = raydium
        .build_swap_ix(&hop.input_mint, &hop.output_mint, amount_in, min_out)
        .map_err(|e| UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("raydium build_swap_ix failed: {}", e),
        })?;

    Ok(ixs)
}

#[allow(clippy::too_many_arguments)]
async fn build_hop_orca(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
    allow_rpc_fallback: bool,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let orca = Orca::new_with_cache_ext(
        Arc::clone(rpc),
        None,
        cache.map(Arc::clone),
        allow_rpc_fallback,
    );
    orca.set_user_authority(wallet_pubkey);
    orca.set_skip_tick_array_rpc_validation(!allow_rpc_fallback);

    // Get pool state from cache or RPC
    let parsed = if let Some(cache) = cache {
        if let Some(CachedPoolState::Orca(orca_state)) = cache.get(pool_address) {
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
        } else {
            // Cache miss or wrong DEX type — fallback to RPC when allowed
            if !allow_rpc_fallback {
                return Err(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "orca pool not in LivePoolCache (multi-hop GEYSER-ONLY)".to_string(),
                });
            }
            fetch_orca_from_rpc(rpc, pool_address).await?
        }
    } else {
        if !allow_rpc_fallback {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: "orca pool not in LivePoolCache (multi-hop GEYSER-ONLY)".to_string(),
            });
        }
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

#[allow(clippy::too_many_arguments)]
async fn build_hop_meteora_dlmm(
    wallet_pubkey: Pubkey,
    hop: &SwapHop,
    pool_address: &Pubkey,
    amount_in: u64,
    min_out: u64,
    rpc: &Arc<SolanaRpc>,
    cache: Option<&SharedLivePoolCache>,
    allow_rpc_fallback: bool,
) -> Result<Vec<Instruction>, UnsupportedTxPlan> {
    let mut meteora = MeteoraDlmm::new_with_live_cache(
        Arc::clone(rpc),
        cache.map(Arc::clone),
        allow_rpc_fallback,
    );
    meteora.set_user_authority(wallet_pubkey);

    // Try to inject state from cache
    let mut used_cache = false;
    if let Some(cache) = cache {
        if let Some(CachedPoolState::Meteora(dlmm_state)) = cache.get(pool_address) {
            if meteora_dlmm_cache_state_injectable(&dlmm_state) {
                if let Ok(true) = meteora.inject_cached_meteora_state(pool_address, &dlmm_state) {
                    tracing::debug!(
                        pool = %pool_address,
                        active_id = dlmm_state.active_id,
                        "multi-hop meteora: using cached pool state"
                    );
                    used_cache = true;
                }
            }
        }
    }

    // If cache didn't help, load from RPC
    if !used_cache {
        if !allow_rpc_fallback {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: "meteora pool not in LivePoolCache (multi-hop GEYSER-ONLY)".to_string(),
            });
        }
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
    use crate::execution::live_pool_cache::{
        CachedPoolState, LivePoolCache, MeteoraState, PumpAmmState,
    };
    use crate::ipc::{
        DexPoolReadiness, ExplicitAmount, IntentOrigin, IntentTier, TradeExecutionConstraints,
        TradeResources, TradingRegime, NATIVE_SOL_MINT,
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

    /// Regression: `active_id == 0` / `bin_step == 0` are valid on-chain; cache must not be rejected on that alone.
    #[test]
    fn meteora_dlmm_cache_injectable_accepts_zero_active_id_with_real_vaults() {
        let vx = Pubkey::new_unique();
        let vy = Pubkey::new_unique();
        let s = MeteoraState {
            token_x_mint: Pubkey::new_unique(),
            token_y_mint: Pubkey::new_unique(),
            reserve_x: vx,
            reserve_y: vy,
            active_id: 0,
            bin_step: 0,
            reserve_x_balance: Some(1),
            reserve_y_balance: Some(2),
        };
        assert!(super::meteora_dlmm_cache_state_injectable(&s));
    }

    #[test]
    fn meteora_dlmm_cache_injectable_rejects_default_vaults() {
        let s = MeteoraState {
            token_x_mint: Pubkey::new_unique(),
            token_y_mint: Pubkey::new_unique(),
            reserve_x: Pubkey::default(),
            reserve_y: Pubkey::new_unique(),
            active_id: 42,
            bin_step: 10,
            reserve_x_balance: Some(1),
            reserve_y_balance: Some(2),
        };
        assert!(!super::meteora_dlmm_cache_state_injectable(&s));
    }

    /// Scope 51: cold-path SELL with non-empty `resources.accounts` must use **this pool’s**
    /// explicit JetStream `DexPoolReadiness::Ready` v14 (not stale intent), so extended `third_meta`
    /// from SLAVE feeds `build_swap_ix_from_pool_accounts`.
    #[tokio::test]
    async fn pump_amm_cold_path_sell_prefers_explicit_jetstream_ready_v14_over_stale_intent_accounts(
    ) {
        let pool_market = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(NATIVE_SOL_MINT).expect("wsol mint");
        let third_meta = Pubkey::new_unique();
        let tail0 = Pubkey::new_unique();
        let tail1 = Pubkey::new_unique();

        let mut ready_v14: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        ready_v14[0] = pool_market;
        ready_v14[2] = base_mint;
        ready_v14[3] = quote_mint;

        let mut stale_intent_accounts: Vec<String> =
            (0..14).map(|_| Pubkey::new_unique().to_string()).collect();
        stale_intent_accounts[0] = pool_market.to_string();

        let cache = LivePoolCache::new();
        cache.upsert(
            pool_market,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: ready_v14.clone(),
                creator: None,
            }),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_market, DexPoolReadiness::Ready);
        cache.merge_pump_amm_sell_extended_layout(
            &pool_market,
            true,
            Some(third_meta),
            Some(tail0),
            Some(tail1),
            None,
            None,
            false,
            false,
            None,
        );

        let mut intent = base_intent();
        intent
            .metadata
            .insert("dex".to_string(), "pump_amm".to_string());
        intent.resources.input_mint = base_mint.to_string();
        intent.resources.output_mint = NATIVE_SOL_MINT.to_string();
        intent.resources.pools = vec![pool_market.to_string()];
        intent.resources.accounts = stale_intent_accounts;
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(1, 9)),
        });

        let wallet = Pubkey::new_unique();
        let rpc = Arc::new(crate::solana::rpc::SolanaRpc::new("http://127.0.0.1:8899"));
        let cache_arc = Arc::new(cache);

        let outcome =
            super::build_tx_plan(&intent, wallet, rpc, Some(&cache_arc), None, true).await;

        let super::TxPlanOutcome::Planned(plan) = outcome else {
            panic!("expected Planned outcome, got {outcome:?}");
        };
        assert!(!plan.instructions.is_empty());
        let sell_ix = &plan.instructions[plan.instructions.len() - 1];
        assert!(
            sell_ix.accounts.len() > 21,
            "expected extended SELL metas, got len={}",
            sell_ix.accounts.len()
        );
        assert_eq!(sell_ix.accounts.last().unwrap().pubkey, third_meta);
        let quote_tp = Pubkey::new_from_array(spl_token::id().to_bytes());
        let (derived_21, derived_22) =
            PumpFunAmmDex::pump_amm_sell_cashback_first_two_metas(wallet, quote_mint, quote_tp);
        assert_eq!(sell_ix.accounts[21].pubkey, derived_21);
        assert_eq!(sell_ix.accounts[22].pubkey, derived_22);
        assert_ne!(derived_21, tail0);
        assert_ne!(derived_22, tail1);
        assert!(sell_ix.accounts[21].is_writable);
        assert!(sell_ix.accounts[22].is_writable);
        assert!(
            !sell_ix.accounts[23].is_writable,
            "derived-user extended SELL: #23 third_meta must be readonly"
        );
    }

    /// Two PumpSwap pools for the same base mint: only the intent’s `pools[0]` row (explicit Ready)
    /// must drive the override — not another pool that happens to be legacy-“ready” first.
    #[tokio::test]
    async fn pump_amm_cold_path_sell_multi_pool_targets_intent_pool_explicit_ready_only() {
        let pool_other = Pubkey::new_unique();
        let pool_target = Pubkey::new_unique();
        let base_mint = Pubkey::new_unique();
        let quote_mint = Pubkey::from_str(NATIVE_SOL_MINT).expect("wsol mint");
        let third_other = Pubkey::new_unique();
        let third_target = Pubkey::new_unique();
        let t0_other = Pubkey::new_unique();
        let t1_other = Pubkey::new_unique();
        let t0_target = Pubkey::new_unique();
        let t1_target = Pubkey::new_unique();

        let mut v14_other: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        v14_other[0] = pool_other;
        v14_other[2] = base_mint;
        v14_other[3] = quote_mint;

        let mut v14_target: Vec<Pubkey> = (0..14).map(|_| Pubkey::new_unique()).collect();
        v14_target[0] = pool_target;
        v14_target[2] = base_mint;
        v14_target[3] = quote_mint;

        let mut stale_intent_accounts: Vec<String> =
            (0..14).map(|_| Pubkey::new_unique().to_string()).collect();
        stale_intent_accounts[0] = pool_target.to_string();

        let cache = LivePoolCache::new();
        // Other pool: legacy-effective ready only (no explicit readiness map merge).
        cache.upsert(
            pool_other,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: v14_other.clone(),
                creator: None,
            }),
            100,
        );
        cache.merge_pump_amm_sell_extended_layout(
            &pool_other,
            true,
            Some(third_other),
            Some(t0_other),
            Some(t1_other),
            None,
            None,
            false,
            false,
            None,
        );

        // Target pool: explicit JetStream Ready (force_refresh / PoolCacheUpdate path).
        cache.upsert(
            pool_target,
            CachedPoolState::PumpAmm(PumpAmmState {
                base_mint,
                quote_mint,
                pool_base_token_account: Pubkey::new_unique(),
                pool_quote_token_account: Pubkey::new_unique(),
                base_reserve: Some(1_000_000_000),
                quote_reserve: Some(50_000_000_000),
                pool_accounts: v14_target.clone(),
                creator: None,
            }),
            100,
        );
        cache.merge_pump_amm_pool_accounts_readiness(pool_target, DexPoolReadiness::Ready);
        cache.merge_pump_amm_sell_extended_layout(
            &pool_target,
            true,
            Some(third_target),
            Some(t0_target),
            Some(t1_target),
            None,
            None,
            false,
            false,
            None,
        );

        let mut intent = base_intent();
        intent
            .metadata
            .insert("dex".to_string(), "pump_amm".to_string());
        intent.resources.input_mint = base_mint.to_string();
        intent.resources.output_mint = NATIVE_SOL_MINT.to_string();
        intent.resources.pools = vec![pool_target.to_string()];
        intent.resources.accounts = stale_intent_accounts;
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(1, 9)),
        });

        let wallet = Pubkey::new_unique();
        let rpc = Arc::new(crate::solana::rpc::SolanaRpc::new("http://127.0.0.1:8899"));
        let cache_arc = Arc::new(cache);

        let outcome =
            super::build_tx_plan(&intent, wallet, rpc, Some(&cache_arc), None, true).await;

        let super::TxPlanOutcome::Planned(plan) = outcome else {
            panic!("expected Planned outcome, got {outcome:?}");
        };
        let sell_ix = &plan.instructions[plan.instructions.len() - 1];
        assert_eq!(sell_ix.accounts.last().unwrap().pubkey, third_target);
        assert_ne!(sell_ix.accounts.last().unwrap().pubkey, third_other);
    }

    #[test]
    fn sell_close_account_verified_rejects_when_required_below_known_balance() {
        assert!(!sell_close_account_verified(Some((
            41_491_153_724,
            10_490_465_194
        ))));
    }

    #[test]
    fn sell_close_account_verified_allows_when_required_equals_known_balance() {
        assert!(sell_close_account_verified(Some((
            41_491_153_724,
            41_491_153_724
        ))));
    }

    #[test]
    fn sell_close_account_verified_rejects_when_required_above_known_balance() {
        assert!(!sell_close_account_verified(Some((
            10_490_465_194,
            41_491_153_724
        ))));
    }
}
