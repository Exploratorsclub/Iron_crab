use crate::execution::live_pool_cache::{CachedPoolState, SharedLivePoolCache};
use crate::ipc::{RejectReason, TradeIntent, TradeSide};
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
/// This is required for BUY trades (SOL → Token) because DEX connectors
/// expect WSOL in the ATA, not native SOL.
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
}

fn dex_hint_from_intent(intent: &TradeIntent) -> Result<DexHint, UnsupportedTxPlan> {
    match intent.metadata.get("dex").map(|s| s.as_str()) {
        Some("raydium") => Ok(DexHint::Raydium),
        Some("orca") => Ok(DexHint::Orca),
        Some("pumpfun") => Ok(DexHint::Pumpfun),
        Some("pump_amm") => Ok(DexHint::PumpAmm),
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
                            details: format!("no min_out in intent and cache calculation failed: {e}"),
                        });
                    }
                }
            } else {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "missing execution.min_out and no cache available for fresh calculation".to_string(),
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

        // For BUY trades (SOL → Token), prepend wrap SOL instructions
        let final_ixs = if intent.side == TradeSide::Buy {
            let mut all_ixs = build_wrap_sol_instructions(wallet_pubkey, intent.required_capital.raw);
            all_ixs.extend(ixs);
            all_ixs
        } else {
            ixs
        };

        return TxPlanOutcome::Planned(TxPlan { instructions: final_ixs });
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

        // For BUY trades (SOL → Token), prepend wrap SOL instructions
        let final_ixs = if intent.side == TradeSide::Buy {
            let mut all_ixs = build_wrap_sol_instructions(wallet_pubkey, intent.required_capital.raw);
            all_ixs.extend(ixs);
            all_ixs
        } else {
            ixs
        };

        return TxPlanOutcome::Planned(TxPlan { instructions: final_ixs });
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

        if intent.resources.accounts.len() != 12 {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!(
                    "pump_amm requires resources.accounts (DexPoolAccounts v2, len=12, no volume accumulators), got len={}",
                    intent.resources.accounts.len()
                ),
            });
        }

        let mut pool_accounts: Vec<Pubkey> = Vec::with_capacity(12);
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

        // For BUY trades (SOL → Token), prepend wrap SOL instructions
        // For SELL trades, just ensure WSOL ATA exists (for receiving output)
        if intent.side == TradeSide::Buy {
            let wrap_ixs = build_wrap_sol_instructions(wallet_pubkey, intent.required_capital.raw);
            for (i, ix) in wrap_ixs.into_iter().enumerate() {
                ixs.insert(i, ix);
            }
        } else {
            // SELL: just ensure WSOL ATA exists (idempotent create)
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

    let ixs = match pumpfun
        .build_swap_ix_async_with_slippage(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
            Some(creator),
            intent.max_slippage_bps,
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

    // For BUY trades (SOL → Token), prepend wrap SOL instructions
    // Pump.fun BC uses native SOL for buys, but WSOL ATA is still needed
    // for consistency and potential WSOL interactions
    let final_ixs = if intent.side == TradeSide::Buy {
        let mut all_ixs = build_wrap_sol_instructions(wallet_pubkey, intent.required_capital.raw);
        all_ixs.extend(ixs);
        all_ixs
    } else {
        ixs
    };

    TxPlanOutcome::Planned(TxPlan { instructions: final_ixs })
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
