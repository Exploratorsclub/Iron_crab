use crate::ipc::{RejectReason, TradeIntent, TradeSide};
use crate::solana::dex::Dex;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::orca_whirlpool_layout;
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::dex::pumpfun_amm::PumpFunAmmDex;
use crate::solana::dex::raydium::Raydium;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::hash::hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::pubkey::Pubkey as SplProgramPubkey;
use std::str::FromStr;
use std::sync::Arc;

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

fn min_out_raw_from_intent(intent: &TradeIntent) -> Result<u64, UnsupportedTxPlan> {
    if let Some(execution) = intent.execution.as_ref() {
        if let Some(min_out) = execution.min_out.as_ref() {
            return Ok(min_out.raw);
        }
    }

    // Legacy fallback (stringly-typed metadata)
    // NOTE: We keep this for backward compatibility, but new producers should
    // use intent.execution.min_out.
    let min_out_raw = match intent.metadata.get("min_out_raw") {
        Some(v) => v,
        None => {
            return Err(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: "missing execution.min_out and metadata.min_out_raw (required for deterministic tx plan)".to_string(),
            })
        }
    };

    match min_out_raw.parse() {
        Ok(v) => Ok(v),
        Err(e) => Err(UnsupportedTxPlan {
            reason: RejectReason::InvalidIntent,
            details: format!("invalid metadata.min_out_raw (u64): {e}"),
        }),
    }
}

pub async fn build_tx_plan(
    intent: &TradeIntent,
    wallet_pubkey: Pubkey,
    rpc: Arc<SolanaRpc>,
    rpc_url: &str,
    helius_rpc_url: Option<&str>,
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
            details: format!("pools_len={} (expected exactly 1)", intent.resources.pools.len()),
        });
    }

    // Strategy must provide min_out for deterministic planning.
    // - For Pump.fun BUY: min token out
    // - For Pump.fun SELL: min SOL out (lamports)
    // - For Orca: min output amount in raw base units of output mint
    let min_out: u64 = match min_out_raw_from_intent(intent) {
        Ok(v) => v,
        Err(e) => {
            return TxPlanOutcome::Unsupported(e);
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

        let acct = match rpc.rpc.get_account(&pool_id).await {
            Ok(a) => a,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("orca whirlpool account fetch failed: {e}"),
                })
            }
        };

        let parsed = match orca_whirlpool_layout::parse_whirlpool(&acct.data) {
            Some(p) => p,
            None => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: "orca whirlpool parse failed (invalid layout/size)".to_string(),
                })
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

        return TxPlanOutcome::Planned(TxPlan { instructions: ixs });
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

        // Load the specific pool snapshot into the Raydium adapter.
        if let Err(e) = raydium.load_pool_from_geyser(&pool_id).await {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: format!("raydium load_pool_from_geyser failed: {e}"),
            });
        }

        // Raydium tx building needs Serum/OpenBook market accounts.
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

        return TxPlanOutcome::Planned(TxPlan { instructions: ixs });
    }

    if dex_hint == DexHint::PumpAmm {
        // Pump.fun AMM (PumpSwap) planning uses the pool/market pubkey in `resources.pools[0]`.
        // We still discover auxiliary accounts from chain for safety and determinism.
        let pool_id_str = &intent.resources.pools[0];
        let _pool_id = match Pubkey::from_str(pool_id_str) {
            Ok(pk) => pk,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid resources.pools[0] pubkey for pump_amm: {e}"),
                })
            }
        };

        let base_mint = match intent.side {
            TradeSide::Buy => Pubkey::from_str(&intent.resources.output_mint),
            TradeSide::Sell => Pubkey::from_str(&intent.resources.input_mint),
        };
        let base_mint = match base_mint {
            Ok(m) => m,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::InvalidIntent,
                    details: format!("invalid base mint pubkey: {e}"),
                })
            }
        };

        let mut pump_amm = PumpFunAmmDex::new(
            Arc::clone(&rpc),
            rpc_url.to_string(),
            helius_rpc_url.map(|s| s.to_string()),
        );
        pump_amm.set_user_authority(wallet_pubkey);

        // Prime caches so build_swap_ix can run synchronously.
        if let Err(e) = pump_amm.ensure_discovered_for_user(base_mint, wallet_pubkey).await {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::QuoteUnavailable,
                details: format!("pump_amm discovery failed: {e}"),
            });
        }

        let ixs = match pump_amm.build_swap_ix(
            &intent.resources.input_mint,
            &intent.resources.output_mint,
            intent.required_capital.raw,
            min_out,
        ) {
            Ok(ixs) => ixs,
            Err(e) => {
                return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                    reason: RejectReason::UnsupportedIntent,
                    details: format!("pump_amm build failed: {e}"),
                })
            }
        };

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

    TxPlanOutcome::Planned(TxPlan { instructions: ixs })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ipc::{ExplicitAmount, IntentOrigin, IntentTier, TradeExecutionConstraints, TradeResources, TradingRegime};

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
        ray.metadata.insert("dex".to_string(), "raydium".to_string());
        assert_eq!(super::dex_hint_from_intent(&ray).unwrap(), super::DexHint::Raydium);

        let mut orca = base_intent();
        orca.metadata.insert("dex".to_string(), "orca".to_string());
        assert_eq!(super::dex_hint_from_intent(&orca).unwrap(), super::DexHint::Orca);

        let mut pump = base_intent();
        pump.metadata.insert("dex".to_string(), "pumpfun".to_string());
        assert_eq!(super::dex_hint_from_intent(&pump).unwrap(), super::DexHint::Pumpfun);

        let mut legacy = base_intent();
        legacy
            .metadata
            .insert("creator".to_string(), "11111111111111111111111111111111".to_string());
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

        intent.metadata.insert("min_out_raw".to_string(), "123".to_string());
        intent.execution = Some(TradeExecutionConstraints {
            min_out: Some(ExplicitAmount::new(999, 9)),
        });

        let parsed = super::min_out_raw_from_intent(&intent).unwrap();

        assert_eq!(parsed, 999);
    }

    #[test]
    fn min_out_falls_back_to_legacy_metadata() {
        let mut intent = base_intent();

        intent.metadata.insert("min_out_raw".to_string(), "456".to_string());

        let parsed = super::min_out_raw_from_intent(&intent).unwrap();

        assert_eq!(parsed, 456);
    }
}
