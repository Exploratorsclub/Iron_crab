use crate::ipc::{RejectReason, TradeIntent, TradeSide};
use crate::solana::dex::Dex;
use crate::solana::dex::orca::Orca;
use crate::solana::dex::orca_whirlpool_layout;
use crate::solana::dex::pumpfun::PumpFunDex;
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

pub async fn build_tx_plan(
    intent: &TradeIntent,
    wallet_pubkey: Pubkey,
    rpc: Arc<SolanaRpc>,
) -> TxPlanOutcome {
    let dex_hint = intent.metadata.get("dex").map(|s| s.as_str());

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
    let min_out_raw = match intent.metadata.get("min_out_raw") {
        Some(v) => v,
        None => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::UnsupportedIntent,
                details: "missing metadata.min_out_raw (required for deterministic tx plan)"
                    .to_string(),
            })
        }
    };

    let min_out: u64 = match min_out_raw.parse() {
        Ok(v) => v,
        Err(e) => {
            return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
                reason: RejectReason::InvalidIntent,
                details: format!("invalid metadata.min_out_raw (u64): {e}"),
            })
        }
    };

    if dex_hint == Some("orca") {
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
