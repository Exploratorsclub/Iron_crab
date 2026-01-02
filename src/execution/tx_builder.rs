use crate::ipc::{RejectReason, TradeIntent, TradeSide};
use crate::solana::dex::pumpfun::PumpFunDex;
use crate::solana::rpc::SolanaRpc;
use solana_sdk::hash::hash;
use solana_sdk::instruction::Instruction;
use solana_sdk::pubkey::Pubkey;
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
    if intent.side != TradeSide::Buy {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("side={:?} (only Buy supported)", intent.side),
        });
    }

    if intent.resources.input_mint != SOL_MINT {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!(
                "input_mint={} (only SOL mint supported)",
                intent.resources.input_mint
            ),
        });
    }

    if intent.resources.pools.len() != 1 {
        return TxPlanOutcome::Unsupported(UnsupportedTxPlan {
            reason: RejectReason::UnsupportedIntent,
            details: format!("pools_len={} (expected exactly 1)", intent.resources.pools.len()),
        });
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

    // For BUY, pump.fun expects `min_out` = desired token amount.
    // Strategy must provide it (raw base units of output mint).
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

    // Currently we only implement a Pump.fun BUY plan (single pool SOL->token).
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
