use crate::config::{AllocatorCfg, MarketCfg};
use crate::solana::rpc::SolanaRpc;
use crate::wallet::Treasury;
use anyhow::Result;
use tracing::info;

/// Simpler Platzhalter: logische Allokation (keine echten Transfers)
pub struct Allocator {
    cfg: AllocatorCfg,
}

impl Allocator {
    pub fn new(cfg: AllocatorCfg) -> Self {
        Self { cfg }
    }

    pub async fn rebalance(
        &self,
        treasury: &Treasury,
        markets: &[MarketCfg],
        rpc: &SolanaRpc,
    ) -> Result<()> {
        let sum: u32 = markets.iter().map(|m| m.allocation_pct).sum();
        if sum != 100 {
            tracing::warn!("allocation sum = {sum}, expected 100");
        }
        let lamports = treasury.sol_balance(rpc).await.unwrap_or_default();
        info!(treasury = %treasury.pubkey(), lamports, mode = %self.cfg.mode, rebalance_secs = self.cfg.rebalance_secs, min_transfer_sol = self.cfg.min_transfer_sol, "Rebalance tick (logical)");
        for m in markets {
            info!(market = %m.name, pct = m.allocation_pct, "Target allocation");
        }
        Ok(())
    }
}
