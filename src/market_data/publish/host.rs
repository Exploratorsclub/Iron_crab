//! Publish host trait — bin implements via `MarketDataContext` (`impl PublishHost`).

use solana_sdk::pubkey::Pubkey;

/// Side effects after successful core NATS publish (bonding-curve timing metrics).
pub trait PublishHost: Send + Sync {
    fn on_pumpfun_trade_core_published(
        &self,
        pool_address: &str,
        now_ms: u64,
        trade_slot: Option<u64>,
    );

    fn on_bonding_curve_progress_core_published(
        &self,
        bonding_curve: &str,
        now_ms: u64,
        slot: Option<u64>,
    );

    /// Resolve bonding-curve pubkey for trade-after-bonding latency (optional).
    fn last_bonding_wall_ms(&self, curve: &Pubkey) -> Option<u64> {
        let _ = curve;
        None
    }

    fn last_bonding_slot(&self, curve: &Pubkey) -> Option<u64> {
        let _ = curve;
        None
    }
}
