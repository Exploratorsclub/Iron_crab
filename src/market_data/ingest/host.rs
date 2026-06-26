//! Ingest host trait — bin implements via `MarketDataContext` (`impl IngestHost`).

use solana_sdk::pubkey::Pubkey;

/// Lock-free ingest surface for Geyser account/TX filters (I-4b: no `tracked_*` map reads).
pub trait IngestHost: Send + Sync {
    /// Tracked wallet + WSOL ATA pubkeys when wallet tracking is active.
    fn ingest_tracked_wallet_pubkeys(&self) -> Option<(Pubkey, Pubkey)>;

    fn ingest_tracked_wallet_token_account_contains(&self, pubkey: &Pubkey) -> bool;

    /// Membership snapshot: vault, mint, or bin-array pubkey.
    fn ingest_membership_contains(&self, pubkey: &Pubkey) -> bool;

    fn ingest_membership_vault_contains(&self, pubkey: &Pubkey) -> bool;

    fn ingest_membership_bin_array_contains(&self, pubkey: &Pubkey) -> bool;

    fn ingest_is_hot_pool(&self, pool: &Pubkey) -> bool;

    fn ingest_pool_mint_map_contains(&self, pool: &Pubkey) -> bool;

    fn ingest_high_priority_bonding_curve_contains(&self, pool: &Pubkey) -> bool;

    fn ingest_wallet_tracks_mint(&self, mint: &Pubkey) -> bool;

    /// Pump.fun bonding-curve PDA relevant for a wallet-tracked mint.
    fn ingest_pumpfun_bonding_curve_tracks_wallet(&self, pool: &Pubkey) -> bool;

    /// Pump.fun pool account: wallet tracks the pool's token mint (via live cache).
    fn ingest_pumpfun_wallet_tracks_pool_mint(&self, pool: &Pubkey) -> bool;

    fn ingest_record_membership_snapshot_hit(&self);

    fn ingest_record_vault_high_priority_dispatch(&self);
}
