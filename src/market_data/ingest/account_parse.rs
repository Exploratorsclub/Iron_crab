//! Account-only parse / emit helpers (Geyser account path only).

use crate::ipc::MarketEventKind;
use crate::metrics::MarketDataLatencySegment;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_option::COption;
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;

/// Parse a Token or Token-2022 mint account for decimals/supply/authorities.
pub fn try_parse_mint_account(
    owner: &Pubkey,
    data: &[u8],
) -> Option<(u8, u64, Option<String>, Option<String>)> {
    if owner.to_bytes() == spl_token::ID.to_bytes() {
        let mint = spl_token::state::Mint::unpack(data).ok()?;
        let mint_authority = match mint.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match mint.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((mint.decimals, mint.supply, mint_authority, freeze_authority))
    } else if owner.to_bytes() == spl_token_2022::ID.to_bytes() {
        let mint = StateWithExtensions::<spl_token_2022::state::Mint>::unpack(data).ok()?;
        let base = mint.base;
        let mint_authority = match base.mint_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        let freeze_authority = match base.freeze_authority {
            COption::Some(p) => Some(p.to_string()),
            COption::None => None,
        };
        Some((base.decimals, base.supply, mint_authority, freeze_authority))
    } else {
        None
    }
}

/// Parse a Token Account to extract the balance (amount).
/// Works with both spl-token and spl-token-2022 accounts.
pub fn try_parse_token_account_balance(data: &[u8]) -> Option<u64> {
    // SPL Token Account layout: 165 bytes
    // Offset 64: amount (u64, little-endian)
    if data.len() >= 72 {
        let amount_bytes: [u8; 8] = data[64..72].try_into().ok()?;
        Some(u64::from_le_bytes(amount_bytes))
    } else {
        None
    }
}

/// WSOL wrapped-SOL token account: balance from a Geyser account update, or `None` to skip.
/// Empty `data` means the account no longer exists as a token account (e.g. ATA closed) → `Some(0)`.
pub fn wsol_ata_balance_lamports_from_geyser_data(data: &[u8]) -> Option<u64> {
    if data.is_empty() {
        return Some(0);
    }
    try_parse_token_account_balance(data)
}

/// Which tracked-wallet Geyser account produced a balance update.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletGeyserUpdateSource {
    NativeSol { lamports: u64, prev_lamports: u64 },
    WsolAta { balance: u64, prev_balance: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalletGeyserSnapshotMint {
    NativeSol,
    Wsol,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalletGeyserSnapshotToPublish {
    pub mint: WalletGeyserSnapshotMint,
    pub balance_raw: u64,
}

/// One Geyser account update → at most one wallet snapshot. Never cross-publish the other mint from cache.
pub fn wallet_geyser_snapshots_to_publish(
    source: WalletGeyserUpdateSource,
) -> Option<WalletGeyserSnapshotToPublish> {
    match source {
        WalletGeyserUpdateSource::NativeSol {
            lamports,
            prev_lamports,
        } => (lamports != prev_lamports).then_some(WalletGeyserSnapshotToPublish {
            mint: WalletGeyserSnapshotMint::NativeSol,
            balance_raw: lamports,
        }),
        WalletGeyserUpdateSource::WsolAta {
            balance,
            prev_balance,
        } => (balance != prev_balance).then_some(WalletGeyserSnapshotToPublish {
            mint: WalletGeyserSnapshotMint::Wsol,
            balance_raw: balance,
        }),
    }
}

#[inline]
pub(crate) fn account_publish_segment(kind: &MarketEventKind) -> MarketDataLatencySegment {
    match kind {
        MarketEventKind::Trade { .. } => MarketDataLatencySegment::Trade,
        MarketEventKind::BondingCurveProgress { .. } => MarketDataLatencySegment::BondingCurve,
        MarketEventKind::PoolCreated { .. } => MarketDataLatencySegment::PoolCreated,
        _ => MarketDataLatencySegment::Other,
    }
}
