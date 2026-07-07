//! TX-only parse / emit helpers (Geyser transaction path only).

use super::tx_host::TxIngestHost;
use crate::ipc::{MarketEvent, MarketEventKind, NATIVE_SOL_MINT};
use crate::market_data::md_state::{md_state_try_enqueue, MdStateCommand, MdStateSender};
use crate::market_data::publish::{
    account_path_enqueue_core_market_event, account_path_enqueue_jetstream, AccountPublishSender,
};
use crate::market_data::sidefx::MarketEventCorePublishTrace;
use crate::metrics::{
    inc_market_data_devwallet_tx_published_total, record_market_data_pool_mint_map_to_devwallet_ms,
    MarketDataLatencySegment,
};
use crate::nats::{wallet_snapshot_subject, NatsClient};
use crate::solana::dex_parser::{
    METEORA_DLMM, ORCA_WHIRLPOOL, PUMPFUN_AMM_PROGRAM, PUMPFUN_PROGRAM, RAYDIUM_AMM_V4,
    RAYDIUM_CPMM,
};
use crate::solana::geyser_listener::{
    geyser_token_balance_account_pubkey, geyser_tx_involves_wallet,
    geyser_wallet_post_token_balances, GeyserTransactionUpdate, TokenBalance,
};
use solana_sdk::pubkey::Pubkey;
use std::str::FromStr;
use std::time::Instant;
use tracing::{info, warn};

/// Wallet-owned WSOL `post_token_balances` row present — skip paired NATIVE_SOL publish (PR #201).
pub fn wallet_tx_meta_has_wsol_post_balance(wallet: &Pubkey, tx: &GeyserTransactionUpdate) -> bool {
    let wallet_str = wallet.to_string();
    tx.post_token_balances
        .iter()
        .any(|b| b.mint == NATIVE_SOL_MINT && b.owner.as_deref() == Some(wallet_str.as_str()))
}

pub fn wallet_tx_meta_native_sol_post_lamports(
    wallet: &Pubkey,
    tx: &GeyserTransactionUpdate,
) -> Option<u64> {
    let idx = tx.account_keys.iter().position(|k| k == wallet)?;
    tx.post_balances.get(idx).copied()
}

/// Idempotent ATA pin + Geyser subscribe list refresh after a wallet TX meta balance row.
pub fn wallet_tx_meta_pin_ata_from_balance<H: TxIngestHost>(
    host: &H,
    tx: &GeyserTransactionUpdate,
    balance: &TokenBalance,
    md_state: &MdStateSender,
) -> bool {
    let Some(ata) = geyser_token_balance_account_pubkey(tx, balance) else {
        return false;
    };
    let added_ata = host.tx_wallet_token_account_insert(ata);
    if let Ok(mint) = Pubkey::from_str(&balance.mint) {
        if host.tx_wallet_mint_needs_track(&mint) {
            md_state_try_enqueue(md_state, MdStateCommand::TrackWalletMint { mint });
        }
        host.tx_wallet_mint_decimals_insert(mint, balance.ui_token_amount.decimals);
    }
    if added_ata {
        host.tx_wallet_notify_geyser_subscribe_accounts_changed();
    }
    added_ata
}

/// Phase 1 (PR1): publish `WalletBalanceSnapshot` from Geyser TX `post_token_balances` / native SOL meta.
pub async fn process_wallet_balance_snapshots_from_tx_meta<H: TxIngestHost>(
    host: &H,
    run_id: &str,
    tx: &GeyserTransactionUpdate,
    account_publish_tx: Option<&AccountPublishSender>,
    md_state: &MdStateSender,
) {
    let Some(tracked_wallet) = host.tx_tracked_wallet_view() else {
        return;
    };
    let wallet = tracked_wallet.wallet;
    if !geyser_tx_involves_wallet(&wallet, tx) {
        return;
    }

    let wallet_str = wallet.to_string();
    let has_wsol_post = wallet_tx_meta_has_wsol_post_balance(&wallet, tx);
    let can_publish = host.tx_nats().is_some() || account_publish_tx.is_some();

    if !has_wsol_post {
        if let Some(lamports) = wallet_tx_meta_native_sol_post_lamports(&wallet, tx) {
            let prev = host.tx_wallet_native_sol_swap(lamports);
            if lamports != prev && can_publish {
                let sol_snapshot = MarketEvent::new(
                    "market-data",
                    host.tx_build_version(),
                    run_id,
                    format!("geyser_wallet_tx_sol_{}", tx.signature),
                    "geyser_wallet_tx_meta",
                    Some(tx.slot),
                    MarketEventKind::WalletBalanceSnapshot {
                        mint: "NATIVE_SOL".to_string(),
                        balance_raw: lamports,
                        decimals: 9,
                        token_program: "system".to_string(),
                    },
                );
                let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                account_path_enqueue_jetstream(
                    account_publish_tx,
                    host.tx_nats(),
                    sol_subject,
                    &sol_snapshot,
                    "native SOL WalletBalanceSnapshot (TX meta)",
                    false,
                )
                .await;
                info!(
                    wallet = %wallet_str,
                    sol_lamports = lamports,
                    slot = tx.slot,
                    sig = %tx.signature,
                    "WalletBalanceSnapshot (NATIVE_SOL) enqueued from TX meta"
                );
            }
        }
    }

    for balance in geyser_wallet_post_token_balances(&wallet, tx) {
        let balance_raw = match balance.ui_token_amount.amount.parse::<u64>() {
            Ok(v) => v,
            Err(e) => {
                warn!(
                    wallet = %wallet_str,
                    mint = %balance.mint,
                    amount = %balance.ui_token_amount.amount,
                    error = %e,
                    sig = %tx.signature,
                    "Skipping wallet TX meta balance: invalid raw amount"
                );
                continue;
            }
        };
        let decimals = balance.ui_token_amount.decimals;
        let token_program = balance
            .program_id
            .clone()
            .unwrap_or_else(|| spl_token::ID.to_string());

        let snapshot_mint = if balance.mint == NATIVE_SOL_MINT {
            host.tx_wallet_wsol_store(balance_raw);
            host.tx_wallet_wsol_seen_set();
            NATIVE_SOL_MINT.to_string()
        } else {
            balance.mint.clone()
        };

        if can_publish {
            let event = MarketEvent::new(
                "market-data",
                host.tx_build_version(),
                run_id,
                format!("geyser_wallet_tx_{}_{}", tx.signature, snapshot_mint),
                "geyser_wallet_tx_meta",
                Some(tx.slot),
                MarketEventKind::WalletBalanceSnapshot {
                    mint: snapshot_mint.clone(),
                    balance_raw,
                    decimals,
                    token_program: token_program.clone(),
                },
            );
            let subject = wallet_snapshot_subject(&wallet_str, &snapshot_mint);
            account_path_enqueue_jetstream(
                account_publish_tx,
                host.tx_nats(),
                subject,
                &event,
                "WalletBalanceSnapshot JetStream (TX meta)",
                true,
            )
            .await;
            info!(
                wallet = %wallet_str,
                mint = %snapshot_mint,
                balance_raw,
                slot = tx.slot,
                sig = %tx.signature,
                "WalletBalanceSnapshot enqueued from TX meta"
            );
        }

        wallet_tx_meta_pin_ata_from_balance(host, tx, balance, md_state);
    }
}

/// Resolve PumpFun creator on TX ingest (P1): parse → mint cache → pool cache → LivePoolCache (no RPC).
pub fn resolve_pumpfun_creator_tx_path<H: TxIngestHost>(
    host: &H,
    pool_address: &str,
    mint: &str,
    pool_pubkey: &Pubkey,
    parsed_creator: Option<&Pubkey>,
) -> Option<String> {
    if let Some(c) = parsed_creator {
        return Some(c.to_string());
    }
    if let Some(c) = host.tx_creator_cache_get(mint) {
        if !c.is_empty() {
            return Some(c);
        }
    }
    if let Some(c) = host.tx_pool_creator_cache_get(pool_address) {
        if !c.is_empty() {
            return Some(c);
        }
    }
    host.tx_live_pool_pumpfun_creator(pool_pubkey)
        .filter(|pk| *pk != Pubkey::default())
        .map(|pk| pk.to_string())
}

/// After `pool_mint_map` insert on the TX path: emit `DevWalletIdentified` when creator is known
/// from TX parse, pool_creator_cache, or LivePoolCache (no Swap parsing; I-4 / BUG-D).
///
/// NATS: when `publish_tx` is set (same channel as dedicated publish runtime), enqueue only —
/// avoids blocking the TX ingest task on Core NATS backpressure (PR151 follow-up).
#[allow(clippy::too_many_arguments)]
pub async fn maybe_emit_dev_wallet_after_pool_mint_map<H: TxIngestHost>(
    host: &H,
    run_id: &str,
    pool: &Pubkey,
    mint: &str,
    slot: Option<u64>,
    tx_grpc_recv_at: Instant,
    publish_tx: Option<&AccountPublishSender>,
    nats: Option<&NatsClient>,
    creator_override: Option<&str>,
) -> bool {
    let pool_str = pool.to_string();
    let creator_str = if let Some(c) = creator_override.filter(|s| !s.is_empty()) {
        c.to_string()
    } else if let Some(s) = host
        .tx_pool_creator_cache_get(&pool_str)
        .filter(|s| !s.is_empty())
    {
        s
    } else {
        match host.tx_live_pool_pumpfun_creator(pool) {
            Some(pk) if pk != Pubkey::default() => pk.to_string(),
            _ => return false,
        }
    };
    host.tx_pool_creator_cache_insert(pool_str.clone(), creator_str.clone());
    let existing =
        host.tx_creator_cache_insert_returning_old(mint.to_string(), creator_str.clone());
    let should_emit = match &existing {
        None => true,
        Some(old) if old != &creator_str => {
            warn!(
                mint = %mint,
                pool = %pool_str,
                old_creator = %old,
                new_creator = %creator_str,
                "FIX-22: Creator mismatch on TX fast-path — pool_creator_cache overwrites stale value"
            );
            true
        }
        _ => false,
    };
    if !should_emit {
        return false;
    }
    let publish_at = Instant::now();
    record_market_data_pool_mint_map_to_devwallet_ms(tx_grpc_recv_at, publish_at);
    inc_market_data_devwallet_tx_published_total();
    info!(
        mint = %mint,
        pool = %pool_str,
        creator = %creator_str,
        corrected = existing.is_some(),
        had_override = creator_override.is_some(),
        "DevWalletIdentified from TX path after pool_mint_map (P1 creator)"
    );
    let dev_event = MarketEvent::new(
        "market-data",
        host.tx_build_version(),
        run_id,
        host.tx_next_event_id(),
        "geyser",
        slot,
        MarketEventKind::DevWalletIdentified {
            mint: mint.to_string(),
            dev_wallet: creator_str.clone(),
            supply_percentage: 0.0,
        },
    );
    host.tx_write_market_event_jsonl(&dev_event);
    if publish_tx.is_some() || host.tx_nats().is_some() {
        account_path_enqueue_core_market_event(
            publish_tx,
            nats,
            host.tx_publish_host(),
            dev_event,
            Some(MarketEventCorePublishTrace {
                recv_at: tx_grpc_recv_at,
                cold_path: false,
                segment: MarketDataLatencySegment::Other,
            }),
        )
        .await;
    }
    true
}

#[inline]
pub(crate) fn tx_publish_segment(kind: &MarketEventKind) -> MarketDataLatencySegment {
    match kind {
        MarketEventKind::Trade { .. } => MarketDataLatencySegment::Trade,
        MarketEventKind::BondingCurveProgress { .. } => MarketDataLatencySegment::BondingCurve,
        MarketEventKind::PoolCreated { .. } => MarketDataLatencySegment::PoolCreated,
        _ => MarketDataLatencySegment::Other,
    }
}

/// Whether any known DEX program id appears in the transaction account keys (no RPC).
#[inline]
pub fn tx_involves_known_dex_program(account_keys: &[Pubkey]) -> bool {
    static PROGRAMS: &[&str] = &[
        RAYDIUM_AMM_V4,
        RAYDIUM_CPMM,
        ORCA_WHIRLPOOL,
        METEORA_DLMM,
        PUMPFUN_PROGRAM,
        PUMPFUN_AMM_PROGRAM,
    ];
    PROGRAMS.iter().any(|program_id| {
        Pubkey::from_str(program_id)
            .ok()
            .is_some_and(|pk| account_keys.contains(&pk))
    })
}

#[cfg(test)]
mod tx_dex_detection_tests {
    use super::*;
    use crate::metrics::MarketDataUnparsedTxDropReason;

    fn unparsed_tx_drop_reason(account_keys: &[Pubkey]) -> MarketDataUnparsedTxDropReason {
        if tx_involves_known_dex_program(account_keys) {
            MarketDataUnparsedTxDropReason::DexParseMiss
        } else {
            MarketDataUnparsedTxDropReason::NonDexTransaction
        }
    }

    #[test]
    fn tx_unparsed_drop_reason_non_dex_without_known_program() {
        let keys = vec![Pubkey::new_unique(), Pubkey::new_unique()];
        assert_eq!(
            unparsed_tx_drop_reason(&keys),
            MarketDataUnparsedTxDropReason::NonDexTransaction
        );
    }

    #[test]
    fn tx_unparsed_drop_reason_dex_parse_miss_when_raydium_present() {
        let raydium = Pubkey::from_str(RAYDIUM_AMM_V4).unwrap();
        let keys = vec![Pubkey::new_unique(), raydium];
        assert!(tx_involves_known_dex_program(&keys));
        assert_eq!(
            unparsed_tx_drop_reason(&keys),
            MarketDataUnparsedTxDropReason::DexParseMiss
        );
    }
}
