//! Geyser account ingest handler (dedizierte worker pool, MARKET-DATA-ACCOUNT-THROUGHPUT-P0).
//! STOP-CHECK: keine neuen RPC-Calls.

use super::account_filter::{
    account_geyser_update_is_dex_pool_owner, account_geyser_update_is_sidefx_only_pool_owner,
    geyser_account_data_looks_like_meteora_bin_array,
};
use super::account_host::{AccountBinArrayView, AccountIngestHost};
use super::account_parse::{
    account_publish_segment, try_parse_mint_account, try_parse_token_account_balance,
    wallet_geyser_snapshots_to_publish, wsol_ata_balance_lamports_from_geyser_data,
    WalletGeyserUpdateSource,
};
use super::dlmm_bin_publish::filter_dlmm_bins_for_publish;
use crate::ipc::{MarketEvent, MarketEventKind, NATIVE_SOL_MINT};
use crate::market_data::ingest::AccountUpdateClass;
use crate::market_data::md_state::MdStateSender;
use crate::market_data::publish::{
    account_path_enqueue_core_market_event, account_path_enqueue_jetstream, AccountPublishSender,
};
use crate::market_data::sidefx::{
    md_sidefx_try_enqueue_classed, MarketEventCorePublishTrace, MdSidefxCommand, MdSidefxSender,
    SidefxUpdateClass,
};
use crate::metrics::{
    inc_market_data_dlmm_bin_emit_skipped_empty_total,
    inc_market_data_dlmm_bin_membership_hit_total, inc_market_data_dlmm_bin_membership_miss_total,
    inc_market_data_dlmm_bin_owner_update_total, inc_market_data_dlmm_bin_parse_fail_total,
    inc_market_data_dlmm_bin_publish_enrich_total, inc_market_data_dlmm_bin_publish_exec_hot_total,
    inc_market_data_dlmm_bin_replay_publish_total, record_market_data_tokio_progress,
    record_market_data_unparsed_account_dropped, MarketDataUnparsedAccountDropReason,
};
use crate::nats::wallet_snapshot_subject;
use crate::solana::dex::meteora_bin_array_layout::BinArray;
use crate::solana::dex::meteora_dlmm::METEORA_DLMM_PROGRAM;
use crate::solana::dex_parser::{parse_account_update, ParsedDexEvent};
use crate::solana::geyser_listener::GeyserAccountUpdate;
use solana_sdk::pubkey::Pubkey;
use spl_token::solana_program::program_pack::Pack;
use spl_token_2022::extension::StateWithExtensions;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tracing::{debug, info, warn};

/// Outcome of the DLMM bin-array publish gate (forensics + replay).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DlmmBinArrayPublishOutcome {
    Published,
    ParseFailed,
    SkippedEmptyLiquidity,
    SkippedNoNats,
}

/// Parse + publish one Meteora DLMM bin-array Geyser update for a tracked membership row.
#[allow(clippy::too_many_arguments)]
pub async fn publish_meteora_dlmm_bin_array_from_geyser<H: AccountIngestHost>(
    host: &H,
    run_id: &str,
    account_geyser_recv_at: Instant,
    account_update: &GeyserAccountUpdate,
    bin_array_info: AccountBinArrayView,
    publish_tx: Option<&AccountPublishSender>,
    md_sidefx: Option<&MdSidefxSender>,
    sidefx_class: SidefxUpdateClass,
    update_class: AccountUpdateClass,
    replay_from_stash: bool,
) -> DlmmBinArrayPublishOutcome {
    if let Some(md_sidefx) = md_sidefx {
        md_sidefx_try_enqueue_classed(
            md_sidefx,
            sidefx_class,
            MdSidefxCommand::TouchBinArrayTick {
                pda: account_update.pubkey,
                update_class: sidefx_class,
            },
        );
    }
    match BinArray::parse(&account_update.data, bin_array_info.bin_step) {
        Ok(parsed_array) => {
            let active_id = host.ingest_pool_dlmm_active_id(&bin_array_info.pool_address);
            let bins = filter_dlmm_bins_for_publish(
                &parsed_array.bins,
                bin_array_info.bin_array_index,
                active_id,
            );

            if bins.is_empty() {
                inc_market_data_dlmm_bin_emit_skipped_empty_total();
                return DlmmBinArrayPublishOutcome::SkippedEmptyLiquidity;
            }

            let bin_event = MarketEvent::new(
                "market-data",
                host.account_build_version(),
                run_id,
                host.account_next_event_id(),
                "geyser_bin_array",
                Some(account_update.slot),
                MarketEventKind::BinArrayUpdate {
                    pool_address: bin_array_info.pool_address.to_string(),
                    bin_array_index: bin_array_info.bin_array_index,
                    bins,
                    update_slot: account_update.slot,
                },
            );

            host.account_write_market_event_jsonl(&bin_event);

            if host.account_nats().is_some() {
                match update_class {
                    AccountUpdateClass::ExecHot => {
                        inc_market_data_dlmm_bin_publish_exec_hot_total()
                    }
                    AccountUpdateClass::Enrich => inc_market_data_dlmm_bin_publish_enrich_total(),
                    AccountUpdateClass::Drop => {}
                }
                if replay_from_stash {
                    inc_market_data_dlmm_bin_replay_publish_total();
                }
                let seg = account_publish_segment(&bin_event.kind);
                account_path_enqueue_core_market_event(
                    publish_tx,
                    host.account_nats(),
                    host.account_publish_host(),
                    bin_event,
                    Some(MarketEventCorePublishTrace {
                        recv_at: account_geyser_recv_at,
                        cold_path: false,
                        segment: seg,
                    }),
                )
                .await;
            } else {
                return DlmmBinArrayPublishOutcome::SkippedNoNats;
            }

            if host.ingest_is_hot_pool(&bin_array_info.pool_address) {
                if let Some(md_sidefx) = md_sidefx {
                    md_sidefx_try_enqueue_classed(
                        md_sidefx,
                        SidefxUpdateClass::ExecHot,
                        MdSidefxCommand::DlmmPoolStatePublishSignal {
                            run_id: run_id.to_string(),
                            pool_address: bin_array_info.pool_address,
                            slot: account_update.slot,
                            grpc_recv_at: account_geyser_recv_at,
                            update_class: SidefxUpdateClass::ExecHot,
                        },
                    );
                }
            }
            DlmmBinArrayPublishOutcome::Published
        }
        Err(e) => {
            inc_market_data_dlmm_bin_parse_fail_total();
            debug!(
                error = %e,
                pubkey = %account_update.pubkey,
                "Failed to parse bin array account"
            );
            DlmmBinArrayPublishOutcome::ParseFailed
        }
    }
}

/// Geyser account ingest (dedizierte worker pool, siehe MARKET-DATA-ACCOUNT-THROUGHPUT-P0).
/// STOP-CHECK: keine neuen RPC-Calls; gleiche Logik wie zuvor im Bin.
#[allow(clippy::too_many_arguments)]
pub async fn handle_geyser_account_update<H: AccountIngestHost>(
    host: &H,
    run_id: &str,
    account_update: GeyserAccountUpdate,
    account_count: &AtomicU64,
    recv_at: Instant,
    publish_tx: Option<&AccountPublishSender>,
    _md_state: &MdStateSender,
    md_sidefx: &MdSidefxSender,
    update_class: AccountUpdateClass,
) {
    let sidefx_class = SidefxUpdateClass::from(update_class);
    let account_geyser_recv_at = recv_at;
    account_count.fetch_add(1, Ordering::Relaxed);
    record_market_data_tokio_progress();
    crate::metrics::record_activity();

    // === WsolManager Support: Wallet Balance Updates ===
    // Track SOL (native) and WSOL (ATA) balance changes for WsolManager
    if let Some(tracked_wallet) = host.account_tracked_wallet_view() {
        let is_wallet_account = account_update.pubkey == tracked_wallet.wallet;
        let is_wsol_ata = account_update.pubkey == tracked_wallet.wsol_ata;
        let is_token_ata =
            host.ingest_tracked_wallet_token_account_contains(&account_update.pubkey);

        if is_wallet_account {
            // Native SOL account — balance is in lamports field. Publish NATIVE_SOL only.
            let lamports = account_update.lamports;
            let prev = host.account_wallet_native_sol_swap(lamports);
            if let Some(snapshot) =
                wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::NativeSol {
                    lamports,
                    prev_lamports: prev,
                })
            {
                if host.account_nats().is_some() {
                    let wallet_str = tracked_wallet.wallet.to_string();
                    let sol_snapshot = MarketEvent::new(
                        "market-data",
                        host.account_build_version(),
                        run_id,
                        format!("geyser_wallet_sol_{}", account_update.slot),
                        "geyser_wallet_update",
                        Some(account_update.slot),
                        MarketEventKind::WalletBalanceSnapshot {
                            mint: "NATIVE_SOL".to_string(),
                            balance_raw: snapshot.balance_raw,
                            decimals: 9,
                            token_program: "system".to_string(),
                        },
                    );
                    let sol_subject = wallet_snapshot_subject(&wallet_str, "NATIVE_SOL");
                    account_path_enqueue_jetstream(
                        publish_tx,
                        host.account_nats(),
                        sol_subject,
                        &sol_snapshot,
                        "native SOL WalletBalanceSnapshot",
                        false,
                    )
                    .await;

                    info!(
                        wallet = %wallet_str,
                        sol_lamports = snapshot.balance_raw,
                        slot = account_update.slot,
                        "WalletBalanceSnapshot (NATIVE_SOL) enqueued for JetStream"
                    );
                }
            }
        } else if is_wsol_ata {
            // WSOL ATA — parse token account balance. Publish WSOL only.
            // When the ATA is closed (unwrap / external Phantom close), Geyser often delivers
            // an update with `data` empty (account gone) or non-empty but unparseable.
            // Both mean WSOL=0. Must always publish WalletBalanceSnapshot so EE LockManager
            // and WsolManager heal (including MD prev=0 drift after EE wrap callback).
            let parsed = wsol_ata_balance_lamports_from_geyser_data(&account_update.data);
            let closed_or_unparseable = account_update.data.is_empty() || parsed.is_none();
            let balance = parsed.unwrap_or(0);
            let prev = host.account_wallet_wsol_swap(balance);
            host.account_wallet_wsol_seen_set();
            let force_zero_publish = closed_or_unparseable && balance == 0;
            if let Some(snapshot) =
                wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::WsolAta {
                    balance,
                    prev_balance: prev,
                    force_zero_publish,
                })
            {
                if host.account_nats().is_some() {
                    let wallet_str = tracked_wallet.wallet.to_string();
                    let wsol_snapshot = MarketEvent::new(
                        "market-data",
                        host.account_build_version(),
                        run_id,
                        format!("geyser_wallet_wsol_{}", account_update.slot),
                        "geyser_wallet_update",
                        Some(account_update.slot),
                        MarketEventKind::WalletBalanceSnapshot {
                            mint: NATIVE_SOL_MINT.to_string(),
                            balance_raw: snapshot.balance_raw,
                            decimals: 9,
                            token_program: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
                                .to_string(),
                        },
                    );
                    let wsol_subject = wallet_snapshot_subject(&wallet_str, NATIVE_SOL_MINT);
                    account_path_enqueue_jetstream(
                        publish_tx,
                        host.account_nats(),
                        wsol_subject,
                        &wsol_snapshot,
                        "WSOL WalletBalanceSnapshot",
                        false,
                    )
                    .await;

                    if closed_or_unparseable {
                        info!(
                            wallet = %wallet_str,
                            wsol_lamports = snapshot.balance_raw,
                            slot = account_update.slot,
                            reason = "wsol_ata_closed_or_unparseable",
                            prev_wsol_lamports = prev,
                            "WalletBalanceSnapshot (WSOL=0) enqueued after ATA close or unparseable data"
                        );
                    } else {
                        info!(
                            wallet = %wallet_str,
                            wsol_lamports = snapshot.balance_raw,
                            slot = account_update.slot,
                            "WalletBalanceSnapshot (WSOL) enqueued for JetStream"
                        );
                    }
                }
            }
        } else if is_token_ata
            && (account_update.owner.to_bytes() == spl_token::ID.to_bytes()
                || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes())
        {
            let (mint, balance_raw) = if account_update.owner.to_bytes() == spl_token::ID.to_bytes()
            {
                match spl_token::state::Account::unpack(&account_update.data) {
                    Ok(acc) => (Pubkey::new_from_array(acc.mint.to_bytes()), acc.amount),
                    Err(_) => return,
                }
            } else {
                // Token-2022 accounts may have extensions (data > 165 bytes).
                // Use StateWithExtensions instead of Pack::unpack.
                match StateWithExtensions::<spl_token_2022::state::Account>::unpack(
                    &account_update.data,
                ) {
                    Ok(state) => (
                        Pubkey::new_from_array(state.base.mint.to_bytes()),
                        state.base.amount,
                    ),
                    Err(_) => return,
                }
            };

            let decimals = host
                .account_wallet_mint_decimals_get(&mint)
                .unwrap_or_else(|| {
                    // This should only happen for accounts created after initial scan.
                    // Log a warning so we can track if this becomes a problem.
                    warn!(
                        mint = %mint,
                        account = %account_update.pubkey,
                        "Decimals not cached for token account, using default 6"
                    );
                    6
                });

            let mint_str = mint.to_string();
            let event = MarketEvent::new(
                "market-data",
                host.account_build_version(),
                run_id,
                host.account_next_event_id(),
                "geyser_wallet_update",
                Some(account_update.slot),
                MarketEventKind::WalletBalanceSnapshot {
                    mint: mint_str.clone(),
                    balance_raw,
                    decimals,
                    token_program: account_update.owner.to_string(),
                },
            );

            if host.account_nats().is_some() {
                let subject =
                    wallet_snapshot_subject(&tracked_wallet.wallet.to_string(), &mint_str);
                account_path_enqueue_jetstream(
                    publish_tx,
                    host.account_nats(),
                    subject,
                    &event,
                    "WalletBalanceSnapshot JetStream",
                    true,
                )
                .await;
            }

            host.account_write_market_event_jsonl(&event);
        }
    }

    // Tracked mint updates (Token mint authority/freeze info)
    if (account_update.owner.to_bytes() == spl_token::ID.to_bytes()
        || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes())
        && host.account_membership_mint_contains(&account_update.pubkey)
    {
        if let Some((decimals, supply, mint_authority, freeze_authority)) =
            try_parse_mint_account(&account_update.owner, &account_update.data)
        {
            // Decimals-Policy: TokenMintInfo from Geyser is authoritative.
            // Keep wallet decimals cache warm so WalletBalanceSnapshot never needs the 6-decimal fallback.
            host.account_wallet_mint_decimals_insert(account_update.pubkey, decimals);

            // Phase-R-R4b: MASTER mint_decimals off account ingest (`md-sidefx`).
            md_sidefx_try_enqueue_classed(
                md_sidefx,
                SidefxUpdateClass::ExecHot,
                MdSidefxCommand::LivePoolCacheMintDecimals {
                    mint: account_update.pubkey,
                    decimals,
                },
            );

            let is_token_2022 = account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes();

            let mint_event = MarketEvent::new(
                "market-data",
                host.account_build_version(),
                run_id,
                host.account_next_event_id(),
                "geyser",
                Some(account_update.slot),
                MarketEventKind::TokenMintInfo {
                    mint: account_update.pubkey.to_string(),
                    token_program: account_update.owner.to_string(),
                    decimals,
                    supply,
                    mint_authority,
                    freeze_authority,
                },
            );

            // Log Token-2022 mints explicitly (debugging)
            if is_token_2022 {
                info!(
                    mint = %account_update.pubkey,
                    token_program = %account_update.owner,
                    decimals,
                    supply,
                    "TokenMintInfo: Token-2022 mint detected via Geyser"
                );
            } else {
                debug!(
                    mint = %account_update.pubkey,
                    token_program = %account_update.owner,
                    decimals,
                    "TokenMintInfo: SPL Token mint via Geyser"
                );
            }

            host.account_write_market_event_jsonl(&mint_event);

            if host.account_nats().is_some() {
                let seg = account_publish_segment(&mint_event.kind);
                account_path_enqueue_core_market_event(
                    publish_tx,
                    host.account_nats(),
                    host.account_publish_host(),
                    mint_event,
                    Some(MarketEventCorePublishTrace {
                        recv_at: account_geyser_recv_at,
                        cold_path: false,
                        segment: seg,
                    }),
                )
                .await;
            }
            return;
        }
    }

    // Vault account updates → emit PoolStateUpdate (Geyser-based reserve balances)
    // This eliminates the need for RPC calls to fetch vault balances.
    if account_update.owner.to_bytes() == spl_token::ID.to_bytes()
        || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes()
    {
        // Phase-R-R4: vault pairing + publish off hot path (`md-sidefx`; no `tracked_vaults` read here).
        if let Some(balance) = try_parse_token_account_balance(&account_update.data) {
            md_sidefx_try_enqueue_classed(
                md_sidefx,
                sidefx_class,
                MdSidefxCommand::VaultBalanceTick {
                    run_id: run_id.to_string(),
                    vault_pubkey: account_update.pubkey,
                    balance,
                    slot: account_update.slot,
                    grpc_recv_at: account_update.grpc_recv_at,
                    update_class: sidefx_class,
                },
            );
            return;
        }
    }

    // Bin Array account updates → emit BinArrayUpdate (Geyser-based liquidity distribution)
    // This eliminates the need for RPC calls to fetch Meteora DLMM bin arrays.
    let dlmm_program =
        Pubkey::from_str(METEORA_DLMM_PROGRAM).expect("Invalid METEORA_DLMM_PROGRAM constant");
    if account_update.owner == dlmm_program {
        if geyser_account_data_looks_like_meteora_bin_array(
            &account_update.owner,
            &account_update.data,
        ) {
            inc_market_data_dlmm_bin_owner_update_total();
        }
        if let Some(bin_array_info) = host.account_membership_bin_array_info(&account_update.pubkey)
        {
            inc_market_data_dlmm_bin_membership_hit_total();
            publish_meteora_dlmm_bin_array_from_geyser(
                host,
                run_id,
                account_geyser_recv_at,
                &account_update,
                bin_array_info,
                publish_tx,
                Some(md_sidefx),
                sidefx_class,
                update_class,
                false,
            )
            .await;
            return;
        }
        if geyser_account_data_looks_like_meteora_bin_array(
            &account_update.owner,
            &account_update.data,
        ) {
            inc_market_data_dlmm_bin_membership_miss_total();
        }
    }

    // Phase-R-R4b: LivePoolCache populate + PoolCacheUpdate off account ingest (`md-sidefx`).
    if account_geyser_update_is_dex_pool_owner(&account_update.owner) {
        md_sidefx_try_enqueue_classed(
            md_sidefx,
            sidefx_class,
            MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: run_id.to_string(),
                pool_pubkey: account_update.pubkey,
                owner: account_update.owner,
                account_data: account_update.data.clone(),
                slot: account_update.slot,
                grpc_recv_at: account_geyser_recv_at,
                update_class: sidefx_class,
            },
        );
        if account_geyser_update_is_sidefx_only_pool_owner(&account_update.owner) {
            return;
        }
    }

    // Try to parse as DEX pool event (for MarketEvents - existing logic)
    let parsed = parse_account_update(&account_update);

    // Special handling for PumpFun BondingCurveUpdate: cache creator
    if let Some(ParsedDexEvent::BondingCurveUpdate {
        pool_address,
        creator,
        virtual_token_reserves,
        virtual_sol_reserves,
        real_token_reserves,
        real_sol_reserves,
        complete,
        cashback_enabled,
        slot,
    }) = &parsed
    {
        md_sidefx_try_enqueue_classed(
            md_sidefx,
            sidefx_class,
            MdSidefxCommand::BondingCurveDevWallet {
                run_id: run_id.to_string(),
                pool_address: *pool_address,
                creator: *creator,
                slot: *slot,
                grpc_recv_at: account_update.grpc_recv_at,
                virtual_token_reserves: *virtual_token_reserves,
                virtual_sol_reserves: *virtual_sol_reserves,
                real_token_reserves: *real_token_reserves,
                real_sol_reserves: *real_sol_reserves,
                complete: *complete,
                cashback_enabled: *cashback_enabled,
            },
        );
        // Don't emit the BondingCurveUpdate as a MarketEvent - it's internal
        return;
    }

    let Some(parsed) = parsed else {
        record_market_data_unparsed_account_dropped(
            MarketDataUnparsedAccountDropReason::LegacyDexParseMiss,
        );
        return;
    };

    debug!(slot = account_update.slot, "Parsed DEX account update");
    let event_kind = parsed.to_market_event_kind();

    let event = MarketEvent::new(
        "market-data",
        host.account_build_version(),
        run_id,
        host.account_next_event_id(),
        "geyser",
        Some(account_update.slot),
        event_kind,
    );

    // Write to JSONL
    host.account_write_market_event_jsonl(&event);

    // Publish to NATS
    if host.account_nats().is_some() {
        let seg = account_publish_segment(&event.kind);
        account_path_enqueue_core_market_event(
            publish_tx,
            host.account_nats(),
            host.account_publish_host(),
            event,
            Some(MarketEventCorePublishTrace {
                recv_at: account_geyser_recv_at,
                cold_path: false,
                segment: seg,
            }),
        )
        .await;
    }
}
