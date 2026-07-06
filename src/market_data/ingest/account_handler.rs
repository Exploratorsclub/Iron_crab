//! Geyser account ingest handler (dedizierte worker pool, MARKET-DATA-ACCOUNT-THROUGHPUT-P0).
//! STOP-CHECK: keine neuen RPC-Calls.

use super::account_filter::account_geyser_update_is_dex_pool_owner;
use super::account_host::AccountIngestHost;
use super::account_parse::{
    account_publish_segment, try_parse_mint_account, try_parse_token_account_balance,
    wallet_geyser_snapshots_to_publish, wsol_ata_balance_lamports_from_geyser_data,
    WalletGeyserUpdateSource,
};
use crate::ipc::{BinData, MarketEvent, MarketEventKind, NATIVE_SOL_MINT};
use crate::market_data::md_state::MdStateSender;
use crate::market_data::publish::{
    account_path_enqueue_core_market_event, account_path_enqueue_jetstream, AccountPublishSender,
};
use crate::market_data::sidefx::{
    md_sidefx_try_enqueue, MarketEventCorePublishTrace, MdSidefxCommand, MdSidefxSender,
};
use crate::metrics::{
    inc_market_data_unparsed_account_dropped_total, record_market_data_tokio_progress,
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
) {
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
            // When the ATA is closed (unwrap), Geyser often delivers an update with
            // `data` empty (account gone) — not parseable as SPL token state.
            // That means WSOL=0. Previously we `continue`d and kept a stale balance
            // with no zero WalletBalanceSnapshot to JetStream.
            let Some(balance) = wsol_ata_balance_lamports_from_geyser_data(&account_update.data)
            else {
                return;
            };
            let prev = host.account_wallet_wsol_swap(balance);
            host.account_wallet_wsol_seen_set();
            if let Some(snapshot) =
                wallet_geyser_snapshots_to_publish(WalletGeyserUpdateSource::WsolAta {
                    balance,
                    prev_balance: prev,
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

                    info!(
                        wallet = %wallet_str,
                        wsol_lamports = snapshot.balance_raw,
                        slot = account_update.slot,
                        "WalletBalanceSnapshot (WSOL) enqueued for JetStream"
                    );
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
            md_sidefx_try_enqueue(
                md_sidefx,
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
        }
    }

    // Vault account updates → emit PoolStateUpdate (Geyser-based reserve balances)
    // This eliminates the need for RPC calls to fetch vault balances.
    if account_update.owner.to_bytes() == spl_token::ID.to_bytes()
        || account_update.owner.to_bytes() == spl_token_2022::ID.to_bytes()
    {
        // Phase-R-R4: vault pairing + publish off hot path (`md-sidefx`; no `tracked_vaults` read here).
        if let Some(balance) = try_parse_token_account_balance(&account_update.data) {
            md_sidefx_try_enqueue(
                md_sidefx,
                MdSidefxCommand::VaultBalanceTick {
                    run_id: run_id.to_string(),
                    vault_pubkey: account_update.pubkey,
                    balance,
                    slot: account_update.slot,
                    grpc_recv_at: account_update.grpc_recv_at,
                },
            );
        }
    }

    // Bin Array account updates → emit BinArrayUpdate (Geyser-based liquidity distribution)
    // This eliminates the need for RPC calls to fetch Meteora DLMM bin arrays.
    let dlmm_program =
        Pubkey::from_str(METEORA_DLMM_PROGRAM).expect("Invalid METEORA_DLMM_PROGRAM constant");
    if account_update.owner == dlmm_program {
        if let Some(bin_array_info) = host.account_membership_bin_array_info(&account_update.pubkey)
        {
            md_sidefx_try_enqueue(
                md_sidefx,
                MdSidefxCommand::TouchBinArrayTick {
                    pda: account_update.pubkey,
                },
            );
            // Parse bin array to extract liquidity distribution
            match BinArray::parse(&account_update.data, bin_array_info.bin_step) {
                Ok(parsed_array) => {
                    // Convert to compact BinData (only bins with liquidity)
                    let bins: Vec<BinData> = parsed_array
                        .bins
                        .iter()
                        .enumerate()
                        .filter(|(_, bin)| bin.amount_x > 0 || bin.amount_y > 0)
                        .map(|(offset, bin)| BinData {
                            offset: offset as u8,
                            amount_x: bin.amount_x,
                            amount_y: bin.amount_y,
                        })
                        .collect();

                    // Only emit if there's any liquidity
                    if !bins.is_empty() {
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
                        }
                        if host.ingest_is_hot_pool(&bin_array_info.pool_address) {
                            md_sidefx_try_enqueue(
                                md_sidefx,
                                MdSidefxCommand::DlmmPoolStatePublishSignal {
                                    run_id: run_id.to_string(),
                                    pool_address: bin_array_info.pool_address,
                                    slot: account_update.slot,
                                    grpc_recv_at: account_geyser_recv_at,
                                },
                            );
                        }
                    }
                }
                Err(e) => {
                    debug!(
                        error = %e,
                        pubkey = %account_update.pubkey,
                        "Failed to parse bin array account"
                    );
                }
            }
        }
    }

    // Phase-R-R4b: LivePoolCache populate + PoolCacheUpdate off account ingest (`md-sidefx`).
    if account_geyser_update_is_dex_pool_owner(&account_update.owner) {
        md_sidefx_try_enqueue(
            md_sidefx,
            MdSidefxCommand::LivePoolCacheAccountUpdate {
                run_id: run_id.to_string(),
                pool_pubkey: account_update.pubkey,
                owner: account_update.owner,
                account_data: account_update.data.clone(),
                slot: account_update.slot,
                grpc_recv_at: account_geyser_recv_at,
            },
        );
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
        md_sidefx_try_enqueue(
            md_sidefx,
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
        inc_market_data_unparsed_account_dropped_total();
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
