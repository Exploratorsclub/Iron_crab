//! Cross-DEX Cold-Path Reserve-Fallback (Raydium, RaydiumCpmm, MeteoraDlmm)
//!
//! Invariante: Bekannter Pool + fehlende Live-Reserves im Cold Path =
//! autoritativer RPC-Fallback oder klarer Failure.
//!
//! Wenn fuer einen bereits bekannten Pool die fuer Quote/Ausfuehrung benoetigten
//! Reserve-/Vault-Daten im LivePoolCache fehlen, darf der Cold Path den Fall
//! nicht still wie einen harmlosen Cache-Miss behandeln. Er muss entweder den
//! autoritativen Reserve-State per RPC nachladen oder einen klaren Fehler liefern.
//! Nicht erlaubt: stilles Ok(None) oder verdeckter lokaler Ersatz-Truth.
//!
//! Scope: Nur Raydium, RaydiumCpmm, MeteoraDlmm. Orca gehoert nicht dazu.
//! Hot Path bleibt GEYSER-ONLY (A.12); dieser Test prueft die Cold-Path-Gegenseite.

use ironcrab::solana::dex::meteora_dlmm::MeteoraDlmm;
use ironcrab::solana::dex::raydium::Raydium;
use ironcrab::solana::dex::raydium_cpmm::RaydiumCpmm;
use ironcrab::solana::dex::Dex;
use ironcrab::solana::rpc::SolanaRpc;
use solana_sdk::pubkey::Pubkey;
use std::sync::Arc;

const DUMMY_RPC: &str = "http://127.0.0.1:0";

/// Raydium AMM: Bekannter Pool, fehlende Vault-Reserves, Cold Path (allow_rpc=true),
/// RPC unreachable → klarer Fehler (Err), NICHT stilles Ok(None).
#[tokio::test]
async fn raydium_cold_path_known_pool_missing_reserves_rpc_unreachable_yields_err() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let raydium = Raydium::new_with_live_cache(rpc, None, true);

    let pool_addr = Pubkey::new_unique();
    let base_mint = Pubkey::new_unique();
    let quote_mint = Pubkey::new_unique();
    let base_vault = Pubkey::new_unique();
    let quote_vault = Pubkey::new_unique();
    let market_id = Pubkey::new_unique();

    raydium.inject_cached_amm_state(
        pool_addr,
        base_mint,
        quote_mint,
        base_vault,
        quote_vault,
        9,
        6,
        None,
        None,
        market_id,
        None,
        None,
        None,
        None,
        None,
    );

    let result = raydium.fetch_and_update_reserves(&pool_addr).await;

    assert!(
        result.is_err(),
        "Cold Path: bekannter Pool + fehlende Reserves + RPC unreachable muss Err liefern, nicht stilles Ok"
    );
}

/// Raydium CPMM: Bekannter Pool, fehlende Vault-Reserves, Cold Path (allow_rpc=true),
/// RPC unreachable → klarer Fehler (Err), NICHT stilles Ok(None).
#[tokio::test]
async fn raydium_cpmm_cold_path_known_pool_missing_reserves_rpc_unreachable_yields_err() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let cpmm = RaydiumCpmm::new_with_live_cache(rpc, None, true);

    let pool_addr = Pubkey::new_unique();
    let token_0 = Pubkey::new_unique();
    let token_1 = Pubkey::new_unique();
    let vault_0 = Pubkey::new_unique();
    let vault_1 = Pubkey::new_unique();

    cpmm.set_pool_from_accounts(
        &pool_addr.to_string(),
        &[
            pool_addr.to_string(),
            token_0.to_string(),
            token_1.to_string(),
            vault_0.to_string(),
            vault_1.to_string(),
        ],
    )
    .expect("set_pool_from_accounts");

    let result = cpmm
        .quote_exact_in(&token_0.to_string(), &token_1.to_string(), 1_000_000)
        .await;

    assert!(
        result.is_err(),
        "Cold Path: bekannter Pool + fehlende Reserves + RPC unreachable muss Err liefern, nicht Ok(None)"
    );
}

/// Meteora DLMM: Bekannter Pool, fehlende Reserve-Daten, Cold Path (allow_rpc=true),
/// RPC unreachable → klarer Fehler (Err), NICHT stilles Ok(None).
#[tokio::test]
async fn meteora_dlmm_cold_path_known_pool_missing_reserves_rpc_unreachable_yields_err() {
    let rpc = Arc::new(SolanaRpc::new(DUMMY_RPC));
    let meteora = MeteoraDlmm::new_with_live_cache(rpc, None, true);

    let pool_addr = Pubkey::new_unique();
    let token_x = Pubkey::new_unique();
    let token_y = Pubkey::new_unique();
    let reserve_x = Pubkey::new_unique();
    let reserve_y = Pubkey::new_unique();

    meteora
        .set_pool_from_accounts(
            &pool_addr.to_string(),
            &[
                pool_addr.to_string(),
                token_x.to_string(),
                token_y.to_string(),
                reserve_x.to_string(),
                reserve_y.to_string(),
            ],
        )
        .expect("set_pool_from_accounts");

    let result = meteora
        .quote_exact_in(&token_x.to_string(), &token_y.to_string(), 1_000_000)
        .await;

    assert!(
        result.is_err(),
        "Cold Path: bekannter Pool + fehlende Reserves + RPC unreachable muss Err liefern, nicht Ok(None)"
    );
}
