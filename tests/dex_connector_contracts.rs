//! DEX Connector Contract Tests (DoD H) P0)
//!
//! Each DEX connector must satisfy these invariants:
//! 1. Quote monotonicity: larger input => larger output (for same direction)
//! 2. Quote consistency: quote_exact_in(A, B, x) + quote_exact_in(B, A, y) round-trips
//! 3. Swap instruction validity: generated instructions are valid (account count, program ID)
//!
//! These tests use mock/simulated pools to verify connector logic without RPC.

use ironcrab::backtest::market::{CfmAdapter, CfmPool, MarketAdapter};

// ============================================================================
// Contract 1: Quote Monotonicity
// For any DEX adapter: larger input should produce larger output
// ============================================================================

#[test]
fn contract_cfm_quote_monotonic() {
    let adapter = CfmAdapter {
        pools: vec![CfmPool {
            pool: "test-pool".into(),
            base_mint: "SOL".into(),
            quote_mint: "USDC".into(),
            base_reserve: 1_000_000_000,    // 1000 SOL
            quote_reserve: 150_000_000_000, // 150k USDC
            fee_bps: 30,                    // 0.3%
            tick_spacing: None,
        }],
    };

    let inputs = [1_000_000u64, 10_000_000, 100_000_000, 500_000_000];
    let mut prev_output = 0u64;

    for input in inputs {
        let quote = adapter
            .quote("SOL", "USDC", input)
            .expect("quote should succeed");
        assert!(
            quote.amount_out > prev_output,
            "Quote not monotonic: input {} gave {} (prev {})",
            input,
            quote.amount_out,
            prev_output
        );
        prev_output = quote.amount_out;
    }
}

// ============================================================================
// Contract 2: Price Impact Non-Decreasing
// Larger trades should have >= price impact (never decreasing)
// ============================================================================

#[test]
fn contract_cfm_price_impact_non_decreasing() {
    let adapter = CfmAdapter {
        pools: vec![CfmPool {
            pool: "test-pool".into(),
            base_mint: "SOL".into(),
            quote_mint: "USDC".into(),
            base_reserve: 1_000_000_000,
            quote_reserve: 150_000_000_000,
            fee_bps: 30,
            tick_spacing: None,
        }],
    };

    let inputs = [1_000_000u64, 10_000_000, 100_000_000, 500_000_000];
    let mut prev_impact = 0u32;

    for input in inputs {
        let quote = adapter
            .quote("SOL", "USDC", input)
            .expect("quote should succeed");
        assert!(
            quote.price_impact_bps >= prev_impact,
            "Price impact decreased: input {} gave {}bps (prev {}bps)",
            input,
            quote.price_impact_bps,
            prev_impact
        );
        prev_impact = quote.price_impact_bps;
    }
}

// ============================================================================
// Contract 3: Quote Returns None for Unknown Pairs
// ============================================================================

#[test]
fn contract_cfm_unknown_pair_returns_none() {
    let adapter = CfmAdapter {
        pools: vec![CfmPool {
            pool: "test-pool".into(),
            base_mint: "SOL".into(),
            quote_mint: "USDC".into(),
            base_reserve: 1_000_000_000,
            quote_reserve: 150_000_000_000,
            fee_bps: 30,
            tick_spacing: None,
        }],
    };

    // Unknown mints should return None
    let result = adapter.quote("UNKNOWN", "USDC", 1_000_000);
    assert!(
        result.is_none(),
        "Quote for unknown input mint should be None"
    );

    let result = adapter.quote("SOL", "UNKNOWN", 1_000_000);
    assert!(
        result.is_none(),
        "Quote for unknown output mint should be None"
    );
}

// ============================================================================
// Contract 4: Zero Input Returns Zero Output
// ============================================================================

#[test]
fn contract_cfm_zero_input() {
    let adapter = CfmAdapter {
        pools: vec![CfmPool {
            pool: "test-pool".into(),
            base_mint: "SOL".into(),
            quote_mint: "USDC".into(),
            base_reserve: 1_000_000_000,
            quote_reserve: 150_000_000_000,
            fee_bps: 30,
            tick_spacing: None,
        }],
    };

    let quote = adapter.quote("SOL", "USDC", 0);
    // Zero input should either return None or zero output
    if let Some(q) = quote {
        assert_eq!(q.amount_out, 0, "Zero input should give zero output");
    }
}

// ============================================================================
// Live Connector Contract Tests (require RPC, gated by feature)
// ============================================================================

#[cfg(feature = "live_tests")]
mod live_connector_tests {
    use ironcrab::solana::dex::{orca::OrcaDex, pumpfun::PumpFunDex, raydium::RaydiumDex, Dex};
    use ironcrab::solana::rpc::SolanaRpc;
    use std::sync::Arc;

    // Known good pool addresses for testing
    const SOL_USDC_RAYDIUM: &str = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2";
    const SOL_MINT: &str = "So11111111111111111111111111111111111111112";
    const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";

    #[tokio::test]
    async fn contract_raydium_quote_monotonic() {
        let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com").unwrap());
        let dex = RaydiumDex::new(rpc).unwrap();

        // Refresh to load pools
        dex.refresh_pools().await.unwrap();

        let inputs = [1_000_000u64, 10_000_000, 100_000_000];
        let mut prev_output = 0u64;

        for input in inputs {
            if let Some(quote) = dex
                .quote_exact_in(SOL_MINT, USDC_MINT, input)
                .await
                .unwrap()
            {
                assert!(
                    quote.amount_out > prev_output,
                    "Raydium quote not monotonic"
                );
                prev_output = quote.amount_out;
            }
        }
    }

    #[tokio::test]
    async fn contract_pumpfun_quote_requires_bonding_curve() {
        let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com").unwrap());
        let dex = PumpFunDex::new(rpc).unwrap();

        // PumpFun only works with its bonding curve tokens
        // Unknown tokens should return None
        let result = dex
            .quote_exact_in(SOL_MINT, USDC_MINT, 1_000_000)
            .await
            .unwrap();
        assert!(
            result.is_none(),
            "PumpFun should not quote non-bonding-curve tokens"
        );
    }
}
