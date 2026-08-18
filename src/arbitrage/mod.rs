//! Multi-Hop Arbitrage Module
//!
//! Implements the Best-First Beam Search algorithm with Branch-and-Bound
//! for cycle detection in the pool graph.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────┐     ┌─────────────┐     ┌─────────────────┐
//! │  PoolGraph  │────▶│ PoolRanker  │────▶│ BeamCycleFinder │
//! │ (adjacency) │     │(probe quotes)│     │ (search algo)   │
//! └─────────────┘     └─────────────┘     └─────────────────┘
//!                                                  │
//!                                                  ▼
//!                                          ┌─────────────┐
//!                                          │  ArbCycle   │
//!                                          │ (with alts) │
//!                                          └─────────────┘
//! ```
//!
//! # Key Design Decisions
//!
//! 1. **Probe-based edge ratios** - NOT spot prices! Accounts for fees & curve shape.
//! 2. **Top-K beam limit** - Proper score-based selection, not First-K.
//! 3. **Dampened liquidity** - `clamp(liq/baseline, 0.3, 1.5)` instead of `sqrt()`.
//! 4. **Pool alternatives** - Top-3 pools per hop for execution fallback.
//!
//! See `docs/MULTI_HOP_ARBITRAGE.md` for full algorithm design.

pub mod arb_slave_sync;
pub mod cycle_finder;
pub mod in_flight;
pub mod multi_hop_integration;
pub mod pool_graph;
pub mod pool_quote;
pub mod pool_ranker;
pub mod track_selection;
pub mod types;

// Re-exports for convenient access
pub use arb_slave_sync::{
    arb_known_pools_synced_bootstrap_total, arb_known_pools_synced_incremental_total,
    populate_arb_slave_from_live_pool_cache, sync_arb_slave_from_pool_cache_update,
};
pub use cycle_finder::{BeamCycleFinder, CycleFinderConfig};
pub use multi_hop_integration::{
    CachedQuoteProvider, MultiHopArbitrage, MultiHopConfig, MultiHopIntentBatch, MultiHopStats,
    QuoteReadyIndex, WSOL_MINT,
};
pub use pool_graph::{GraphStats, PoolGraph};
pub use pool_quote::{
    classify_cross_dex_sell_failure, diagnose_no_fresh_buy_quote, diagnose_quote_not_fresh,
    diagnose_sell_quote_none, dlmm_marginal_price_plausible, dlmm_sol_output_from_bins,
    dlmm_token_output_from_bins, flatten_bin_array_bins, freshness_age_bucket,
    is_arb_route_executable, is_expected_token_output_plausible, is_quote_fresh,
    is_quote_fresh_with_bins, is_usable_quote_kind, price_based_token_output_raw, quote_exact_in,
    quote_exact_in_with_freshness, quote_from_cached_pool, quote_sell_round_trip,
    quote_sol_per_token_for_screening, quotes_pairable, round_trip_profit_lamports,
    round_trip_profit_lamports_with_freshness, select_round_trip_pools,
    sol_quoted_seed_from_cached_state, state_fingerprint, token_decimals_from_cached_state,
    vault_state_age_ms, CrossDexSellFailure, DlmmBinArrays, FreshnessAgeBucket,
    NoCrossDexSellDetailReason, NoFreshBuyQuoteSubreason, PoolQuote, QuoteFreshnessConfig,
    QuoteKind, QuoteNotFreshCause, QuoteNotFreshDiagnosis, QuoteNotFreshKind, QuotePoolInput,
    QuoteSide, QuoteVaultInput, RoundTripInsufficient, RoundTripInsufficientSubreason,
    RoundTripLeg, RoundTripPoolCandidate, RoundTripPoolSelection, RoundTripSelectFailure,
    SellQuoteNoneDetailReason, SolQuotedPoolSeed, ARB_TOKEN_OUT_FLOOR_TRADE_AMOUNT_LAMPORTS,
    ARB_TOKEN_OUT_MIN_PRICE_FRACTION_BPS, ARB_TOKEN_OUT_MIN_RAW_FLOOR, DLMM_PROBE_SOL_LAMPORTS,
    STATE_TTL_MS, TRADE_TTL_MS,
};
pub use pool_ranker::{PoolRanker, QuoteProvider, RankerConfig};
pub use track_selection::{
    arb_track_removal_reason, select_arb_track_pools, SelectedTrackPool, TrackCandidateCounts,
    TrackMintInput, TrackPoolInput, TrackPoolReadiness, TrackSelectionConfig, TrackSelectionResult,
};
pub use types::{
    clamp_edge_ratio, profit_to_return_bps, ArbCycle, DexType, ParseDexTypeError, PoolEdge,
    RankedPool, SearchNode, MAX_CYCLE_PROFIT_MULTIPLIER, MAX_EDGE_RATIO, MAX_RETURN_BPS,
    MIN_EDGE_RATIO, MIN_RETURN_BPS,
};

#[cfg(any(test, feature = "test_helpers"))]
pub use pool_ranker::MockQuoteProvider;
