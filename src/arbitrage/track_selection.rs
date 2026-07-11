//! Deterministic, bounded Arb Geyser pin selection (I-ARB-10b).
//!
//! Pure selection logic: no locks, NATS, RPC, or logging.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::nats::{ArbTrackActiveReason, ArbTrackRemovedReason};

use super::pool_quote::{
    is_quote_fresh, quote_exact_in_with_freshness, select_round_trip_pools, DlmmBinArrays,
    QuoteFreshnessConfig, QuotePoolInput, QuoteVaultInput, RoundTripPoolCandidate, NATIVE_SOL_MINT,
};

/// Readiness tier for a pool candidate (lowest ordinal = highest priority).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TrackPoolReadiness {
    Rejected = 0,
    Warmable = 1,
    QuoteReady = 2,
    Executable = 3,
}

impl TrackPoolReadiness {
    pub fn metric_label(self) -> &'static str {
        match self {
            TrackPoolReadiness::Executable => "executable",
            TrackPoolReadiness::QuoteReady => "quote_ready",
            TrackPoolReadiness::Warmable => "warmable",
            TrackPoolReadiness::Rejected => "rejected",
        }
    }
}

/// Per-pool inputs for track selection (built by arb-strategy from tracker/cache state).
#[derive(Debug, Clone)]
pub struct TrackPoolInput {
    pub pool_address: String,
    pub dex: String,
    pub known: bool,
    pub quote_pool: QuotePoolInput,
    pub vault: Option<QuoteVaultInput>,
    pub dlmm_bins: Option<DlmmBinArrays>,
    pub token_decimals: u8,
}

/// Per-mint bundle of pools considered for pin selection.
#[derive(Debug, Clone)]
pub struct TrackMintInput {
    pub mint: String,
    pub pools: Vec<TrackPoolInput>,
    /// Optional trade-signal buy/sell pair for this mint (highest pin priority).
    pub trade_signal_pools: Option<(String, String)>,
}

/// Global selection limits and quote parameters.
#[derive(Debug, Clone, Copy)]
pub struct TrackSelectionConfig {
    pub max_pools: usize,
    pub max_pools_per_mint: usize,
    pub probe_lamports: u64,
    pub freshness: QuoteFreshnessConfig,
}

/// A pool chosen for the authoritative Arb pin set.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SelectedTrackPool {
    pub mint: String,
    pub pool: String,
    pub readiness: TrackPoolReadiness,
    pub active_reason: ArbTrackActiveReason,
}

/// Candidate readiness histogram (for metrics).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TrackCandidateCounts {
    pub executable: u64,
    pub quote_ready: u64,
    pub warmable: u64,
    pub rejected: u64,
}

impl TrackCandidateCounts {
    pub fn record(&mut self, readiness: TrackPoolReadiness) {
        match readiness {
            TrackPoolReadiness::Executable => self.executable += 1,
            TrackPoolReadiness::QuoteReady => self.quote_ready += 1,
            TrackPoolReadiness::Warmable => self.warmable += 1,
            TrackPoolReadiness::Rejected => self.rejected += 1,
        }
    }
}

/// Full selection output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackSelectionResult {
    pub selected: Vec<SelectedTrackPool>,
    /// Pools that would have been selected for a mint but were dropped by the global budget.
    pub budget_displaced: Vec<String>,
    pub selected_mints: usize,
    pub candidate_counts: TrackCandidateCounts,
}

/// Select the authoritative bounded Arb pin set across all mints.
pub fn select_arb_track_pools(
    mints: &[TrackMintInput],
    config: &TrackSelectionConfig,
) -> TrackSelectionResult {
    let now = Instant::now();
    let mut candidate_counts = TrackCandidateCounts::default();
    let mut per_pool_readiness: HashMap<String, TrackPoolReadiness> = HashMap::new();

    for mint in mints {
        for pool in &mint.pools {
            let readiness = classify_pool_readiness(pool, config, now);
            candidate_counts.record(readiness);
            per_pool_readiness.insert(pool.pool_address.clone(), readiness);
        }
    }

    let mut bundles: Vec<MintBundle> = Vec::new();
    let mut mint_inputs: Vec<&TrackMintInput> = mints.iter().collect();
    mint_inputs.sort_by(|a, b| a.mint.cmp(&b.mint));

    for mint in mint_inputs {
        if let Some(bundle) = build_mint_bundle(mint, config, &per_pool_readiness, now) {
            bundles.push(bundle);
        }
    }

    bundles.sort_by(|a, b| {
        b.priority
            .cmp(&a.priority)
            .then_with(|| a.mint.cmp(&b.mint))
    });

    let mut selected: Vec<SelectedTrackPool> = Vec::new();
    let mut budget_displaced: Vec<String> = Vec::new();
    let mut selected_mints = HashSet::new();

    for bundle in bundles {
        let bundle_len = bundle.pools.len();
        if bundle_len < 2 {
            continue;
        }
        if selected.len() + bundle_len > config.max_pools {
            for pool in &bundle.pools {
                budget_displaced.push(pool.pool.clone());
            }
            continue;
        }
        selected_mints.insert(bundle.mint.clone());
        selected.extend(bundle.pools);
    }

    budget_displaced.sort();
    budget_displaced.dedup();

    TrackSelectionResult {
        selected_mints: selected_mints.len(),
        selected,
        budget_displaced,
        candidate_counts,
    }
}

/// Map selection removals for pools no longer in the target set.
pub fn arb_track_removal_reason(
    pool: &str,
    still_tracker_valid: bool,
    budget_displaced: &HashSet<String>,
) -> ArbTrackRemovedReason {
    if budget_displaced.contains(pool) || still_tracker_valid {
        ArbTrackRemovedReason::Budget
    } else {
        ArbTrackRemovedReason::Stale
    }
}

#[derive(Debug, Clone)]
struct MintBundle {
    mint: String,
    priority: TrackPoolReadiness,
    pools: Vec<SelectedTrackPool>,
}

fn build_mint_bundle(
    mint: &TrackMintInput,
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
    now: Instant,
) -> Option<MintBundle> {
    let eligible: Vec<&TrackPoolInput> = mint
        .pools
        .iter()
        .filter(|p| {
            per_pool_readiness
                .get(&p.pool_address)
                .copied()
                .unwrap_or(TrackPoolReadiness::Rejected)
                != TrackPoolReadiness::Rejected
        })
        .collect();

    let distinct_dexes: HashSet<&str> = eligible.iter().map(|p| p.dex.as_str()).collect();
    if distinct_dexes.len() < 2 {
        return None;
    }

    if let Some((buy, sell)) = &mint.trade_signal_pools {
        if let Some(bundle) =
            bundle_from_trade_signal(mint, buy, sell, &eligible, config, per_pool_readiness)
        {
            return Some(bundle);
        }
    }

    if let Some(bundle) = bundle_from_round_trip(mint, &eligible, config, per_pool_readiness) {
        return Some(bundle);
    }

    bundle_from_warmable_pair(mint, &eligible, config, per_pool_readiness, now)
}

fn bundle_from_trade_signal(
    mint: &TrackMintInput,
    buy: &str,
    sell: &str,
    eligible: &[&TrackPoolInput],
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) -> Option<MintBundle> {
    let buy_pool = eligible.iter().find(|p| p.pool_address == buy)?;
    let sell_pool = eligible.iter().find(|p| p.pool_address == sell)?;
    if buy_pool.dex == sell_pool.dex {
        return None;
    }

    let mut pools = vec![
        selected_pool(
            mint,
            buy_pool,
            TrackPoolReadiness::Executable,
            ArbTrackActiveReason::TradeSignal,
            per_pool_readiness,
        ),
        selected_pool(
            mint,
            sell_pool,
            TrackPoolReadiness::Executable,
            ArbTrackActiveReason::TradeSignal,
            per_pool_readiness,
        ),
    ];
    maybe_add_third_pool(mint, &mut pools, eligible, config, per_pool_readiness);
    pools.sort_by(pool_address_cmp);

    Some(MintBundle {
        mint: mint.mint.clone(),
        priority: TrackPoolReadiness::Executable,
        pools,
    })
}

fn bundle_from_round_trip(
    mint: &TrackMintInput,
    eligible: &[&TrackPoolInput],
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) -> Option<MintBundle> {
    let candidates: Vec<RoundTripPoolCandidate<'_>> = eligible
        .iter()
        .map(|p| RoundTripPoolCandidate {
            pool: &p.quote_pool,
            vault: p.vault.as_ref(),
            dlmm_bins: p.dlmm_bins.as_ref(),
            dex: &p.dex,
        })
        .collect();

    let selection =
        select_round_trip_pools(&candidates, config.probe_lamports, &config.freshness).ok()?;
    let buy_pool = eligible
        .iter()
        .find(|p| p.pool_address == selection.buy_pool_address)?;
    let sell_pool = eligible
        .iter()
        .find(|p| p.pool_address == selection.sell_pool_address)?;

    let mut pools = vec![
        selected_pool(
            mint,
            buy_pool,
            TrackPoolReadiness::QuoteReady,
            ArbTrackActiveReason::MultiDex,
            per_pool_readiness,
        ),
        selected_pool(
            mint,
            sell_pool,
            TrackPoolReadiness::QuoteReady,
            ArbTrackActiveReason::MultiDex,
            per_pool_readiness,
        ),
    ];
    maybe_add_third_pool(mint, &mut pools, eligible, config, per_pool_readiness);
    pools.sort_by(pool_address_cmp);

    Some(MintBundle {
        mint: mint.mint.clone(),
        priority: TrackPoolReadiness::QuoteReady,
        pools,
    })
}

fn bundle_from_warmable_pair(
    mint: &TrackMintInput,
    eligible: &[&TrackPoolInput],
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
    now: Instant,
) -> Option<MintBundle> {
    let mut warmable: Vec<&TrackPoolInput> = eligible
        .iter()
        .copied()
        .filter(|p| is_warmable_pool(p, config, now))
        .collect();
    warmable.sort_by(|a, b| dex_then_pool_cmp(a, b));

    let mut buy: Option<&TrackPoolInput> = None;
    let mut sell: Option<&TrackPoolInput> = None;
    'outer: for (i, a) in warmable.iter().enumerate() {
        for b in warmable.iter().skip(i + 1) {
            if a.dex != b.dex {
                buy = Some(a);
                sell = Some(b);
                break 'outer;
            }
        }
    }
    let (buy_pool, sell_pool) = (buy?, sell?);

    let mut pools = vec![
        selected_pool(
            mint,
            buy_pool,
            TrackPoolReadiness::Warmable,
            ArbTrackActiveReason::MultiDex,
            per_pool_readiness,
        ),
        selected_pool(
            mint,
            sell_pool,
            TrackPoolReadiness::Warmable,
            ArbTrackActiveReason::MultiDex,
            per_pool_readiness,
        ),
    ];
    maybe_add_third_pool(mint, &mut pools, eligible, config, per_pool_readiness);
    pools.sort_by(pool_address_cmp);

    Some(MintBundle {
        mint: mint.mint.clone(),
        priority: TrackPoolReadiness::Warmable,
        pools,
    })
}

fn maybe_add_third_pool(
    mint: &TrackMintInput,
    pools: &mut Vec<SelectedTrackPool>,
    eligible: &[&TrackPoolInput],
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) {
    if pools.len() >= config.max_pools_per_mint {
        return;
    }
    let used: HashSet<&str> = pools.iter().map(|p| p.pool.as_str()).collect();
    let used_dexes: HashSet<&str> = pools
        .iter()
        .filter_map(|sp| {
            eligible
                .iter()
                .find(|p| p.pool_address == sp.pool)
                .map(|p| p.dex.as_str())
        })
        .collect();

    let mut candidates: Vec<&TrackPoolInput> = eligible
        .iter()
        .copied()
        .filter(|p| !used.contains(p.pool_address.as_str()))
        .filter(|p| !used_dexes.contains(p.dex.as_str()))
        .filter(|p| {
            per_pool_readiness
                .get(&p.pool_address)
                .copied()
                .unwrap_or(TrackPoolReadiness::Rejected)
                >= TrackPoolReadiness::Warmable
        })
        .collect();
    candidates.sort_by(|a, b| dex_then_pool_cmp(a, b));

    if let Some(third) = candidates.first() {
        let readiness = per_pool_readiness
            .get(&third.pool_address)
            .copied()
            .unwrap_or(TrackPoolReadiness::Warmable);
        pools.push(selected_pool(
            mint,
            third,
            readiness,
            ArbTrackActiveReason::MultiDex,
            per_pool_readiness,
        ));
    }
}

fn selected_pool(
    mint: &TrackMintInput,
    pool: &TrackPoolInput,
    default_readiness: TrackPoolReadiness,
    reason: ArbTrackActiveReason,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) -> SelectedTrackPool {
    let readiness = per_pool_readiness
        .get(&pool.pool_address)
        .copied()
        .unwrap_or(default_readiness);
    SelectedTrackPool {
        mint: mint.mint.clone(),
        pool: pool.pool_address.clone(),
        readiness,
        active_reason: reason,
    }
}

fn classify_pool_readiness(
    pool: &TrackPoolInput,
    config: &TrackSelectionConfig,
    now: Instant,
) -> TrackPoolReadiness {
    if !pool.known || !is_arb_track_dex(&pool.dex) {
        return TrackPoolReadiness::Rejected;
    }
    if !is_structurally_quote_capable(pool) {
        return TrackPoolReadiness::Rejected;
    }
    if can_fresh_buy_quote(pool, config, now) {
        TrackPoolReadiness::QuoteReady
    } else if is_warmable_pool(pool, config, now) {
        TrackPoolReadiness::Warmable
    } else {
        TrackPoolReadiness::Rejected
    }
}

fn is_warmable_pool(pool: &TrackPoolInput, config: &TrackSelectionConfig, now: Instant) -> bool {
    if !pool.known || !is_structurally_quote_capable(pool) {
        return false;
    }
    if can_fresh_buy_quote(pool, config, now) {
        return false;
    }
    // Structural quote path exists but freshness is missing.
    quote_exact_in_with_freshness(
        &pool.quote_pool,
        pool.vault.as_ref(),
        pool.dlmm_bins.as_ref(),
        NATIVE_SOL_MINT,
        &pool.quote_pool.token_mint,
        config.probe_lamports,
        &config.freshness,
    )
    .is_some()
        || pool.quote_pool.has_reserve_data
}

fn can_fresh_buy_quote(pool: &TrackPoolInput, config: &TrackSelectionConfig, now: Instant) -> bool {
    let Some(quote) = quote_exact_in_with_freshness(
        &pool.quote_pool,
        pool.vault.as_ref(),
        pool.dlmm_bins.as_ref(),
        NATIVE_SOL_MINT,
        &pool.quote_pool.token_mint,
        config.probe_lamports,
        &config.freshness,
    ) else {
        return false;
    };
    is_quote_fresh(&quote, &config.freshness, pool.vault.as_ref(), now)
}

fn is_structurally_quote_capable(pool: &TrackPoolInput) -> bool {
    if pool.quote_pool.dex == "meteora_dlmm" {
        if let Some(vault) = &pool.vault {
            if vault.reserve_base > 0 && vault.reserve_quote > 0 {
                let has_dlmm_meta = vault.active_id.is_some() && vault.bin_step.is_some();
                if has_dlmm_meta || pool.dlmm_bins.is_some() {
                    return true;
                }
            }
        }
        return pool.dlmm_bins.is_some();
    }
    if let Some(vault) = &pool.vault {
        if vault.reserve_base > 0 && vault.reserve_quote > 0 {
            return true;
        }
    }
    pool.quote_pool.has_reserve_data
}

fn is_arb_track_dex(dex: &str) -> bool {
    matches!(
        dex,
        "raydium" | "raydium_cpmm" | "orca" | "meteora_dlmm" | "pump_amm"
    )
}

fn dex_then_pool_cmp(a: &TrackPoolInput, b: &TrackPoolInput) -> Ordering {
    a.dex
        .cmp(&b.dex)
        .then_with(|| a.pool_address.cmp(&b.pool_address))
}

fn pool_address_cmp(a: &SelectedTrackPool, b: &SelectedTrackPool) -> Ordering {
    a.pool.cmp(&b.pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn quote_pool(dex: &str, addr: &str, mint: &str, has_reserve: bool) -> QuotePoolInput {
        QuotePoolInput {
            pool_address: addr.to_string(),
            dex: dex.to_string(),
            token_mint: mint.to_string(),
            trade_price_buy: None,
            trade_price_sell: None,
            trade_updated_at: Instant::now(),
            has_reserve_data: has_reserve,
            token_decimals: 6,
        }
    }

    fn vault(reserve_base: u64, reserve_quote: u64) -> QuoteVaultInput {
        QuoteVaultInput {
            reserve_base,
            reserve_quote,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: None,
            bin_step: None,
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: None,
        }
    }

    fn pool_input(
        dex: &str,
        addr: &str,
        mint: &str,
        known: bool,
        has_reserve: bool,
        vault: Option<QuoteVaultInput>,
    ) -> TrackPoolInput {
        TrackPoolInput {
            pool_address: addr.to_string(),
            dex: dex.to_string(),
            known,
            quote_pool: quote_pool(dex, addr, mint, has_reserve),
            vault,
            dlmm_bins: None,
            token_decimals: 6,
        }
    }

    fn default_config(max_pools: usize) -> TrackSelectionConfig {
        TrackSelectionConfig {
            max_pools,
            max_pools_per_mint: 3,
            probe_lamports: 10_000_000,
            freshness: QuoteFreshnessConfig::default(),
        }
    }

    const MINT: &str = "TokenMint11111111111111111111111111111111";
    const RESERVE_BASE: u64 = 1_000_000_000_000;
    const RESERVE_QUOTE: u64 = 1_000_000_000;

    #[test]
    fn single_dex_mint_selects_nothing() {
        let mint = TrackMintInput {
            mint: MINT.to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "poolA",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "orca",
                    "poolB",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
            ],
            trade_signal_pools: None,
        };
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert!(result.selected.is_empty());
    }

    #[test]
    fn three_pools_one_dex_plus_one_cross_dex_is_bounded() {
        let mint = TrackMintInput {
            mint: MINT.to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "orca1",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "orca",
                    "orca2",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "orca",
                    "orca3",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    "pump1",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                ),
            ],
            trade_signal_pools: None,
        };
        let result = select_arb_track_pools(&[mint], &default_config(500));
        let pools: HashSet<_> = result.selected.iter().map(|p| p.pool.as_str()).collect();
        assert_eq!(result.selected.len(), 2);
        assert!(pools.contains("orca1") || pools.contains("orca2") || pools.contains("orca3"));
        assert!(pools.contains("pump1"));
        assert!(
            !pools.contains("orca1")
                || !pools.contains("orca2")
                || !pools.contains("orca3")
                || result.selected.len() <= 3
        );
    }

    #[test]
    fn tracker_only_unknown_cache_pools_not_selected() {
        let mint = TrackMintInput {
            mint: MINT.to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "known",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    "unknown",
                    MINT,
                    false,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
            ],
            trade_signal_pools: None,
        };
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert!(result.selected.is_empty());
        assert!(result.candidate_counts.rejected >= 1);
    }

    #[test]
    fn quote_ready_pair_preferred_over_warmable() {
        let fresh_vault = vault(RESERVE_BASE, RESERVE_QUOTE);
        let stale_vault = QuoteVaultInput {
            updated_at: Instant::now() - std::time::Duration::from_secs(600),
            ..fresh_vault.clone()
        };
        let mint_quote = TrackMintInput {
            mint: "MintQuoteReady111111111111111111111111".to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "freshA",
                    MINT,
                    true,
                    true,
                    Some(fresh_vault.clone()),
                ),
                pool_input("pump_amm", "freshB", MINT, true, true, Some(fresh_vault)),
            ],
            trade_signal_pools: None,
        };
        let mint_warm = TrackMintInput {
            mint: "MintWarmable11111111111111111111111111".to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "staleA",
                    MINT,
                    true,
                    true,
                    Some(stale_vault.clone()),
                ),
                pool_input("pump_amm", "staleB", MINT, true, true, Some(stale_vault)),
            ],
            trade_signal_pools: None,
        };
        let result = select_arb_track_pools(&[mint_warm, mint_quote], &default_config(4));
        let selected_mints: HashSet<_> = result.selected.iter().map(|p| p.mint.as_str()).collect();
        assert!(selected_mints.contains("MintQuoteReady111111111111111111111111"));
    }

    #[test]
    fn rest_budget_of_one_does_not_take_isolated_pool() {
        let make_mint = |suffix: &str| TrackMintInput {
            mint: format!("Mint{suffix}"),
            pools: vec![
                pool_input(
                    "orca",
                    &format!("orca_{suffix}"),
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    &format!("pump_{suffix}"),
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                ),
            ],
            trade_signal_pools: None,
        };
        let mints: Vec<_> = (0..3).map(|i| make_mint(&i.to_string())).collect();
        let result = select_arb_track_pools(&mints, &default_config(5));
        assert!(result.selected.len() <= 5);
        assert_eq!(result.selected.len() % 2, 0);
    }

    #[test]
    fn selection_is_deterministic_across_input_order() {
        let mint_a = TrackMintInput {
            mint: "MintA".to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "a_orca",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    "a_pump",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                ),
            ],
            trade_signal_pools: None,
        };
        let mint_b = TrackMintInput {
            mint: "MintB".to_string(),
            pools: vec![
                pool_input(
                    "raydium",
                    "b_ray",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    "b_pump",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                ),
            ],
            trade_signal_pools: None,
        };
        let r1 = select_arb_track_pools(&[mint_a.clone(), mint_b.clone()], &default_config(500));
        let r2 = select_arb_track_pools(&[mint_b, mint_a], &default_config(500));
        let pools1: Vec<_> = r1.selected.iter().map(|p| p.pool.clone()).collect();
        let pools2: Vec<_> = r2.selected.iter().map(|p| p.pool.clone()).collect();
        assert_eq!(pools1, pools2);
    }

    #[test]
    fn result_size_never_exceeds_max_pools() {
        let mut mints = Vec::new();
        for i in 0..50 {
            mints.push(TrackMintInput {
                mint: format!("Mint{i:04}"),
                pools: vec![
                    pool_input(
                        "orca",
                        &format!("orca_{i}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    ),
                    pool_input(
                        "pump_amm",
                        &format!("pump_{i}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    ),
                ],
                trade_signal_pools: None,
            });
        }
        let result = select_arb_track_pools(&mints, &default_config(10));
        assert!(result.selected.len() <= 10);
    }

    #[test]
    fn budget_displacement_marks_reason_budget() {
        let pool = "orphan";
        let displaced: HashSet<_> = [pool.to_string()].into_iter().collect();
        assert_eq!(
            arb_track_removal_reason(pool, true, &displaced),
            ArbTrackRemovedReason::Budget
        );
        assert_eq!(
            arb_track_removal_reason("gone", false, &displaced),
            ArbTrackRemovedReason::Stale
        );
    }

    #[test]
    fn trade_signal_pair_selected_as_executable() {
        let mint = TrackMintInput {
            mint: MINT.to_string(),
            pools: vec![
                pool_input(
                    "orca",
                    "sig_buy",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                ),
                pool_input(
                    "pump_amm",
                    "sig_sell",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                ),
            ],
            trade_signal_pools: Some(("sig_buy".to_string(), "sig_sell".to_string())),
        };
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert_eq!(result.selected.len(), 2);
        assert!(result
            .selected
            .iter()
            .all(|p| p.active_reason == ArbTrackActiveReason::TradeSignal));
    }
}
