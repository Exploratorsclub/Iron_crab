//! Deterministic, bounded Arb Geyser pin selection (I-ARB-10b).
//!
//! Pure selection logic: no locks, NATS, RPC, or logging.

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;

use crate::nats::{ArbTrackActiveReason, ArbTrackRemovedReason};

use super::pool_quote::{
    is_quote_fresh_with_bins, quote_exact_in_with_freshness, select_round_trip_pools,
    DlmmBinArrays, QuoteFreshnessConfig, QuotePoolInput, QuoteVaultInput, RoundTripPoolCandidate,
    NATIVE_SOL_MINT,
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
    /// Latest activity from tracker/cache state (unix ms).
    pub last_activity_unix_ms: u64,
}

/// Per-mint bundle of pools considered for pin selection.
#[derive(Debug, Clone)]
pub struct TrackMintInput {
    pub mint: String,
    pub pools: Vec<TrackPoolInput>,
    /// Optional trade-signal buy/sell pair for this mint (highest pin priority).
    pub trade_signal_pools: Option<(String, String)>,
    /// Mint-level recency for global bundle ranking.
    pub last_activity_unix_ms: u64,
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
    /// Mints with ≥2 selected pools on ≥2 distinct DEXes (I-ARB-10b).
    pub pair_complete_mints: usize,
    /// Selected pools whose mint lacks a cross-DEX pair in the selected set (Soll: 0).
    pub orphan_pools: usize,
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
        let signal_pools: HashSet<&str> = mint
            .trade_signal_pools
            .as_ref()
            .map(|(buy, sell)| [buy.as_str(), sell.as_str()])
            .into_iter()
            .flatten()
            .collect();

        for pool in &mint.pools {
            let mut readiness = classify_pool_readiness(pool, config, now);
            if signal_pools.contains(pool.pool_address.as_str())
                && readiness >= TrackPoolReadiness::Warmable
            {
                readiness = TrackPoolReadiness::Executable;
            }
            candidate_counts.record(readiness);
            per_pool_readiness.insert(pool.pool_address.clone(), readiness);
        }
    }

    let mut bundles: Vec<MintBundle> = Vec::new();
    for mint in mints {
        if let Some(bundle) = build_mint_bundle(mint, config, &per_pool_readiness, now) {
            bundles.push(bundle);
        }
    }

    bundles.sort_by(compare_bundles);

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

    let (pair_complete_mints, orphan_pools) = compute_pair_completeness_stats(&selected, mints);

    TrackSelectionResult {
        selected_mints: selected_mints.len(),
        pair_complete_mints,
        orphan_pools,
        selected,
        budget_displaced,
        candidate_counts,
    }
}

/// Count pair-complete mints and orphan pools in the selected set (I-ARB-10b observability).
pub fn compute_pair_completeness_stats(
    selected: &[SelectedTrackPool],
    mints: &[TrackMintInput],
) -> (usize, usize) {
    let pool_dex: HashMap<&str, &str> = mints
        .iter()
        .flat_map(|mint| {
            mint.pools
                .iter()
                .map(|pool| (pool.pool_address.as_str(), pool.dex.as_str()))
        })
        .collect();

    let mut by_mint: HashMap<&str, Vec<&str>> = HashMap::new();
    for entry in selected {
        if let Some(dex) = pool_dex.get(entry.pool.as_str()) {
            by_mint.entry(entry.mint.as_str()).or_default().push(*dex);
        }
    }

    let mut pair_complete_mints = 0usize;
    let mut orphan_pools = 0usize;
    for pools in by_mint.values() {
        let distinct_dexes: HashSet<&str> = pools.iter().copied().collect();
        if pools.len() >= 2 && distinct_dexes.len() >= 2 {
            pair_complete_mints += 1;
        } else {
            orphan_pools += pools.len();
        }
    }
    (pair_complete_mints, orphan_pools)
}

/// Map selection removals for pools no longer in the target set.
pub fn arb_track_removal_reason(
    pool: &str,
    budget_displaced: &HashSet<String>,
) -> ArbTrackRemovedReason {
    if budget_displaced.contains(pool) {
        ArbTrackRemovedReason::Budget
    } else {
        ArbTrackRemovedReason::Stale
    }
}

#[derive(Debug, Clone)]
struct MintBundle {
    mint: String,
    priority: TrackPoolReadiness,
    last_activity_unix_ms: u64,
    pools: Vec<SelectedTrackPool>,
}

fn compare_bundles(a: &MintBundle, b: &MintBundle) -> Ordering {
    b.priority
        .cmp(&a.priority)
        .then_with(|| b.last_activity_unix_ms.cmp(&a.last_activity_unix_ms))
        .then_with(|| a.mint.cmp(&b.mint))
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

    bundle_from_pinable_pair(mint, &eligible, config, per_pool_readiness, now)
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
        last_activity_unix_ms: mint.last_activity_unix_ms,
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
        last_activity_unix_ms: mint.last_activity_unix_ms,
        pools,
    })
}

fn bundle_from_pinable_pair(
    mint: &TrackMintInput,
    eligible: &[&TrackPoolInput],
    config: &TrackSelectionConfig,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
    now: Instant,
) -> Option<MintBundle> {
    let mut pinable: Vec<&TrackPoolInput> = eligible
        .iter()
        .copied()
        .filter(|p| is_pinable_pool(p, config, now, per_pool_readiness))
        .collect();
    pinable.sort_by(|a, b| {
        pool_readiness(a, per_pool_readiness)
            .cmp(&pool_readiness(b, per_pool_readiness))
            .reverse()
            .then_with(|| dex_then_pool_cmp(a, b))
    });

    let mut best: Option<(&TrackPoolInput, &TrackPoolInput, TrackPoolReadiness)> = None;
    for (i, a) in pinable.iter().enumerate() {
        for b in pinable.iter().skip(i + 1) {
            if a.dex == b.dex {
                continue;
            }
            let ra = pool_readiness(a, per_pool_readiness);
            let rb = pool_readiness(b, per_pool_readiness);
            let bundle_priority = ra.min(rb);
            let replace = match best {
                None => true,
                Some((best_a, best_b, current_priority)) => {
                    if bundle_priority != current_priority {
                        bundle_priority > current_priority
                    } else {
                        let activity = a.last_activity_unix_ms.max(b.last_activity_unix_ms);
                        let best_activity = best_a
                            .last_activity_unix_ms
                            .max(best_b.last_activity_unix_ms);
                        activity > best_activity
                    }
                }
            };
            if replace {
                best = Some((a, b, bundle_priority));
            }
        }
    }
    let (buy_pool, sell_pool, bundle_priority) = best?;

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
        priority: bundle_priority,
        last_activity_unix_ms: mint.last_activity_unix_ms,
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
        .filter(|p| is_pinable_pool(p, config, Instant::now(), per_pool_readiness))
        .collect();
    candidates.sort_by(|a, b| {
        pool_readiness(a, per_pool_readiness)
            .cmp(&pool_readiness(b, per_pool_readiness))
            .reverse()
            .then_with(|| dex_then_pool_cmp(a, b))
    });

    if let Some(third) = candidates.first() {
        let readiness = pool_readiness(third, per_pool_readiness);
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
    let classified = per_pool_readiness
        .get(&pool.pool_address)
        .copied()
        .unwrap_or(TrackPoolReadiness::Rejected);
    // C1h5: bundle default_readiness (e.g. QuoteReady for cross-dex legs) must not be
    // downgraded by classify-only buy freshness — sell-leg pins need must_hot on cold start.
    let readiness = classified.max(default_readiness);
    SelectedTrackPool {
        mint: mint.mint.clone(),
        pool: pool.pool_address.clone(),
        readiness,
        active_reason: reason,
    }
}

fn pool_readiness(
    pool: &TrackPoolInput,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) -> TrackPoolReadiness {
    per_pool_readiness
        .get(&pool.pool_address)
        .copied()
        .unwrap_or(TrackPoolReadiness::Rejected)
}

fn is_pinable_pool(
    pool: &TrackPoolInput,
    config: &TrackSelectionConfig,
    now: Instant,
    per_pool_readiness: &HashMap<String, TrackPoolReadiness>,
) -> bool {
    pool_readiness(pool, per_pool_readiness) >= TrackPoolReadiness::Warmable
        || is_warmable_pool(pool, config, now)
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
    if can_fresh_buy_quote(pool, config, now) || can_fresh_sell_quote(pool, config, now) {
        TrackPoolReadiness::QuoteReady
    } else if is_warmable_pool(pool, config, now) {
        TrackPoolReadiness::Warmable
    } else {
        TrackPoolReadiness::Rejected
    }
}

/// Probe token amount for sell-direction freshness (CPMM estimate from cached reserves).
fn probe_token_amount_for_sell(pool: &TrackPoolInput, probe_lamports: u64) -> u64 {
    let Some(vault) = &pool.vault else {
        return 1;
    };
    if vault.reserve_base == 0 || vault.reserve_quote == 0 {
        return 1;
    }
    let token = vault.reserve_base as u128;
    let sol = vault.reserve_quote as u128;
    let sol_in = probe_lamports as u128;
    let out = token.saturating_mul(sol_in) / sol.saturating_add(sol_in);
    out.max(1) as u64
}

fn can_fresh_sell_quote(
    pool: &TrackPoolInput,
    config: &TrackSelectionConfig,
    now: Instant,
) -> bool {
    let probe_tokens = probe_token_amount_for_sell(pool, config.probe_lamports);
    let Some(quote) = quote_exact_in_with_freshness(
        &pool.quote_pool,
        pool.vault.as_ref(),
        pool.dlmm_bins.as_ref(),
        &pool.quote_pool.token_mint,
        NATIVE_SOL_MINT,
        probe_tokens,
        &config.freshness,
    ) else {
        return false;
    };
    is_quote_fresh_with_bins(
        &quote,
        &config.freshness,
        pool.vault.as_ref(),
        pool.dlmm_bins.as_ref(),
        now,
    )
}

fn is_warmable_pool(pool: &TrackPoolInput, config: &TrackSelectionConfig, now: Instant) -> bool {
    if !pool.known || !is_structurally_quote_capable(pool) {
        return false;
    }
    if can_fresh_buy_quote(pool, config, now) || can_fresh_sell_quote(pool, config, now) {
        return false;
    }
    // Known + structural DEX support; reserve/bin freshness may be missing.
    true
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
    is_quote_fresh_with_bins(
        &quote,
        &config.freshness,
        pool.vault.as_ref(),
        pool.dlmm_bins.as_ref(),
        now,
    )
}

fn is_structurally_quote_capable(pool: &TrackPoolInput) -> bool {
    if !pool.known || !is_arb_track_dex(&pool.dex) {
        return false;
    }
    match pool.quote_pool.dex.as_str() {
        "meteora_dlmm" => {
            if let Some(vault) = &pool.vault {
                if vault.active_id.is_some() && vault.bin_step.is_some() {
                    return true;
                }
            }
            pool.dlmm_bins.is_some() || pool.known
        }
        "orca" | "raydium" | "raydium_cpmm" | "pump_amm" => true,
        _ => false,
    }
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
        activity_ms: u64,
    ) -> TrackPoolInput {
        TrackPoolInput {
            pool_address: addr.to_string(),
            dex: dex.to_string(),
            known,
            quote_pool: quote_pool(dex, addr, mint, has_reserve),
            vault,
            dlmm_bins: None,
            token_decimals: 6,
            last_activity_unix_ms: activity_ms,
        }
    }

    fn mint_input(
        mint: &str,
        pools: Vec<TrackPoolInput>,
        activity_ms: u64,
        trade_signal: Option<(String, String)>,
    ) -> TrackMintInput {
        TrackMintInput {
            mint: mint.to_string(),
            pools,
            trade_signal_pools: trade_signal,
            last_activity_unix_ms: activity_ms,
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
    fn cross_dex_quotable_pair_both_selected() {
        let fresh_vault = vault(RESERVE_BASE, RESERVE_QUOTE);
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "orca",
                    "orca_quote",
                    MINT,
                    true,
                    true,
                    Some(fresh_vault),
                    10,
                ),
                pool_input(
                    "pump_amm",
                    "pump_quote",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    9,
                ),
            ],
            10,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert_eq!(result.selected.len(), 2);
        assert_eq!(result.pair_complete_mints, 1);
        assert_eq!(result.orphan_pools, 0);
        let pools: HashSet<_> = result.selected.iter().map(|p| p.pool.as_str()).collect();
        assert!(pools.contains("orca_quote"));
        assert!(pools.contains("pump_quote"));
    }

    #[test]
    fn multi_dex_mint_rejected_when_filters_leave_lt_2_candidates() {
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "orca",
                    "orca_ok",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    10,
                ),
                pool_input("pump_amm", "pump_unknown", MINT, false, false, None, 9),
                pool_input("raydium", "ray_unknown", MINT, false, false, None, 8),
            ],
            10,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert!(result.selected.is_empty());
        assert_eq!(result.pair_complete_mints, 0);
        assert_eq!(result.orphan_pools, 0);
    }

    #[test]
    fn budget_prefers_fewer_complete_mints_over_orphan_fill() {
        let make_mint = |idx: u64| {
            mint_input(
                &format!("Mint{idx:04}"),
                vec![
                    pool_input(
                        "orca",
                        &format!("orca_{idx}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                        idx,
                    ),
                    pool_input(
                        "pump_amm",
                        &format!("pump_{idx}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                        idx,
                    ),
                ],
                idx,
                None,
            )
        };
        let mints: Vec<_> = (0..5).map(make_mint).collect();
        let result = select_arb_track_pools(&mints, &default_config(4));
        assert_eq!(result.selected.len(), 4);
        assert_eq!(result.orphan_pools, 0);
        assert_eq!(result.pair_complete_mints, 2);
        assert_eq!(result.selected_mints, 2);
    }

    #[test]
    fn pair_complete_stats_detect_orphans() {
        let selected = vec![
            SelectedTrackPool {
                mint: "m1".to_string(),
                pool: "solo".to_string(),
                readiness: TrackPoolReadiness::QuoteReady,
                active_reason: ArbTrackActiveReason::MultiDex,
            },
            SelectedTrackPool {
                mint: "m2".to_string(),
                pool: "a".to_string(),
                readiness: TrackPoolReadiness::QuoteReady,
                active_reason: ArbTrackActiveReason::MultiDex,
            },
            SelectedTrackPool {
                mint: "m2".to_string(),
                pool: "b".to_string(),
                readiness: TrackPoolReadiness::QuoteReady,
                active_reason: ArbTrackActiveReason::MultiDex,
            },
        ];
        let mints = vec![
            TrackMintInput {
                mint: "m1".to_string(),
                pools: vec![pool_input("orca", "solo", MINT, true, true, None, 1)],
                trade_signal_pools: None,
                last_activity_unix_ms: 1,
            },
            TrackMintInput {
                mint: "m2".to_string(),
                pools: vec![
                    pool_input("orca", "a", MINT, true, true, None, 2),
                    pool_input("pump_amm", "b", MINT, true, true, None, 2),
                ],
                trade_signal_pools: None,
                last_activity_unix_ms: 2,
            },
        ];
        let (pair_complete, orphans) = compute_pair_completeness_stats(&selected, &mints);
        assert_eq!(pair_complete, 1);
        assert_eq!(orphans, 1);
    }

    #[test]
    fn single_dex_mint_selects_nothing() {
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "orca",
                    "poolA",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    1,
                ),
                pool_input(
                    "orca",
                    "poolB",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    2,
                ),
            ],
            2,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert!(result.selected.is_empty());
    }

    #[test]
    fn known_cache_pool_without_reserves_is_warmable() {
        let mint = mint_input(
            MINT,
            vec![
                pool_input("orca", "warm", MINT, true, false, None, 1),
                pool_input(
                    "pump_amm",
                    "fresh",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    2,
                ),
            ],
            2,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        let pools: HashSet<_> = result.selected.iter().map(|p| p.pool.as_str()).collect();
        assert!(pools.contains("warm"));
        assert!(pools.contains("fresh"));
        assert!(result.candidate_counts.warmable >= 1);
    }

    #[test]
    fn quote_ready_plus_warmable_pair_selected() {
        let fresh_vault = vault(RESERVE_BASE, RESERVE_QUOTE);
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "orca",
                    "quote_ready",
                    MINT,
                    true,
                    true,
                    Some(fresh_vault),
                    10,
                ),
                pool_input("pump_amm", "warm_only", MINT, true, false, None, 5),
            ],
            10,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        let pools: HashSet<_> = result.selected.iter().map(|p| p.pool.as_str()).collect();
        assert_eq!(result.selected.len(), 2);
        assert!(pools.contains("quote_ready"));
        assert!(pools.contains("warm_only"));
    }

    #[test]
    fn newer_equal_readiness_bundle_wins_over_older_mint_name() {
        let old_mint = mint_input(
            "AAAAOldMint111111111111111111111111111",
            vec![
                pool_input(
                    "orca",
                    "old_orca",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    1,
                ),
                pool_input(
                    "pump_amm",
                    "old_pump",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    1,
                ),
            ],
            1,
            None,
        );
        let new_mint = mint_input(
            "ZZZZNewMint111111111111111111111111111",
            vec![
                pool_input(
                    "orca",
                    "new_orca",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    9_999,
                ),
                pool_input(
                    "pump_amm",
                    "new_pump",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    9_999,
                ),
            ],
            9_999,
            None,
        );
        let result = select_arb_track_pools(&[old_mint, new_mint], &default_config(2));
        let selected_mints: HashSet<_> = result.selected.iter().map(|p| p.mint.as_str()).collect();
        assert!(selected_mints.contains("ZZZZNewMint111111111111111111111111111"));
        assert!(!selected_mints.contains("AAAAOldMint111111111111111111111111111"));
    }

    #[test]
    fn budget_displacement_marks_reason_budget_only_when_displaced() {
        let displaced: HashSet<_> = ["pool_budget".to_string()].into_iter().collect();
        assert_eq!(
            arb_track_removal_reason("pool_budget", &displaced),
            ArbTrackRemovedReason::Budget
        );
        assert_eq!(
            arb_track_removal_reason("pool_stale", &displaced),
            ArbTrackRemovedReason::Stale
        );
    }

    #[test]
    fn trade_signal_pair_selected_as_executable() {
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "orca",
                    "sig_buy",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    1,
                ),
                pool_input(
                    "pump_amm",
                    "sig_sell",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                    2,
                ),
            ],
            2,
            Some(("sig_buy".to_string(), "sig_sell".to_string())),
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert_eq!(result.selected.len(), 2);
        assert_eq!(result.candidate_counts.executable, 2);
        assert!(result
            .selected
            .iter()
            .all(|p| p.active_reason == ArbTrackActiveReason::TradeSignal));
    }

    #[test]
    fn result_size_never_exceeds_max_pools() {
        let mut mints = Vec::new();
        for i in 0..50 {
            mints.push(mint_input(
                &format!("Mint{i:04}"),
                vec![
                    pool_input(
                        "orca",
                        &format!("orca_{i}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                        i as u64,
                    ),
                    pool_input(
                        "pump_amm",
                        &format!("pump_{i}"),
                        MINT,
                        true,
                        true,
                        Some(vault(RESERVE_BASE, RESERVE_QUOTE * 2)),
                        i as u64,
                    ),
                ],
                i as u64,
                None,
            ));
        }
        let result = select_arb_track_pools(&mints, &default_config(10));
        assert!(result.selected.len() <= 10);
    }

    /// C1h5: cross-dex pinable bundles must emit QuoteReady on both legs (must_hot) even when
    /// classify only sees warmable buy-side freshness (sell-leg cold-start).
    #[test]
    fn pinable_cross_dex_pair_promotes_warmable_legs_to_quote_ready() {
        let mint = mint_input(
            MINT,
            vec![
                pool_input(
                    "meteora_dlmm",
                    "dlmm_buy",
                    MINT,
                    true,
                    true,
                    Some(vault(RESERVE_BASE, RESERVE_QUOTE)),
                    10,
                ),
                pool_input("pump_amm", "pump_sell", MINT, true, false, None, 5),
            ],
            10,
            None,
        );
        let result = select_arb_track_pools(&[mint], &default_config(500));
        assert_eq!(result.selected.len(), 2);
        for entry in &result.selected {
            assert_eq!(
                entry.readiness,
                TrackPoolReadiness::QuoteReady,
                "pool {} must be quote_ready for must_hot pin",
                entry.pool
            );
        }
    }

    #[test]
    fn fresh_sell_quote_classifies_pool_quote_ready_without_fresh_buy() {
        use std::time::{Duration, Instant};

        let mut sell_vault = vault(RESERVE_BASE, RESERVE_QUOTE);
        sell_vault.updated_at = Instant::now();
        let pool = pool_input(
            "pump_amm",
            "sell_only_fresh",
            MINT,
            true,
            true,
            Some(sell_vault),
            1,
        );
        let config = TrackSelectionConfig {
            max_pools: 10,
            max_pools_per_mint: 3,
            probe_lamports: 10_000_000,
            freshness: QuoteFreshnessConfig {
                trade_ttl_ms: 30_000,
                state_ttl_ms: 120_000,
            },
        };
        assert!(can_fresh_sell_quote(&pool, &config, Instant::now()));
        assert_eq!(
            classify_pool_readiness(&pool, &config, Instant::now()),
            TrackPoolReadiness::QuoteReady
        );

        let stale_vault = vault(RESERVE_BASE, RESERVE_QUOTE);
        let stale_pool = pool_input(
            "pump_amm",
            "sell_stale",
            MINT,
            true,
            true,
            Some({
                let mut v = stale_vault;
                v.updated_at = Instant::now() - Duration::from_secs(400);
                v
            }),
            1,
        );
        assert!(!can_fresh_sell_quote(&stale_pool, &config, Instant::now()));
    }
}
