//! Unified pool quote layer for profit-first arbitrage (M1).
//!
//! Contract: `Iron_crab-eval/docs/spec/ARB_QUOTE_CONTRACT.md`
//! Legacy `comparable_price_sol_per_token` in arb-strategy remains authoritative until M5.

use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::str::FromStr;
use std::time::{Duration, Instant};

use crate::execution::live_pool_cache::{CachedPoolState, MeteoraState};
use crate::ipc::BinData;
use crate::solana::dex::meteora_bin_walker::{dlmm_fee_bps, walker_from_bins};
use solana_sdk::pubkey::Pubkey;

pub const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111112";
/// Small SOL probe for DLMM marginal price / screening (0.01 SOL).
pub const DLMM_PROBE_SOL_LAMPORTS: u64 = 10_000_000;
/// Trade-implied quote TTL (v2 default 30s).
pub const TRADE_TTL_MS: u64 = 30_000;
/// Vault/bin state TTL when reserve snapshot unchanged (v2 default 120s).
pub const STATE_TTL_MS: u64 = 120_000;

/// Configurable TTLs for quote freshness (I-ARB-4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuoteFreshnessConfig {
    pub trade_ttl_ms: u64,
    pub state_ttl_ms: u64,
}

impl Default for QuoteFreshnessConfig {
    fn default() -> Self {
        Self {
            trade_ttl_ms: TRADE_TTL_MS,
            state_ttl_ms: STATE_TTL_MS,
        }
    }
}

/// Hash of vault reserve snapshot used for ExecutableMarginal freshness.
pub fn state_fingerprint(vault: &QuoteVaultInput) -> u64 {
    let mut hasher = DefaultHasher::new();
    vault.reserve_base.hash(&mut hasher);
    vault.reserve_quote.hash(&mut hasher);
    vault.active_id.hash(&mut hasher);
    vault.bin_step.hash(&mut hasher);
    hasher.finish()
}

/// I-ARB-4: re-check quote freshness (trade TTL vs state fingerprint + state TTL).
pub fn is_quote_fresh(
    quote: &PoolQuote,
    config: &QuoteFreshnessConfig,
    current_vault: Option<&QuoteVaultInput>,
    now: Instant,
) -> bool {
    diagnose_quote_not_fresh(quote, config, current_vault, now).is_none()
}

/// Age bucket for vault/state-stale forensics (C1h2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FreshnessAgeBucket {
    Le30s,
    Le120s,
    Le300s,
    Gt300s,
}

impl FreshnessAgeBucket {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::Le30s => "le_30s",
            Self::Le120s => "le_120s",
            Self::Le300s => "le_300s",
            Self::Gt300s => "gt_300s",
        }
    }
}

/// Map elapsed milliseconds to freshness forensics buckets.
pub fn freshness_age_bucket(age_ms: u64) -> FreshnessAgeBucket {
    if age_ms <= 30_000 {
        FreshnessAgeBucket::Le30s
    } else if age_ms <= 120_000 {
        FreshnessAgeBucket::Le120s
    } else if age_ms <= 300_000 {
        FreshnessAgeBucket::Le300s
    } else {
        FreshnessAgeBucket::Gt300s
    }
}

/// Vault reserve snapshot age in milliseconds (for StateStale drill-down).
pub fn vault_state_age_ms(vault: &QuoteVaultInput, now: Instant) -> u64 {
    now.duration_since(vault.updated_at).as_millis() as u64
}

/// Quote kind label for NotFresh forensics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuoteNotFreshKind {
    ExecutableMarginal,
    LastTradeMid,
}

impl QuoteNotFreshKind {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::ExecutableMarginal => "executable_marginal",
            Self::LastTradeMid => "last_trade_mid",
        }
    }

    fn from_quote_kind(kind: QuoteKind) -> Self {
        match kind {
            QuoteKind::ExecutableMarginal => Self::ExecutableMarginal,
            QuoteKind::LastTradeMid => Self::LastTradeMid,
        }
    }
}

/// Root cause when `is_quote_fresh` returns false.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuoteNotFreshCause {
    AgeExceeded,
    FingerprintMismatch,
}

impl QuoteNotFreshCause {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::AgeExceeded => "age_exceeded",
            Self::FingerprintMismatch => "fingerprint_mismatch",
        }
    }
}

/// Drill-down for cross-DEX sell `NotFresh` (C1h2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuoteNotFreshDiagnosis {
    pub kind: QuoteNotFreshKind,
    pub cause: QuoteNotFreshCause,
}

/// Returns `Some(diagnosis)` when quote fails freshness re-check.
pub fn diagnose_quote_not_fresh(
    quote: &PoolQuote,
    config: &QuoteFreshnessConfig,
    current_vault: Option<&QuoteVaultInput>,
    now: Instant,
) -> Option<QuoteNotFreshDiagnosis> {
    let kind = QuoteNotFreshKind::from_quote_kind(quote.kind);
    match quote.kind {
        QuoteKind::LastTradeMid => {
            if now.duration_since(quote.as_of_ts) <= Duration::from_millis(config.trade_ttl_ms) {
                return None;
            }
            Some(QuoteNotFreshDiagnosis {
                kind,
                cause: QuoteNotFreshCause::AgeExceeded,
            })
        }
        QuoteKind::ExecutableMarginal => {
            if now.duration_since(quote.as_of_ts) > Duration::from_millis(config.state_ttl_ms) {
                return Some(QuoteNotFreshDiagnosis {
                    kind,
                    cause: QuoteNotFreshCause::AgeExceeded,
                });
            }
            match current_vault {
                Some(vault) if state_fingerprint(vault) != quote.state_fingerprint => {
                    Some(QuoteNotFreshDiagnosis {
                        kind,
                        cause: QuoteNotFreshCause::FingerprintMismatch,
                    })
                }
                _ => None,
            }
        }
    }
}

/// Subreason when no candidate produces a fresh buy quote (C1h2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoFreshBuyQuoteSubreason {
    QuoteNone,
    StateStale,
    TradeStale,
    NotFreshAfterQuote,
}

impl NoFreshBuyQuoteSubreason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::QuoteNone => "quote_none",
            Self::StateStale => "state_stale",
            Self::TradeStale => "trade_stale",
            Self::NotFreshAfterQuote => "not_fresh_after_quote",
        }
    }
}

/// Diagnose why a buy leg failed freshness for round-trip selection.
pub fn diagnose_no_fresh_buy_quote(
    pool: &QuotePoolInput,
    vault: Option<&QuoteVaultInput>,
    dlmm_bins: Option<&DlmmBinArrays>,
    probe_lamports: u64,
    freshness: &QuoteFreshnessConfig,
    now: Instant,
) -> NoFreshBuyQuoteSubreason {
    let buy_quote = quote_exact_in_with_freshness(
        pool,
        vault,
        dlmm_bins,
        NATIVE_SOL_MINT,
        &pool.token_mint,
        probe_lamports,
        freshness,
    );
    let Some(buy_quote) = buy_quote else {
        if let Some(v) = vault {
            if !state_fresh(v, now, freshness.state_ttl_ms) {
                return NoFreshBuyQuoteSubreason::StateStale;
            }
        }
        if trade_fresh(pool, now, freshness.trade_ttl_ms) {
            return NoFreshBuyQuoteSubreason::QuoteNone;
        }
        return NoFreshBuyQuoteSubreason::TradeStale;
    };
    if is_quote_fresh(&buy_quote, freshness, vault, now) {
        return NoFreshBuyQuoteSubreason::NotFreshAfterQuote;
    }
    match diagnose_quote_not_fresh(&buy_quote, freshness, vault, now) {
        Some(QuoteNotFreshDiagnosis {
            kind: QuoteNotFreshKind::LastTradeMid,
            ..
        }) => NoFreshBuyQuoteSubreason::TradeStale,
        Some(QuoteNotFreshDiagnosis {
            kind: QuoteNotFreshKind::ExecutableMarginal,
            cause: QuoteNotFreshCause::AgeExceeded,
        }) => NoFreshBuyQuoteSubreason::StateStale,
        Some(QuoteNotFreshDiagnosis {
            kind: QuoteNotFreshKind::ExecutableMarginal,
            cause: QuoteNotFreshCause::FingerprintMismatch,
        }) => NoFreshBuyQuoteSubreason::StateStale,
        None => NoFreshBuyQuoteSubreason::NotFreshAfterQuote,
    }
}

const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const STABLECOIN_MIN_SOL_PER_TOKEN: &str = "0.0001";
const STABLECOIN_MAX_SOL_PER_TOKEN: &str = "1";
const DLMM_MARGINAL_MAX_DEVIATION_FACTOR: u64 = 100;

/// Quote provenance — cross-DEX pairing requires identical kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteKind {
    ExecutableMarginal,
    LastTradeMid,
}

/// Swap direction for `quote_exact_in`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuoteSide {
    Buy,
    Sell,
}

/// Executable or trade-mid quote for a single pool leg.
#[derive(Debug, Clone)]
pub struct PoolQuote {
    pub pool_address: String,
    pub dex: String,
    pub kind: QuoteKind,
    pub side: QuoteSide,
    pub as_of_slot: u64,
    pub as_of_ts: Instant,
    pub fresh: bool,
    /// Vault reserve snapshot hash at quote time (ExecutableMarginal only).
    pub state_fingerprint: u64,
    pub amount_in: u64,
    pub amount_out: u64,
}

/// Pool-level inputs for quoting (SOL-quoted pairs only).
#[derive(Debug, Clone)]
pub struct QuotePoolInput {
    pub pool_address: String,
    pub dex: String,
    pub token_mint: String,
    pub trade_price_buy: Option<Decimal>,
    pub trade_price_sell: Option<Decimal>,
    pub trade_updated_at: Instant,
    pub has_reserve_data: bool,
    pub token_decimals: u8,
}

/// Geyser vault / DLMM state for marginal quotes.
#[derive(Debug, Clone)]
pub struct QuoteVaultInput {
    pub reserve_base: u64,
    pub reserve_quote: u64,
    pub update_slot: u64,
    pub updated_at: Instant,
    pub active_id: Option<i32>,
    pub bin_step: Option<u16>,
    pub dlmm_sol_is_x: bool,
    pub dlmm_token_x_mint: Option<String>,
}

pub type DlmmBinArrays = HashMap<i64, Vec<BinData>>;

/// True iff both quotes may be paired for cross-DEX round-trip screening.
pub fn quotes_pairable(a: &PoolQuote, b: &PoolQuote) -> bool {
    a.kind == b.kind
}

pub fn vault_dlmm_sol_is_x(vault: &QuoteVaultInput) -> bool {
    vault
        .dlmm_token_x_mint
        .as_deref()
        .map(|m| m == NATIVE_SOL_MINT)
        .unwrap_or(vault.dlmm_sol_is_x)
}

pub fn flatten_bin_array_bins(arrays: &HashMap<i64, Vec<BinData>>) -> DlmmBinArrays {
    arrays.clone()
}

fn flatten_bins_for_walker(bin_arrays: &HashMap<i64, Vec<BinData>>) -> Vec<(i32, u64, u64)> {
    let mut all_bins: Vec<(i32, u64, u64)> = Vec::new();
    for (array_idx, bins) in bin_arrays {
        for bin in bins {
            let bin_id = (*array_idx * 70 + bin.offset as i64) as i32;
            all_bins.push((bin_id, bin.amount_x, bin.amount_y));
        }
    }
    all_bins.sort_by_key(|(id, _, _)| *id);
    all_bins
}

fn active_bin_present(active_id: i32, flat_bins: &[(i32, u64, u64)]) -> bool {
    flat_bins.iter().any(|(id, _, _)| *id == active_id)
}

pub fn dlmm_marginal_price_plausible(
    marginal: Decimal,
    reserve_mid: Option<Decimal>,
    trade_mid: Option<Decimal>,
) -> bool {
    let reference = match reserve_mid.or(trade_mid) {
        Some(mid) if mid > Decimal::ZERO => mid,
        _ => return true,
    };
    if marginal <= Decimal::ZERO {
        return false;
    }
    let ratio = if marginal > reference {
        marginal / reference
    } else {
        reference / marginal
    };
    ratio <= Decimal::from(DLMM_MARGINAL_MAX_DEVIATION_FACTOR)
}

/// DLMM token output via BinWalker (SOL → token).
pub fn dlmm_token_output_from_bins(
    active_id: i32,
    bin_step: u16,
    sol_in_lamports: u64,
    bin_arrays: &HashMap<i64, Vec<BinData>>,
    sol_is_x: bool,
) -> Option<u64> {
    if bin_arrays.is_empty() || sol_in_lamports == 0 {
        return None;
    }

    let flat_bins = flatten_bins_for_walker(bin_arrays);
    if !active_bin_present(active_id, &flat_bins) {
        return None;
    }

    let walker = walker_from_bins(active_id, bin_step, &flat_bins);
    let fee_bps = dlmm_fee_bps(bin_step);
    let (amount_out, _, _) = if sol_is_x {
        walker.quote_x_to_y(sol_in_lamports, fee_bps).ok()?
    } else {
        walker.quote_y_to_x(sol_in_lamports, fee_bps).ok()?
    };
    if amount_out == 0 {
        return None;
    }
    Some(amount_out)
}

/// DLMM SOL output via BinWalker (token → SOL).
pub fn dlmm_sol_output_from_bins(
    active_id: i32,
    bin_step: u16,
    token_in: u64,
    bin_arrays: &HashMap<i64, Vec<BinData>>,
    sol_is_x: bool,
) -> Option<u64> {
    if bin_arrays.is_empty() || token_in == 0 {
        return None;
    }

    let flat_bins = flatten_bins_for_walker(bin_arrays);
    if !active_bin_present(active_id, &flat_bins) {
        return None;
    }

    let walker = walker_from_bins(active_id, bin_step, &flat_bins);
    let fee_bps = dlmm_fee_bps(bin_step);
    let (amount_out, _, _) = if sol_is_x {
        walker.quote_y_to_x(token_in, fee_bps).ok()?
    } else {
        walker.quote_x_to_y(token_in, fee_bps).ok()?
    };
    if amount_out == 0 {
        return None;
    }
    Some(amount_out)
}

fn is_stablecoin_mint(mint: &str) -> bool {
    mint == USDC_MINT || mint == USDT_MINT
}

fn is_plausible_sol_per_token_price(mint: &str, price: Decimal) -> bool {
    if price <= Decimal::ZERO {
        return false;
    }
    if is_stablecoin_mint(mint) {
        let min = Decimal::from_str(STABLECOIN_MIN_SOL_PER_TOKEN).unwrap_or(Decimal::ZERO);
        let max = Decimal::from_str(STABLECOIN_MAX_SOL_PER_TOKEN).unwrap_or(Decimal::ONE);
        price >= min && price <= max
    } else {
        true
    }
}

fn reserve_mid_sol_per_token(
    reserve_base: u64,
    reserve_quote: u64,
    token_decimals: u8,
) -> Option<Decimal> {
    if reserve_base == 0 || reserve_quote == 0 {
        return None;
    }
    let sol = Decimal::from(reserve_quote) / Decimal::from(1_000_000_000u64);
    let token_divisor = 10u64.pow(token_decimals as u32);
    let tokens = Decimal::from(reserve_base) / Decimal::from(token_divisor);
    if tokens <= Decimal::ZERO {
        return None;
    }
    Some(sol / tokens)
}

fn reserves_plausible(
    reserve_base: u64,
    reserve_quote: u64,
    token_decimals: u8,
    token_mint: &str,
) -> bool {
    reserve_base > 0
        && reserve_quote > 0
        && reserve_mid_sol_per_token(reserve_base, reserve_quote, token_decimals)
            .is_some_and(|mid| is_plausible_sol_per_token_price(token_mint, mid))
}

fn trade_mid_sol_per_token(pool: &QuotePoolInput) -> Option<Decimal> {
    match (pool.trade_price_buy, pool.trade_price_sell) {
        (Some(buy), Some(sell)) if buy > Decimal::ZERO && sell > Decimal::ZERO => {
            Some((buy + sell) / Decimal::from(2))
        }
        (Some(one), None) | (None, Some(one)) if one > Decimal::ZERO => Some(one),
        _ => None,
    }
}

fn trade_fresh(pool: &QuotePoolInput, now: Instant, trade_ttl_ms: u64) -> bool {
    now.duration_since(pool.trade_updated_at) <= Duration::from_millis(trade_ttl_ms)
}

fn state_fresh(vault: &QuoteVaultInput, now: Instant, state_ttl_ms: u64) -> bool {
    now.duration_since(vault.updated_at) <= Duration::from_millis(state_ttl_ms)
}

fn cpmm_fee_bps(dex: &str) -> u64 {
    match dex {
        "pump_amm" => 100,
        "orca" => 30,
        _ => 25,
    }
}

fn cpmm_amount_out(
    reserve_in: u128,
    reserve_out: u128,
    amount_in: u64,
    fee_bps: u64,
) -> Option<u64> {
    if reserve_in == 0 || reserve_out == 0 || amount_in == 0 {
        return None;
    }
    let amount_in = amount_in as u128;
    let fee_multiplier = 10000u128 - fee_bps as u128;
    let amount_after_fee = amount_in.checked_mul(fee_multiplier)? / 10000;
    if amount_after_fee == 0 {
        return None;
    }
    let numerator = reserve_out.checked_mul(amount_after_fee)?;
    let denominator = reserve_in.checked_add(amount_after_fee)?;
    if denominator == 0 {
        return None;
    }
    Some((numerator / denominator) as u64)
}

fn supports_cpmm(dex: &str) -> bool {
    matches!(
        dex,
        "pump_amm" | "raydium" | "raydium_cpmm" | "orca" | "meteora_cpmm"
    )
}

fn quote_side_from_mints(token_mint: &str, mint_in: &str, mint_out: &str) -> Option<QuoteSide> {
    if mint_in == NATIVE_SOL_MINT && mint_out == token_mint {
        Some(QuoteSide::Buy)
    } else if mint_in == token_mint && mint_out == NATIVE_SOL_MINT {
        Some(QuoteSide::Sell)
    } else {
        None
    }
}

fn trade_implied_sol_per_token(sol_amount: u64, token_amount: u64, token_decimals: u8) -> Decimal {
    let sol_dec = Decimal::from(sol_amount) / Decimal::from(1_000_000_000u64);
    let token_divisor = 10u64.pow(token_decimals as u32);
    let token_dec = Decimal::from(token_amount) / Decimal::from(token_divisor);
    if token_dec <= Decimal::ZERO {
        return Decimal::ZERO;
    }
    sol_dec / token_dec
}

fn tokens_from_trade_price(sol_lamports: u64, price: Decimal, token_decimals: u8) -> Option<u64> {
    if price <= Decimal::ZERO || sol_lamports == 0 {
        return None;
    }
    let sol = Decimal::from(sol_lamports) / Decimal::from(1_000_000_000u64);
    let tokens_whole = sol / price;
    let token_divisor = Decimal::from(10u64.pow(token_decimals as u32));
    let raw = (tokens_whole * token_divisor).floor();
    raw.to_u64()
}

fn sol_from_trade_price(token_raw: u64, price: Decimal, token_decimals: u8) -> Option<u64> {
    if price <= Decimal::ZERO || token_raw == 0 {
        return None;
    }
    let token_divisor = Decimal::from(10u64.pow(token_decimals as u32));
    let tokens_whole = Decimal::from(token_raw) / token_divisor;
    let sol = tokens_whole * price;
    let lamports = (sol * Decimal::from(1_000_000_000u64)).floor();
    lamports.to_u64().filter(|v| *v > 0)
}

fn executable_marginal_quote(
    pool: &QuotePoolInput,
    vault: &QuoteVaultInput,
    dlmm_bins: Option<&DlmmBinArrays>,
    side: QuoteSide,
    amount_in: u64,
    now: Instant,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    if !state_fresh(vault, now, freshness.state_ttl_ms) {
        return None;
    }

    let fingerprint = state_fingerprint(vault);

    if pool.dex == "meteora_dlmm" {
        let active_id = vault.active_id?;
        let bin_step = vault.bin_step?;
        let bins = dlmm_bins?;
        let sol_is_x = vault_dlmm_sol_is_x(vault);
        let amount_out = match side {
            QuoteSide::Buy => {
                dlmm_token_output_from_bins(active_id, bin_step, amount_in, bins, sol_is_x)?
            }
            QuoteSide::Sell => {
                dlmm_sol_output_from_bins(active_id, bin_step, amount_in, bins, sol_is_x)?
            }
        };
        let reserve_mid = reserves_plausible(
            vault.reserve_base,
            vault.reserve_quote,
            pool.token_decimals,
            &pool.token_mint,
        )
        .then(|| {
            reserve_mid_sol_per_token(vault.reserve_base, vault.reserve_quote, pool.token_decimals)
        })
        .flatten();
        let trade_mid = trade_mid_sol_per_token(pool)
            .filter(|p| is_plausible_sol_per_token_price(&pool.token_mint, *p));
        let marginal_price = match side {
            QuoteSide::Buy => {
                trade_implied_sol_per_token(amount_in, amount_out, pool.token_decimals)
            }
            QuoteSide::Sell => {
                trade_implied_sol_per_token(amount_out, amount_in, pool.token_decimals)
            }
        };
        // Buy screening uses a small SOL probe where marginal ≈ mid. Sell legs in round-trip
        // pairing use the full buy token amount; slippage vs reserve mid is expected and must
        // not reject an otherwise valid bin-walker quote (P2 sell_quote_none fix).
        let marginal_ok = if side == QuoteSide::Sell {
            is_plausible_sol_per_token_price(&pool.token_mint, marginal_price)
        } else {
            dlmm_marginal_price_plausible(marginal_price, reserve_mid, trade_mid)
                && is_plausible_sol_per_token_price(&pool.token_mint, marginal_price)
        };
        if !marginal_ok {
            return None;
        }
        return Some(PoolQuote {
            pool_address: pool.pool_address.clone(),
            dex: pool.dex.clone(),
            kind: QuoteKind::ExecutableMarginal,
            side,
            as_of_slot: vault.update_slot,
            as_of_ts: vault.updated_at,
            fresh: true,
            state_fingerprint: fingerprint,
            amount_in,
            amount_out,
        });
    }

    if !supports_cpmm(&pool.dex) {
        return None;
    }
    if !reserves_plausible(
        vault.reserve_base,
        vault.reserve_quote,
        pool.token_decimals,
        &pool.token_mint,
    ) {
        return None;
    }

    let fee_bps = cpmm_fee_bps(&pool.dex);
    let (reserve_in, reserve_out) = match side {
        QuoteSide::Buy => (vault.reserve_quote as u128, vault.reserve_base as u128),
        QuoteSide::Sell => (vault.reserve_base as u128, vault.reserve_quote as u128),
    };
    let amount_out = cpmm_amount_out(reserve_in, reserve_out, amount_in, fee_bps)?;
    Some(PoolQuote {
        pool_address: pool.pool_address.clone(),
        dex: pool.dex.clone(),
        kind: QuoteKind::ExecutableMarginal,
        side,
        as_of_slot: vault.update_slot,
        as_of_ts: vault.updated_at,
        fresh: true,
        state_fingerprint: fingerprint,
        amount_in,
        amount_out,
    })
}

fn last_trade_mid_quote(
    pool: &QuotePoolInput,
    side: QuoteSide,
    amount_in: u64,
    now: Instant,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    if !trade_fresh(pool, now, freshness.trade_ttl_ms) {
        return None;
    }
    let price = match side {
        QuoteSide::Buy => pool.trade_price_buy?,
        QuoteSide::Sell => pool.trade_price_sell?,
    };
    if price <= Decimal::ZERO || !is_plausible_sol_per_token_price(&pool.token_mint, price) {
        return None;
    }
    let amount_out = match side {
        QuoteSide::Buy => tokens_from_trade_price(amount_in, price, pool.token_decimals)?,
        QuoteSide::Sell => sol_from_trade_price(amount_in, price, pool.token_decimals)?,
    };
    Some(PoolQuote {
        pool_address: pool.pool_address.clone(),
        dex: pool.dex.clone(),
        kind: QuoteKind::LastTradeMid,
        side,
        as_of_slot: 0,
        as_of_ts: pool.trade_updated_at,
        fresh: true,
        state_fingerprint: 0,
        amount_in,
        amount_out,
    })
}

/// SOL-quoted token reserves extracted from [`CachedPoolState`] (base = token, quote = SOL).
#[derive(Debug, Clone)]
pub struct SolQuotedPoolSeed {
    pub token_mint: String,
    pub reserve_base: u64,
    pub reserve_quote: u64,
    pub active_id: Option<i32>,
    pub bin_step: Option<u16>,
    pub dlmm_token_x_mint: Option<String>,
}

/// True when a quote may drive beam expansion / quote-ready index.
pub fn is_usable_quote_kind(kind: QuoteKind) -> bool {
    matches!(
        kind,
        QuoteKind::ExecutableMarginal | QuoteKind::LastTradeMid
    )
}

fn orca_sol_quoted_vault_reserves(
    mint_a: &str,
    mint_b: &str,
    vault_a: u64,
    vault_b: u64,
) -> Option<(u64, u64)> {
    if mint_a == NATIVE_SOL_MINT {
        Some((vault_b, vault_a))
    } else if mint_b == NATIVE_SOL_MINT {
        Some((vault_a, vault_b))
    } else {
        None
    }
}

/// Map SLAVE [`CachedPoolState`] to SOL-quoted reserves for `quote_exact_in`.
pub fn sol_quoted_seed_from_cached_state(state: &CachedPoolState) -> Option<SolQuotedPoolSeed> {
    match state {
        CachedPoolState::Orca(s) => {
            let mint_a = s.token_mint_a.to_string();
            let mint_b = s.token_mint_b.to_string();
            let va = s.vault_a_balance?;
            let vb = s.vault_b_balance?;
            let (reserve_base, reserve_quote) =
                orca_sol_quoted_vault_reserves(&mint_a, &mint_b, va, vb)?;
            let token_mint = if mint_a == NATIVE_SOL_MINT {
                mint_b
            } else {
                mint_a
            };
            Some(SolQuotedPoolSeed {
                token_mint,
                reserve_base,
                reserve_quote,
                active_id: None,
                bin_step: None,
                dlmm_token_x_mint: None,
            })
        }
        CachedPoolState::RaydiumAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let cr = s.coin_reserve?;
            let pr = s.pc_reserve?;
            if quote == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: base,
                    reserve_base: cr,
                    reserve_quote: pr,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else if base == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: quote,
                    reserve_base: pr,
                    reserve_quote: cr,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else {
                None
            }
        }
        CachedPoolState::RaydiumCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            let r0 = s.reserve_0?;
            let r1 = s.reserve_1?;
            if t1 == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: t0,
                    reserve_base: r0,
                    reserve_quote: r1,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else if t0 == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: t1,
                    reserve_base: r1,
                    reserve_quote: r0,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else {
                None
            }
        }
        CachedPoolState::Meteora(s) => sol_quoted_seed_from_meteora_dlmm(s),
        CachedPoolState::MeteoraCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            if t1 == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: t0,
                    reserve_base: s.reserve_0,
                    reserve_quote: s.reserve_1,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else if t0 == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: t1,
                    reserve_base: s.reserve_1,
                    reserve_quote: s.reserve_0,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else {
                None
            }
        }
        CachedPoolState::PumpAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let br = s.base_reserve?;
            let qr = s.quote_reserve?;
            if quote == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: base,
                    reserve_base: br,
                    reserve_quote: qr,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else if base == NATIVE_SOL_MINT {
                Some(SolQuotedPoolSeed {
                    token_mint: quote,
                    reserve_base: qr,
                    reserve_quote: br,
                    active_id: None,
                    bin_step: None,
                    dlmm_token_x_mint: None,
                })
            } else {
                None
            }
        }
        CachedPoolState::PumpFun(s) => {
            let mint = s.token_mint.to_string();
            if mint == NATIVE_SOL_MINT {
                return None;
            }
            let token_r = s.virtual_token_reserves;
            let sol_r = s.virtual_sol_reserves;
            (token_r > 0 && sol_r > 0).then_some(SolQuotedPoolSeed {
                token_mint: mint,
                reserve_base: token_r,
                reserve_quote: sol_r,
                active_id: None,
                bin_step: None,
                dlmm_token_x_mint: None,
            })
        }
    }
}

fn sol_quoted_seed_from_meteora_dlmm(s: &MeteoraState) -> Option<SolQuotedPoolSeed> {
    let x = s.token_x_mint.to_string();
    let y = s.token_y_mint.to_string();
    let rx = s.reserve_x_balance?;
    let ry = s.reserve_y_balance?;
    if y == NATIVE_SOL_MINT {
        Some(SolQuotedPoolSeed {
            token_mint: x.clone(),
            reserve_base: rx,
            reserve_quote: ry,
            active_id: Some(s.active_id),
            bin_step: Some(s.bin_step),
            dlmm_token_x_mint: Some(x),
        })
    } else if x == NATIVE_SOL_MINT {
        Some(SolQuotedPoolSeed {
            token_mint: y.clone(),
            reserve_base: ry,
            reserve_quote: rx,
            active_id: Some(s.active_id),
            bin_step: Some(s.bin_step),
            dlmm_token_x_mint: Some(x),
        })
    } else {
        None
    }
}

fn cached_pool_inputs(
    state: &CachedPoolState,
    pool_address: &str,
    seed: &SolQuotedPoolSeed,
    slot: u64,
    updated_at: Instant,
    token_decimals: u8,
) -> (QuotePoolInput, QuoteVaultInput) {
    let pool_input = QuotePoolInput {
        pool_address: pool_address.to_string(),
        dex: state.dex_name().to_string(),
        token_mint: seed.token_mint.clone(),
        trade_price_buy: None,
        trade_price_sell: None,
        trade_updated_at: updated_at,
        has_reserve_data: seed.reserve_base > 0 && seed.reserve_quote > 0,
        token_decimals,
    };
    let vault_input = QuoteVaultInput {
        reserve_base: seed.reserve_base,
        reserve_quote: seed.reserve_quote,
        update_slot: slot,
        updated_at,
        active_id: seed.active_id,
        bin_step: seed.bin_step,
        dlmm_sol_is_x: seed.dlmm_token_x_mint.as_deref() == Some(NATIVE_SOL_MINT),
        dlmm_token_x_mint: seed.dlmm_token_x_mint.clone(),
    };
    (pool_input, vault_input)
}

fn hop_mints_match_sol_seed(seed: &SolQuotedPoolSeed, mint_in: &str, mint_out: &str) -> bool {
    (mint_in == NATIVE_SOL_MINT && mint_out == seed.token_mint)
        || (mint_out == NATIVE_SOL_MINT && mint_in == seed.token_mint)
}

fn cpmm_hop_from_cached_state(
    state: &CachedPoolState,
    pool_address: &str,
    mint_in: &str,
    mint_out: &str,
    amount_in: u64,
    slot: u64,
    updated_at: Instant,
) -> Option<PoolQuote> {
    if state.dex_name() == "meteora_dlmm" {
        return None;
    }
    let (reserve_in, reserve_out, dex) = match state {
        CachedPoolState::Orca(s) => {
            let a = s.token_mint_a.to_string();
            let b = s.token_mint_b.to_string();
            let va = s.vault_a_balance?;
            let vb = s.vault_b_balance?;
            if mint_in == a && mint_out == b {
                (va as u128, vb as u128, "orca")
            } else if mint_in == b && mint_out == a {
                (vb as u128, va as u128, "orca")
            } else {
                return None;
            }
        }
        CachedPoolState::RaydiumAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let cr = s.coin_reserve?;
            let pr = s.pc_reserve?;
            if mint_in == base && mint_out == quote {
                (cr as u128, pr as u128, "raydium")
            } else if mint_in == quote && mint_out == base {
                (pr as u128, cr as u128, "raydium")
            } else {
                return None;
            }
        }
        CachedPoolState::RaydiumCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            let r0 = s.reserve_0?;
            let r1 = s.reserve_1?;
            if mint_in == t0 && mint_out == t1 {
                (r0 as u128, r1 as u128, "raydium_cpmm")
            } else if mint_in == t1 && mint_out == t0 {
                (r1 as u128, r0 as u128, "raydium_cpmm")
            } else {
                return None;
            }
        }
        CachedPoolState::MeteoraCpmm(s) => {
            let t0 = s.token_0_mint.to_string();
            let t1 = s.token_1_mint.to_string();
            if mint_in == t0 && mint_out == t1 {
                (s.reserve_0 as u128, s.reserve_1 as u128, "meteora_cpmm")
            } else if mint_in == t1 && mint_out == t0 {
                (s.reserve_1 as u128, s.reserve_0 as u128, "meteora_cpmm")
            } else {
                return None;
            }
        }
        CachedPoolState::PumpAmm(s) => {
            let base = s.base_mint.to_string();
            let quote = s.quote_mint.to_string();
            let br = s.base_reserve?;
            let qr = s.quote_reserve?;
            if mint_in == base && mint_out == quote {
                (br as u128, qr as u128, "pump_amm")
            } else if mint_in == quote && mint_out == base {
                (qr as u128, br as u128, "pump_amm")
            } else {
                return None;
            }
        }
        CachedPoolState::PumpFun(s) => {
            let mint = s.token_mint.to_string();
            if mint_in == NATIVE_SOL_MINT && mint_out == mint {
                (
                    s.virtual_sol_reserves as u128,
                    s.virtual_token_reserves as u128,
                    "pumpfun",
                )
            } else if mint_in == mint && mint_out == NATIVE_SOL_MINT {
                (
                    s.virtual_token_reserves as u128,
                    s.virtual_sol_reserves as u128,
                    "pumpfun",
                )
            } else {
                return None;
            }
        }
        CachedPoolState::Meteora(_) => return None,
    };
    if !supports_cpmm(dex) && dex != "pumpfun" {
        return None;
    }
    let fee_bps = cpmm_fee_bps(dex);
    let amount_out = cpmm_amount_out(reserve_in, reserve_out, amount_in, fee_bps)?;
    Some(PoolQuote {
        pool_address: pool_address.to_string(),
        dex: dex.to_string(),
        kind: QuoteKind::ExecutableMarginal,
        side: quote_side_for_token_hop(mint_in, mint_out),
        as_of_slot: slot,
        as_of_ts: updated_at,
        fresh: true,
        state_fingerprint: 0,
        amount_in,
        amount_out,
    })
}

fn quote_side_for_token_hop(_mint_in: &str, mint_out: &str) -> QuoteSide {
    if mint_out == NATIVE_SOL_MINT {
        QuoteSide::Sell
    } else {
        QuoteSide::Buy
    }
}

/// Unified exact-in quote from SLAVE [`CachedPoolState`] (multi-hop + 2-hop SSOT).
///
/// SOL-quoted hops delegate to [`quote_exact_in_with_freshness`] (DLMM uses bin walker).
/// Token-token hops on CPMM DEXes use reserve math from the same module (no `quote_calculator` CP DLMM).
#[allow(clippy::too_many_arguments)]
pub fn quote_from_cached_pool(
    state: &CachedPoolState,
    pool_address: &str,
    mint_in: &str,
    mint_out: &str,
    amount_in: u64,
    dlmm_bins: Option<&DlmmBinArrays>,
    slot: u64,
    updated_at: Instant,
    token_decimals: u8,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    if amount_in == 0 {
        return None;
    }
    if mint_in == NATIVE_SOL_MINT || mint_out == NATIVE_SOL_MINT {
        let seed = sol_quoted_seed_from_cached_state(state)?;
        if !hop_mints_match_sol_seed(&seed, mint_in, mint_out) {
            return None;
        }
        let (pool_input, vault_input) =
            cached_pool_inputs(state, pool_address, &seed, slot, updated_at, token_decimals);
        return quote_exact_in_with_freshness(
            &pool_input,
            Some(&vault_input),
            dlmm_bins,
            mint_in,
            mint_out,
            amount_in,
            freshness,
        );
    }
    cpmm_hop_from_cached_state(
        state,
        pool_address,
        mint_in,
        mint_out,
        amount_in,
        slot,
        updated_at,
    )
}

/// Token decimals from cache state when mint-decimals map has no entry.
pub fn token_decimals_from_cached_state(state: &CachedPoolState, token_mint: &Pubkey) -> u8 {
    match state {
        CachedPoolState::RaydiumAmm(s) => {
            if s.base_mint == *token_mint && s.base_decimals > 0 {
                s.base_decimals
            } else if s.quote_mint == *token_mint && s.quote_decimals > 0 {
                s.quote_decimals
            } else {
                6
            }
        }
        CachedPoolState::MeteoraCpmm(s) => {
            if s.token_0_mint == *token_mint && s.mint_0_decimals > 0 {
                s.mint_0_decimals
            } else if s.token_1_mint == *token_mint && s.mint_1_decimals > 0 {
                s.mint_1_decimals
            } else {
                6
            }
        }
        _ => 6,
    }
}

/// Exact-in quote for SOL-quoted pools. Priority: ExecutableMarginal, then LastTradeMid.
pub fn quote_exact_in(
    pool: &QuotePoolInput,
    vault: Option<&QuoteVaultInput>,
    dlmm_bins: Option<&DlmmBinArrays>,
    mint_in: &str,
    mint_out: &str,
    amount_in: u64,
) -> Option<PoolQuote> {
    quote_exact_in_with_freshness(
        pool,
        vault,
        dlmm_bins,
        mint_in,
        mint_out,
        amount_in,
        &QuoteFreshnessConfig::default(),
    )
}

/// Exact-in quote with configurable freshness TTLs.
pub fn quote_exact_in_with_freshness(
    pool: &QuotePoolInput,
    vault: Option<&QuoteVaultInput>,
    dlmm_bins: Option<&DlmmBinArrays>,
    mint_in: &str,
    mint_out: &str,
    amount_in: u64,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    if amount_in == 0 {
        return None;
    }
    let side = quote_side_from_mints(&pool.token_mint, mint_in, mint_out)?;
    let now = Instant::now();

    if let Some(vault) = vault {
        if let Some(q) =
            executable_marginal_quote(pool, vault, dlmm_bins, side, amount_in, now, freshness)
        {
            return Some(q);
        }
    }

    last_trade_mid_quote(pool, side, amount_in, now, freshness)
}

/// SOL per whole token for screening (marginal probe > reserve mid > trade mid).
pub fn quote_sol_per_token_for_screening(
    pool: &QuotePoolInput,
    vault: Option<&QuoteVaultInput>,
    dlmm_bins: Option<&DlmmBinArrays>,
    side: QuoteSide,
) -> Option<Decimal> {
    let probe = match side {
        QuoteSide::Buy => DLMM_PROBE_SOL_LAMPORTS,
        QuoteSide::Sell => {
            let buy_quote = quote_exact_in(
                pool,
                vault,
                dlmm_bins,
                NATIVE_SOL_MINT,
                &pool.token_mint,
                DLMM_PROBE_SOL_LAMPORTS,
            )?;
            buy_quote.amount_out
        }
    };

    let quote = quote_exact_in(
        pool,
        vault,
        dlmm_bins,
        if side == QuoteSide::Buy {
            NATIVE_SOL_MINT
        } else {
            &pool.token_mint
        },
        if side == QuoteSide::Buy {
            &pool.token_mint
        } else {
            NATIVE_SOL_MINT
        },
        probe,
    )?;

    match side {
        QuoteSide::Buy => Some(trade_implied_sol_per_token(
            quote.amount_in,
            quote.amount_out,
            pool.token_decimals,
        )),
        QuoteSide::Sell => Some(trade_implied_sol_per_token(
            quote.amount_out,
            quote.amount_in,
            pool.token_decimals,
        )),
    }
}

/// Leg inputs for round-trip profit screening.
#[derive(Debug, Clone)]
pub struct RoundTripLeg<'a> {
    pub pool: &'a QuotePoolInput,
    pub vault: Option<&'a QuoteVaultInput>,
    pub dlmm_bins: Option<&'a DlmmBinArrays>,
}

/// Round-trip profit in lamports for a buy+sell pair (same QuoteKind required).
pub fn round_trip_profit_lamports(
    buy: &RoundTripLeg<'_>,
    sell: &RoundTripLeg<'_>,
    probe_sol_lamports: u64,
    tx_cost_lamports: u64,
) -> Option<i64> {
    round_trip_profit_lamports_with_freshness(
        buy,
        sell,
        probe_sol_lamports,
        tx_cost_lamports,
        &QuoteFreshnessConfig::default(),
    )
}

/// Round-trip profit with configurable freshness TTLs.
pub fn round_trip_profit_lamports_with_freshness(
    buy: &RoundTripLeg<'_>,
    sell: &RoundTripLeg<'_>,
    probe_sol_lamports: u64,
    tx_cost_lamports: u64,
    freshness: &QuoteFreshnessConfig,
) -> Option<i64> {
    let now = Instant::now();
    let buy_quote = quote_exact_in_with_freshness(
        buy.pool,
        buy.vault,
        buy.dlmm_bins,
        NATIVE_SOL_MINT,
        &buy.pool.token_mint,
        probe_sol_lamports,
        freshness,
    )?;
    if !is_quote_fresh(&buy_quote, freshness, buy.vault, now) {
        return None;
    }
    let sell_quote = quote_exact_in_with_freshness(
        sell.pool,
        sell.vault,
        sell.dlmm_bins,
        &sell.pool.token_mint,
        NATIVE_SOL_MINT,
        buy_quote.amount_out,
        freshness,
    )?;
    if !is_quote_fresh(&sell_quote, freshness, sell.vault, now) {
        return None;
    }
    if !quotes_pairable(&buy_quote, &sell_quote) {
        return None;
    }
    let profit = sell_quote.amount_out as i64 - probe_sol_lamports as i64 - tx_cost_lamports as i64;
    Some(profit)
}

/// Candidate pool for round-trip selection (I-ARB-5).
pub struct RoundTripPoolCandidate<'a> {
    pub pool: &'a QuotePoolInput,
    pub vault: Option<&'a QuoteVaultInput>,
    pub dlmm_bins: Option<&'a DlmmBinArrays>,
    pub dex: &'a str,
}

/// Selected buy/sell pools with executable quotes.
#[derive(Debug, Clone)]
pub struct RoundTripPoolSelection {
    pub buy_pool_address: String,
    pub buy_dex: String,
    pub sell_pool_address: String,
    pub sell_dex: String,
    pub buy_quote: PoolQuote,
    pub sell_quote: PoolQuote,
}

/// Drill-down when `quote_exact_in_with_freshness` returns `None` on a sell leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SellQuoteNoneDetailReason {
    StateStale,
    ReservesImplausible,
    DlmmActiveBinMissing,
    DlmmWalkerZero,
    DlmmMarginalReject,
    CpmmMathNone,
    UnsupportedDex,
    TradeFallbackNone,
    MintDirectionInvalid,
}

impl SellQuoteNoneDetailReason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::StateStale => "state_stale",
            Self::ReservesImplausible => "reserves_implausible",
            Self::DlmmActiveBinMissing => "dlmm_active_bin_missing",
            Self::DlmmWalkerZero => "dlmm_walker_zero",
            Self::DlmmMarginalReject => "dlmm_marginal_reject",
            Self::CpmmMathNone => "cpmm_math_none",
            Self::UnsupportedDex => "unsupported_dex",
            Self::TradeFallbackNone => "trade_fallback_none",
            Self::MintDirectionInvalid => "mint_direction_invalid",
        }
    }
}

/// Outcome of classifying a cross-DEX sell leg (metrics / snapshots).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrossDexSellFailure {
    MissingDlmmBins,
    MissingVault,
    QuoteNone(SellQuoteNoneDetailReason),
    NotFresh(QuoteNotFreshDiagnosis),
    ZeroOut,
}

impl CrossDexSellFailure {
    pub fn as_top_level_detail(self) -> NoCrossDexSellDetailReason {
        match self {
            Self::MissingDlmmBins => NoCrossDexSellDetailReason::SellMissingDlmmBins,
            Self::MissingVault => NoCrossDexSellDetailReason::SellMissingVault,
            Self::QuoteNone(SellQuoteNoneDetailReason::StateStale) => {
                NoCrossDexSellDetailReason::SellNotFresh
            }
            Self::QuoteNone(_) => NoCrossDexSellDetailReason::SellQuoteNone,
            Self::NotFresh(_) => NoCrossDexSellDetailReason::SellNotFresh,
            Self::ZeroOut => NoCrossDexSellDetailReason::SellZeroOut,
        }
    }

    pub fn sell_quote_none_subreason(self) -> Option<SellQuoteNoneDetailReason> {
        match self {
            Self::QuoteNone(reason) => Some(reason),
            _ => None,
        }
    }
}

fn diagnose_executable_marginal_none(
    pool: &QuotePoolInput,
    vault: &QuoteVaultInput,
    dlmm_bins: Option<&DlmmBinArrays>,
    amount_in: u64,
    now: Instant,
    freshness: &QuoteFreshnessConfig,
) -> Option<SellQuoteNoneDetailReason> {
    if !state_fresh(vault, now, freshness.state_ttl_ms) {
        return Some(SellQuoteNoneDetailReason::StateStale);
    }

    if pool.dex == "meteora_dlmm" {
        let active_id = vault.active_id?;
        let bin_step = vault.bin_step?;
        let bins = dlmm_bins?;
        let sol_is_x = vault_dlmm_sol_is_x(vault);
        let flat_bins = flatten_bins_for_walker(bins);
        if !active_bin_present(active_id, &flat_bins) {
            return Some(SellQuoteNoneDetailReason::DlmmActiveBinMissing);
        }

        let walker = walker_from_bins(active_id, bin_step, &flat_bins);
        let fee_bps = dlmm_fee_bps(bin_step);
        let amount_out = {
            let (out, _, _) = if sol_is_x {
                walker.quote_y_to_x(amount_in, fee_bps).ok()?
            } else {
                walker.quote_x_to_y(amount_in, fee_bps).ok()?
            };
            if out == 0 {
                return Some(SellQuoteNoneDetailReason::DlmmWalkerZero);
            }
            out
        };

        let marginal_price =
            trade_implied_sol_per_token(amount_out, amount_in, pool.token_decimals);
        if !is_plausible_sol_per_token_price(&pool.token_mint, marginal_price) {
            return Some(SellQuoteNoneDetailReason::DlmmMarginalReject);
        }
        return None;
    }

    if !supports_cpmm(&pool.dex) {
        return Some(SellQuoteNoneDetailReason::UnsupportedDex);
    }
    if !reserves_plausible(
        vault.reserve_base,
        vault.reserve_quote,
        pool.token_decimals,
        &pool.token_mint,
    ) {
        return Some(SellQuoteNoneDetailReason::ReservesImplausible);
    }

    let (reserve_in, reserve_out) = (vault.reserve_base as u128, vault.reserve_quote as u128);
    if cpmm_amount_out(reserve_in, reserve_out, amount_in, cpmm_fee_bps(&pool.dex)).is_none() {
        return Some(SellQuoteNoneDetailReason::CpmmMathNone);
    }
    None
}

/// Diagnose why a token→SOL sell quote returned `None` (no RPC; existing inputs only).
pub fn diagnose_sell_quote_none(
    pool: &QuotePoolInput,
    vault: Option<&QuoteVaultInput>,
    dlmm_bins: Option<&DlmmBinArrays>,
    token_amount_in: u64,
    freshness: &QuoteFreshnessConfig,
    now: Instant,
) -> SellQuoteNoneDetailReason {
    if quote_side_from_mints(&pool.token_mint, &pool.token_mint, NATIVE_SOL_MINT).is_none() {
        return SellQuoteNoneDetailReason::MintDirectionInvalid;
    }
    if token_amount_in == 0 {
        return SellQuoteNoneDetailReason::CpmmMathNone;
    }

    if let Some(vault) = vault {
        if let Some(reason) = diagnose_executable_marginal_none(
            pool,
            vault,
            dlmm_bins,
            token_amount_in,
            now,
            freshness,
        ) {
            return reason;
        }
    }

    SellQuoteNoneDetailReason::TradeFallbackNone
}

/// Max token input sellable via DLMM bins (sum of token-side liquidity from active bin).
fn dlmm_max_sell_token_in(
    active_id: i32,
    bin_step: u16,
    bin_arrays: &DlmmBinArrays,
    sol_is_x: bool,
) -> u64 {
    let flat_bins = flatten_bins_for_walker(bin_arrays);
    if !active_bin_present(active_id, &flat_bins) {
        return 0;
    }
    let walker = walker_from_bins(active_id, bin_step, &flat_bins);
    let fee_bps = dlmm_fee_bps(bin_step);
    let mut lo = 0u64;
    let mut hi = flat_bins
        .iter()
        .map(|(_, x, y)| if sol_is_x { *y } else { *x })
        .sum::<u64>()
        .saturating_add(1);
    if hi == 0 {
        return 0;
    }
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let ok = if sol_is_x {
            walker
                .quote_y_to_x(mid, fee_bps)
                .ok()
                .map(|(out, _, _)| out)
                .filter(|o| *o > 0)
                .is_some()
        } else {
            walker
                .quote_x_to_y(mid, fee_bps)
                .ok()
                .map(|(out, _, _)| out)
                .filter(|o| *o > 0)
                .is_some()
        };
        if ok {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

/// Cap sell token input to what the pool can quote (DLMM bin depth / CPMM reserves).
fn cap_sell_token_in(
    pool: &QuotePoolInput,
    vault: &QuoteVaultInput,
    dlmm_bins: Option<&DlmmBinArrays>,
    token_amount_in: u64,
) -> u64 {
    if pool.dex == "meteora_dlmm" {
        if let (Some(active_id), Some(bin_step), Some(bins)) =
            (vault.active_id, vault.bin_step, dlmm_bins)
        {
            let max_in =
                dlmm_max_sell_token_in(active_id, bin_step, bins, vault_dlmm_sol_is_x(vault));
            if max_in > 0 && token_amount_in > max_in {
                return max_in;
            }
        }
    } else if supports_cpmm(&pool.dex)
        && reserves_plausible(
            vault.reserve_base,
            vault.reserve_quote,
            pool.token_decimals,
            &pool.token_mint,
        )
    {
        let fee_bps = cpmm_fee_bps(&pool.dex);
        let reserve_in = vault.reserve_base as u128;
        let reserve_out = vault.reserve_quote as u128;
        let mut lo = 0u64;
        let mut hi = token_amount_in;
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if cpmm_amount_out(reserve_in, reserve_out, mid, fee_bps).is_some() {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        if lo > 0 && lo < token_amount_in {
            return lo;
        }
    }
    token_amount_in
}

/// Token→SOL sell quote for round-trip pairing (caps input to pool realizable depth).
pub fn quote_sell_round_trip(
    pool: &QuotePoolInput,
    vault: &QuoteVaultInput,
    dlmm_bins: Option<&DlmmBinArrays>,
    token_amount_in: u64,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    quote_sell_exact_in_with_cap(pool, vault, dlmm_bins, token_amount_in, freshness)
}

fn quote_sell_exact_in_with_cap(
    pool: &QuotePoolInput,
    vault: &QuoteVaultInput,
    dlmm_bins: Option<&DlmmBinArrays>,
    token_amount_in: u64,
    freshness: &QuoteFreshnessConfig,
) -> Option<PoolQuote> {
    let capped = cap_sell_token_in(pool, vault, dlmm_bins, token_amount_in);
    quote_exact_in_with_freshness(
        pool,
        Some(vault),
        dlmm_bins,
        &pool.token_mint,
        NATIVE_SOL_MINT,
        capped,
        freshness,
    )
}

/// Subreason when `select_round_trip_pools` cannot form a cross-DEX round trip.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundTripInsufficientSubreason {
    CandidatesLt2,
    NoFreshBuyQuote,
    NoCrossDexSell,
    SingleDexCandidates,
}

/// Drill-down reason when every cross-DEX sell leg failed after at least one valid buy quote.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NoCrossDexSellDetailReason {
    SellMissingVault,
    SellMissingDlmmBins,
    SellQuoteNone,
    SellNotFresh,
    SellZeroOut,
}

impl NoCrossDexSellDetailReason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::SellMissingVault => "sell_missing_vault",
            Self::SellMissingDlmmBins => "sell_missing_dlmm_bins",
            Self::SellQuoteNone => "sell_quote_none",
            Self::SellNotFresh => "sell_not_fresh",
            Self::SellZeroOut => "sell_zero_out",
        }
    }
}

/// Insufficient-pool outcome with optional `NoCrossDexSell` drill-down.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoundTripInsufficient {
    pub subreason: RoundTripInsufficientSubreason,
    pub no_cross_dex_sell_detail: Option<NoCrossDexSellDetailReason>,
    pub sell_quote_none_detail_counts: Option<HashMap<SellQuoteNoneDetailReason, usize>>,
    pub sell_not_fresh_detail_counts: Option<HashMap<QuoteNotFreshDiagnosis, usize>>,
    pub no_fresh_buy_quote_detail: Option<NoFreshBuyQuoteSubreason>,
    pub state_stale_age_bucket_counts: Option<HashMap<FreshnessAgeBucket, usize>>,
}

impl RoundTripInsufficient {
    pub fn new(subreason: RoundTripInsufficientSubreason) -> Self {
        Self {
            subreason,
            no_cross_dex_sell_detail: None,
            sell_quote_none_detail_counts: None,
            sell_not_fresh_detail_counts: None,
            no_fresh_buy_quote_detail: None,
            state_stale_age_bucket_counts: None,
        }
    }
}

/// Failure reason for `select_round_trip_pools`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoundTripSelectFailure {
    InsufficientPools(RoundTripInsufficient),
    IncompatibleQuoteKind,
    QuoteStale,
}

fn sol_per_token_from_buy_quote(quote: &PoolQuote, token_decimals: u8) -> Decimal {
    trade_implied_sol_per_token(quote.amount_in, quote.amount_out, token_decimals)
}

fn sol_per_token_from_sell_quote(quote: &PoolQuote, token_decimals: u8) -> Decimal {
    trade_implied_sol_per_token(quote.amount_out, quote.amount_in, token_decimals)
}

fn is_meteora_dlmm_dex(dex: &str) -> bool {
    dex == "meteora_dlmm"
}

/// Classify why a cross-DEX sell leg failed for drill-down metrics / snapshots.
pub fn classify_cross_dex_sell_failure(
    candidate: &RoundTripPoolCandidate<'_>,
    token_amount_in: u64,
    freshness: &QuoteFreshnessConfig,
    now: Instant,
    token_decimals: u8,
) -> Option<CrossDexSellFailure> {
    if is_meteora_dlmm_dex(candidate.dex) && candidate.dlmm_bins.is_none() {
        return Some(CrossDexSellFailure::MissingDlmmBins);
    }
    if candidate.vault.is_none() {
        return Some(CrossDexSellFailure::MissingVault);
    }
    let sell_quote = quote_sell_exact_in_with_cap(
        candidate.pool,
        candidate.vault.expect("vault checked"),
        candidate.dlmm_bins,
        token_amount_in,
        freshness,
    );
    let Some(sell_quote) = sell_quote else {
        let sub = diagnose_sell_quote_none(
            candidate.pool,
            candidate.vault,
            candidate.dlmm_bins,
            token_amount_in,
            freshness,
            now,
        );
        return Some(CrossDexSellFailure::QuoteNone(sub));
    };
    if let Some(diagnosis) = diagnose_quote_not_fresh(&sell_quote, freshness, candidate.vault, now)
    {
        return Some(CrossDexSellFailure::NotFresh(diagnosis));
    }
    let sol_per_token = sol_per_token_from_sell_quote(&sell_quote, token_decimals);
    if sol_per_token <= Decimal::ZERO {
        return Some(CrossDexSellFailure::ZeroOut);
    }
    None
}

fn dominant_no_cross_dex_sell_detail(
    counts: &std::collections::HashMap<NoCrossDexSellDetailReason, usize>,
) -> Option<NoCrossDexSellDetailReason> {
    counts
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(reason, _)| *reason)
}

/// I-ARB-5 evolution: enumerate valid cross-DEX (buy, sell) pairs and pick best round-trip.
#[allow(clippy::result_large_err)]
pub fn select_round_trip_pools(
    candidates: &[RoundTripPoolCandidate<'_>],
    probe_lamports: u64,
    freshness: &QuoteFreshnessConfig,
) -> Result<RoundTripPoolSelection, RoundTripSelectFailure> {
    if candidates.len() < 2 {
        return Err(RoundTripSelectFailure::InsufficientPools(
            RoundTripInsufficient::new(RoundTripInsufficientSubreason::CandidatesLt2),
        ));
    }

    let now = Instant::now();
    let token_decimals = candidates
        .first()
        .map(|c| c.pool.token_decimals)
        .unwrap_or(6);

    struct BuyCandidate<'a> {
        candidate: &'a RoundTripPoolCandidate<'a>,
        quote: PoolQuote,
        sol_per_token: Decimal,
    }

    struct SellCandidate<'a> {
        candidate: &'a RoundTripPoolCandidate<'a>,
        quote: PoolQuote,
    }

    let mut valid_buys: Vec<BuyCandidate<'_>> = Vec::new();
    for candidate in candidates {
        let buy_quote = quote_exact_in_with_freshness(
            candidate.pool,
            candidate.vault,
            candidate.dlmm_bins,
            NATIVE_SOL_MINT,
            &candidate.pool.token_mint,
            probe_lamports,
            freshness,
        );
        let Some(buy_quote) = buy_quote else {
            continue;
        };
        if !is_quote_fresh(&buy_quote, freshness, candidate.vault, now) {
            continue;
        }
        let sol_per_token = sol_per_token_from_buy_quote(&buy_quote, token_decimals);
        if sol_per_token <= Decimal::ZERO {
            continue;
        }
        valid_buys.push(BuyCandidate {
            candidate,
            quote: buy_quote,
            sol_per_token,
        });
    }

    if valid_buys.is_empty() {
        let mut buy_fail_counts: HashMap<NoFreshBuyQuoteSubreason, usize> = HashMap::new();
        for candidate in candidates {
            let sub = diagnose_no_fresh_buy_quote(
                candidate.pool,
                candidate.vault,
                candidate.dlmm_bins,
                probe_lamports,
                freshness,
                now,
            );
            *buy_fail_counts.entry(sub).or_default() += 1;
        }
        let dominant_buy_detail = buy_fail_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(reason, _)| *reason);
        return Err(RoundTripSelectFailure::InsufficientPools(
            RoundTripInsufficient {
                subreason: RoundTripInsufficientSubreason::NoFreshBuyQuote,
                no_cross_dex_sell_detail: None,
                sell_quote_none_detail_counts: None,
                sell_not_fresh_detail_counts: None,
                no_fresh_buy_quote_detail: dominant_buy_detail,
                state_stale_age_bucket_counts: None,
            },
        ));
    }

    let distinct_dexes: std::collections::HashSet<&str> =
        candidates.iter().map(|c| c.dex).collect();
    if distinct_dexes.len() < 2 {
        return Err(RoundTripSelectFailure::InsufficientPools(
            RoundTripInsufficient::new(RoundTripInsufficientSubreason::SingleDexCandidates),
        ));
    }

    let mut best_pair: Option<(BuyCandidate<'_>, SellCandidate<'_>)> = None;
    let mut best_round_trip_profit: i64 = i64::MIN;
    let mut saw_incompatible_kind = false;
    let mut sell_fail_counts: std::collections::HashMap<NoCrossDexSellDetailReason, usize> =
        std::collections::HashMap::new();
    let mut sell_quote_none_detail_counts: HashMap<SellQuoteNoneDetailReason, usize> =
        HashMap::new();
    let mut sell_not_fresh_detail_counts: HashMap<QuoteNotFreshDiagnosis, usize> = HashMap::new();
    let mut state_stale_age_bucket_counts: HashMap<FreshnessAgeBucket, usize> = HashMap::new();

    for buy in &valid_buys {
        for sell_candidate in candidates {
            if sell_candidate.dex == buy.candidate.dex {
                continue;
            }

            if is_meteora_dlmm_dex(sell_candidate.dex) && sell_candidate.dlmm_bins.is_none() {
                *sell_fail_counts
                    .entry(NoCrossDexSellDetailReason::SellMissingDlmmBins)
                    .or_default() += 1;
                continue;
            }
            let Some(sell_vault) = sell_candidate.vault else {
                *sell_fail_counts
                    .entry(NoCrossDexSellDetailReason::SellMissingVault)
                    .or_default() += 1;
                continue;
            };

            let sell_quote = quote_sell_exact_in_with_cap(
                sell_candidate.pool,
                sell_vault,
                sell_candidate.dlmm_bins,
                buy.quote.amount_out,
                freshness,
            );
            let Some(sell_quote) = sell_quote else {
                let sub = diagnose_sell_quote_none(
                    sell_candidate.pool,
                    sell_candidate.vault,
                    sell_candidate.dlmm_bins,
                    buy.quote.amount_out,
                    freshness,
                    now,
                );
                *sell_quote_none_detail_counts.entry(sub).or_default() += 1;
                if sub == SellQuoteNoneDetailReason::StateStale {
                    if let Some(vault) = sell_candidate.vault {
                        let bucket = freshness_age_bucket(vault_state_age_ms(vault, now));
                        *state_stale_age_bucket_counts.entry(bucket).or_default() += 1;
                        // C1h3 H5: same classify path as `sell_not_fresh` top-level counter.
                        let diagnosis = QuoteNotFreshDiagnosis {
                            kind: QuoteNotFreshKind::ExecutableMarginal,
                            cause: QuoteNotFreshCause::AgeExceeded,
                        };
                        *sell_not_fresh_detail_counts.entry(diagnosis).or_default() += 1;
                    }
                }
                let top = if sub == SellQuoteNoneDetailReason::StateStale {
                    NoCrossDexSellDetailReason::SellNotFresh
                } else {
                    NoCrossDexSellDetailReason::SellQuoteNone
                };
                *sell_fail_counts.entry(top).or_default() += 1;
                continue;
            };
            if let Some(diagnosis) =
                diagnose_quote_not_fresh(&sell_quote, freshness, sell_candidate.vault, now)
            {
                *sell_not_fresh_detail_counts.entry(diagnosis).or_default() += 1;
                *sell_fail_counts
                    .entry(NoCrossDexSellDetailReason::SellNotFresh)
                    .or_default() += 1;
                continue;
            }
            if !quotes_pairable(&buy.quote, &sell_quote) {
                saw_incompatible_kind = true;
                continue;
            }
            let sol_per_token = sol_per_token_from_sell_quote(&sell_quote, token_decimals);
            if sol_per_token <= Decimal::ZERO {
                *sell_fail_counts
                    .entry(NoCrossDexSellDetailReason::SellZeroOut)
                    .or_default() += 1;
                continue;
            }

            let round_trip_profit = sell_quote.amount_out as i64 - probe_lamports as i64;
            if round_trip_profit > best_round_trip_profit {
                best_round_trip_profit = round_trip_profit;
                best_pair = Some((
                    BuyCandidate {
                        candidate: buy.candidate,
                        quote: buy.quote.clone(),
                        sol_per_token: buy.sol_per_token,
                    },
                    SellCandidate {
                        candidate: sell_candidate,
                        quote: sell_quote,
                    },
                ));
            }
        }
    }

    let Some((best_buy, best_sell)) = best_pair else {
        if saw_incompatible_kind {
            return Err(RoundTripSelectFailure::IncompatibleQuoteKind);
        }
        return Err(RoundTripSelectFailure::InsufficientPools(
            RoundTripInsufficient {
                subreason: RoundTripInsufficientSubreason::NoCrossDexSell,
                no_cross_dex_sell_detail: dominant_no_cross_dex_sell_detail(&sell_fail_counts),
                sell_quote_none_detail_counts: (!sell_quote_none_detail_counts.is_empty())
                    .then_some(sell_quote_none_detail_counts),
                sell_not_fresh_detail_counts: (!sell_not_fresh_detail_counts.is_empty())
                    .then_some(sell_not_fresh_detail_counts),
                no_fresh_buy_quote_detail: None,
                state_stale_age_bucket_counts: (!state_stale_age_bucket_counts.is_empty())
                    .then_some(state_stale_age_bucket_counts),
            },
        ));
    };

    Ok(RoundTripPoolSelection {
        buy_pool_address: best_buy.candidate.pool.pool_address.clone(),
        buy_dex: best_buy.candidate.dex.to_string(),
        sell_pool_address: best_sell.candidate.pool.pool_address.clone(),
        sell_dex: best_sell.candidate.dex.to_string(),
        buy_quote: best_buy.quote,
        sell_quote: best_sell.quote,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_pool(dex: &str, address: &str) -> QuotePoolInput {
        QuotePoolInput {
            pool_address: address.to_string(),
            dex: dex.to_string(),
            token_mint: "TokenMint11111111111111111111111111111111".to_string(),
            trade_price_buy: None,
            trade_price_sell: None,
            trade_updated_at: Instant::now(),
            has_reserve_data: true,
            token_decimals: 6,
        }
    }

    fn sample_vault(token_reserve: u64, sol_reserve: u64) -> QuoteVaultInput {
        QuoteVaultInput {
            reserve_base: token_reserve,
            reserve_quote: sol_reserve,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: None,
            bin_step: None,
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: None,
        }
    }

    #[test]
    fn pump_amm_cpmm_round_numbers() {
        let pool = sample_pool("pump_amm", "pumpPool");
        let vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        let sol_in = 100_000_000u64;
        let quote = quote_exact_in(
            &pool,
            Some(&vault),
            None,
            NATIVE_SOL_MINT,
            &pool.token_mint,
            sol_in,
        )
        .expect("cpmm quote");
        assert_eq!(quote.kind, QuoteKind::ExecutableMarginal);
        assert_eq!(quote.side, QuoteSide::Buy);
        assert!(quote.amount_out > 0);
        assert!(quote.amount_out < vault.reserve_base);
    }

    #[test]
    fn quotes_pairable_same_kind_only() {
        let q_exec = PoolQuote {
            pool_address: "a".into(),
            dex: "orca".into(),
            kind: QuoteKind::ExecutableMarginal,
            side: QuoteSide::Buy,
            as_of_slot: 1,
            as_of_ts: Instant::now(),
            fresh: true,
            state_fingerprint: 42,
            amount_in: 1,
            amount_out: 2,
        };
        let q_trade = PoolQuote {
            kind: QuoteKind::LastTradeMid,
            ..q_exec.clone()
        };
        assert!(quotes_pairable(&q_exec, &q_exec));
        assert!(!quotes_pairable(&q_exec, &q_trade));
    }

    #[test]
    fn stale_trade_falls_back_to_none_without_reserves() {
        let mut pool = sample_pool("pump_amm", "stale");
        pool.trade_price_buy = Some(Decimal::new(1, 3));
        pool.trade_price_sell = Some(Decimal::new(1, 3));
        pool.trade_updated_at = Instant::now() - Duration::from_secs(60);
        let quote = quote_exact_in(
            &pool,
            None,
            None,
            NATIVE_SOL_MINT,
            &pool.token_mint,
            DLMM_PROBE_SOL_LAMPORTS,
        );
        assert!(quote.is_none());
    }

    #[test]
    fn dlmm_marginal_vs_reserve_mid_divergence_bounded() {
        let active_id = 0i32;
        let bin_step = 100u16;
        let token_amount = 1_000_000_000_000u64;
        let sol_amount = 1_000_000_000u64;
        let array_index = active_id as i64 / 70;
        let mut bins: DlmmBinArrays = HashMap::new();
        bins.insert(
            array_index,
            vec![BinData {
                offset: 0,
                amount_x: token_amount,
                amount_y: sol_amount,
            }],
        );
        let pool = sample_pool("meteora_dlmm", "dlmm");
        let vault = QuoteVaultInput {
            reserve_base: token_amount,
            reserve_quote: sol_amount,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: Some(active_id),
            bin_step: Some(bin_step),
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: Some(pool.token_mint.clone()),
        };
        let reserve_mid =
            reserve_mid_sol_per_token(token_amount, sol_amount, pool.token_decimals).unwrap();
        let marginal_buy =
            quote_sol_per_token_for_screening(&pool, Some(&vault), Some(&bins), QuoteSide::Buy)
                .expect("marginal buy");
        let ratio = if marginal_buy > reserve_mid {
            marginal_buy / reserve_mid
        } else {
            reserve_mid / marginal_buy
        };
        assert!(
            ratio <= Decimal::from(10),
            "marginal vs reserve mid divergence too large: {ratio}"
        );
    }

    #[test]
    fn is_quote_fresh_trade_ttl_expires() {
        let config = QuoteFreshnessConfig::default();
        let quote = PoolQuote {
            pool_address: "p".into(),
            dex: "orca".into(),
            kind: QuoteKind::LastTradeMid,
            side: QuoteSide::Buy,
            as_of_slot: 0,
            as_of_ts: Instant::now() - Duration::from_secs(60),
            fresh: false,
            state_fingerprint: 0,
            amount_in: 1,
            amount_out: 2,
        };
        assert!(!is_quote_fresh(&quote, &config, None, Instant::now()));
    }

    #[test]
    fn is_quote_fresh_state_fingerprint_mismatch() {
        let config = QuoteFreshnessConfig::default();
        let vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        let fingerprint = state_fingerprint(&vault);
        let quote = PoolQuote {
            pool_address: "p".into(),
            dex: "orca".into(),
            kind: QuoteKind::ExecutableMarginal,
            side: QuoteSide::Buy,
            as_of_slot: 1,
            as_of_ts: Instant::now(),
            fresh: true,
            state_fingerprint: fingerprint,
            amount_in: 1,
            amount_out: 2,
        };
        let mut changed_vault = vault.clone();
        changed_vault.reserve_base += 1;
        assert!(!is_quote_fresh(
            &quote,
            &config,
            Some(&changed_vault),
            Instant::now()
        ));
    }

    #[test]
    fn dlmm_cached_pool_quote_uses_bin_walker_not_reserve_ratio() {
        let active_id = 0i32;
        let bin_step = 100u16;
        let token_amount = 500_000_000_000u64;
        let sol_amount = 2_000_000_000u64;
        let array_index = active_id as i64 / 70;
        let mut bins: DlmmBinArrays = HashMap::new();
        bins.insert(
            array_index,
            vec![BinData {
                offset: 0,
                amount_x: token_amount,
                amount_y: sol_amount,
            }],
        );
        let token_mint = Pubkey::new_unique();
        let state = CachedPoolState::Meteora(MeteoraState {
            token_x_mint: token_mint,
            token_y_mint: Pubkey::from_str(NATIVE_SOL_MINT).unwrap(),
            reserve_x: Pubkey::new_unique(),
            reserve_y: Pubkey::new_unique(),
            active_id,
            bin_step,
            reserve_x_balance: Some(1_000_000_000_000),
            reserve_y_balance: Some(500_000_000),
        });
        let sol_in = DLMM_PROBE_SOL_LAMPORTS;
        let cp_out = cpmm_amount_out(
            sol_amount as u128,
            token_amount as u128,
            sol_in,
            cpmm_fee_bps("meteora_dlmm"),
        )
        .expect("cp approx");
        let bin_out = dlmm_token_output_from_bins(active_id, bin_step, sol_in, &bins, false)
            .expect("bin walker");
        assert_ne!(
            cp_out, bin_out,
            "test requires bin walker and reserve CP to diverge"
        );
        let quote = quote_from_cached_pool(
            &state,
            "dlmmPool",
            NATIVE_SOL_MINT,
            &token_mint.to_string(),
            sol_in,
            Some(&bins),
            1,
            Instant::now(),
            6,
            &QuoteFreshnessConfig::default(),
        )
        .expect("dlmm pool quote");
        assert_eq!(quote.kind, QuoteKind::ExecutableMarginal);
        assert_eq!(quote.amount_out, bin_out);
        assert_ne!(quote.amount_out, cp_out);
    }

    #[test]
    fn select_round_trip_pools_picks_cross_dex_pair() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(1_000_000_000_000, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let selection = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .expect("cross-dex selection");
        assert_eq!(selection.buy_pool_address, "poolA");
        assert_eq!(selection.sell_pool_address, "poolB");
        assert_eq!(selection.buy_quote.kind, selection.sell_quote.kind);
    }

    #[test]
    fn select_round_trip_pools_one_candidate_is_candidates_lt_2() {
        let pool = sample_pool("orca", "poolOnly");
        let vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        let candidates = [RoundTripPoolCandidate {
            pool: &pool,
            vault: Some(&vault),
            dlmm_bins: None,
            dex: "orca",
        }];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            RoundTripSelectFailure::InsufficientPools(RoundTripInsufficient::new(
                RoundTripInsufficientSubreason::CandidatesLt2
            ))
        );
    }

    #[test]
    fn select_round_trip_pools_same_dex_quotable_is_single_dex_candidates() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("orca", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(1_000_000_000_000, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "orca",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            RoundTripSelectFailure::InsufficientPools(RoundTripInsufficient::new(
                RoundTripInsufficientSubreason::SingleDexCandidates
            ))
        );
    }

    #[test]
    fn select_round_trip_pools_cross_dex_missing_sell_is_no_cross_dex_sell() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(0, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(
            err,
            RoundTripSelectFailure::InsufficientPools(RoundTripInsufficient {
                subreason: RoundTripInsufficientSubreason::NoCrossDexSell,
                no_cross_dex_sell_detail: Some(NoCrossDexSellDetailReason::SellQuoteNone),
                ..
            })
        ));
        if let RoundTripSelectFailure::InsufficientPools(insufficient) = err {
            let counts = insufficient
                .sell_quote_none_detail_counts
                .expect("sell quote none subcounts");
            assert_eq!(
                counts.get(&SellQuoteNoneDetailReason::ReservesImplausible),
                Some(&1)
            );
        }
    }

    #[test]
    fn select_round_trip_pools_pair_aware_matches_brute_force_best_round_trip() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(1_000, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let freshness = QuoteFreshnessConfig::default();
        let probe = DLMM_PROBE_SOL_LAMPORTS;
        let now = Instant::now();

        let mut brute_best_profit = i64::MIN;
        let mut brute_best: Option<(String, String)> = None;
        for buy in &candidates {
            let buy_quote = quote_exact_in_with_freshness(
                buy.pool,
                buy.vault,
                buy.dlmm_bins,
                NATIVE_SOL_MINT,
                &buy.pool.token_mint,
                probe,
                &freshness,
            );
            let Some(buy_quote) = buy_quote else { continue };
            if !is_quote_fresh(&buy_quote, &freshness, buy.vault, now) {
                continue;
            }
            for sell in &candidates {
                if sell.dex == buy.dex {
                    continue;
                }
                let sell_quote = quote_exact_in_with_freshness(
                    sell.pool,
                    sell.vault,
                    sell.dlmm_bins,
                    &sell.pool.token_mint,
                    NATIVE_SOL_MINT,
                    buy_quote.amount_out,
                    &freshness,
                );
                let Some(sell_quote) = sell_quote else {
                    continue;
                };
                if !is_quote_fresh(&sell_quote, &freshness, sell.vault, now) {
                    continue;
                }
                if !quotes_pairable(&buy_quote, &sell_quote) {
                    continue;
                }
                let profit = sell_quote.amount_out as i64 - probe as i64;
                if profit > brute_best_profit {
                    brute_best_profit = profit;
                    brute_best = Some((
                        buy.pool.pool_address.clone(),
                        sell.pool.pool_address.clone(),
                    ));
                }
            }
        }

        let selection =
            select_round_trip_pools(&candidates, probe, &freshness).expect("pair-aware selection");
        let expected = brute_best.expect("brute-force must find a pair");
        assert_eq!(selection.buy_pool_address, expected.0);
        assert_eq!(selection.sell_pool_address, expected.1);
        assert!(
            selection.sell_quote.amount_out as i64 - probe as i64 == brute_best_profit,
            "selection must maximize round-trip profit"
        );
    }

    #[test]
    fn select_round_trip_pools_cross_dex_missing_vault_records_detail() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: None,
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            RoundTripSelectFailure::InsufficientPools(RoundTripInsufficient {
                subreason: RoundTripInsufficientSubreason::NoCrossDexSell,
                no_cross_dex_sell_detail: Some(NoCrossDexSellDetailReason::SellMissingVault),
                sell_quote_none_detail_counts: None,
                sell_not_fresh_detail_counts: None,
                no_fresh_buy_quote_detail: None,
                state_stale_age_bucket_counts: None,
            })
        );
    }

    #[test]
    fn select_round_trip_pools_dlmm_missing_bins_records_detail() {
        let pool_a = sample_pool("pump_amm", "poolA");
        let pool_b = sample_pool("meteora_dlmm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(1_000_000_000_000, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "pump_amm",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "meteora_dlmm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            RoundTripSelectFailure::InsufficientPools(RoundTripInsufficient {
                subreason: RoundTripInsufficientSubreason::NoCrossDexSell,
                no_cross_dex_sell_detail: Some(NoCrossDexSellDetailReason::SellMissingDlmmBins),
                sell_quote_none_detail_counts: None,
                sell_not_fresh_detail_counts: None,
                no_fresh_buy_quote_detail: None,
                state_stale_age_bucket_counts: None,
            })
        );
    }

    #[test]
    fn classify_cross_dex_sell_failure_missing_vault() {
        let pool = sample_pool("orca", "poolO");
        let candidate = RoundTripPoolCandidate {
            pool: &pool,
            vault: None,
            dlmm_bins: None,
            dex: "orca",
        };
        let reason = classify_cross_dex_sell_failure(
            &candidate,
            1_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
            6,
        );
        assert_eq!(reason, Some(CrossDexSellFailure::MissingVault));
    }

    #[test]
    fn select_round_trip_pools_incompatible_kinds_only() {
        use rust_decimal::Decimal;
        use std::str::FromStr;

        let pool_a = sample_pool("orca", "poolA");
        let mut pool_b = sample_pool("pump_amm", "poolB");
        let trade_price = Decimal::from_str("0.000001").unwrap();
        pool_b.trade_price_buy = Some(trade_price);
        pool_b.trade_price_sell = Some(trade_price);
        pool_b.trade_updated_at = Instant::now();
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: None,
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        assert_eq!(err, RoundTripSelectFailure::IncompatibleQuoteKind);
    }

    #[test]
    fn diagnose_sell_quote_none_state_stale() {
        let pool = sample_pool("orca", "stale");
        let mut vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        vault.updated_at = Instant::now() - Duration::from_secs(300);
        let reason = diagnose_sell_quote_none(
            &pool,
            Some(&vault),
            None,
            1_000_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
        );
        assert_eq!(reason, SellQuoteNoneDetailReason::StateStale);
    }

    #[test]
    fn diagnose_sell_quote_none_reserves_implausible() {
        let pool = sample_pool("pump_amm", "bad");
        let vault = sample_vault(0, 1_000_000_000);
        let reason = diagnose_sell_quote_none(
            &pool,
            Some(&vault),
            None,
            1_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
        );
        assert_eq!(reason, SellQuoteNoneDetailReason::ReservesImplausible);
    }

    #[test]
    fn diagnose_sell_quote_none_trade_fallback_none() {
        let pool = sample_pool("orca", "noTrade");
        let reason = diagnose_sell_quote_none(
            &pool,
            None,
            None,
            1_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
        );
        assert_eq!(reason, SellQuoteNoneDetailReason::TradeFallbackNone);
    }

    #[test]
    fn diagnose_sell_quote_none_dlmm_active_bin_missing() {
        let pool = sample_pool("meteora_dlmm", "dlmm");
        let vault = QuoteVaultInput {
            reserve_base: 1_000_000_000_000,
            reserve_quote: 1_000_000_000,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: Some(100),
            bin_step: Some(100),
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: Some(pool.token_mint.clone()),
        };
        let bins: DlmmBinArrays = HashMap::new();
        let reason = diagnose_sell_quote_none(
            &pool,
            Some(&vault),
            Some(&bins),
            1_000_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
        );
        assert_eq!(reason, SellQuoteNoneDetailReason::DlmmActiveBinMissing);
    }

    #[test]
    fn classify_cross_dex_sell_failure_ok_returns_none() {
        let pool = sample_pool("orca", "poolO");
        let vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        let candidate = RoundTripPoolCandidate {
            pool: &pool,
            vault: Some(&vault),
            dlmm_bins: None,
            dex: "orca",
        };
        let failure = classify_cross_dex_sell_failure(
            &candidate,
            1_000_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
            6,
        );
        assert!(failure.is_none());
    }

    #[test]
    fn dlmm_sell_quote_with_zero_liquidity_active_bin_uses_adjacent_bins() {
        let active_id = 5i32;
        let bin_step = 100u16;
        let token_amount = 1_000_000_000u64;
        let sol_amount = 500_000_000u64;
        let array_index = 0i64;
        let mut bins: DlmmBinArrays = HashMap::new();
        bins.insert(
            array_index,
            vec![
                BinData {
                    offset: active_id as u8,
                    amount_x: 0,
                    amount_y: 0,
                },
                BinData {
                    offset: 4,
                    amount_x: token_amount,
                    amount_y: sol_amount,
                },
            ],
        );
        let pool = sample_pool("meteora_dlmm", "dlmm");
        let vault = QuoteVaultInput {
            reserve_base: token_amount,
            reserve_quote: sol_amount,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: Some(active_id),
            bin_step: Some(bin_step),
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: Some(pool.token_mint.clone()),
        };
        let failure = classify_cross_dex_sell_failure(
            &RoundTripPoolCandidate {
                pool: &pool,
                vault: Some(&vault),
                dlmm_bins: Some(&bins),
                dex: "meteora_dlmm",
            },
            1_000_000,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
            6,
        );
        assert!(
            failure.is_none()
                || failure
                    != Some(CrossDexSellFailure::QuoteNone(
                        SellQuoteNoneDetailReason::DlmmActiveBinMissing
                    )),
            "zero-liquidity active bin present must not yield DlmmActiveBinMissing: {:?}",
            failure
        );
    }

    #[test]
    fn dlmm_sell_large_token_amount_succeeds_without_marginal_probe_gate() {
        let active_id = 0i32;
        let bin_step = 100u16;
        let token_amount = 1_000_000_000_000u64;
        let sol_amount = 1_000_000_000u64;
        let array_index = active_id as i64 / 70;
        let mut bins: DlmmBinArrays = HashMap::new();
        bins.insert(
            array_index,
            vec![BinData {
                offset: 0,
                amount_x: token_amount,
                amount_y: sol_amount,
            }],
        );
        let pool = sample_pool("meteora_dlmm", "dlmm");
        let vault = QuoteVaultInput {
            reserve_base: token_amount,
            reserve_quote: sol_amount,
            update_slot: 1,
            updated_at: Instant::now(),
            active_id: Some(active_id),
            bin_step: Some(bin_step),
            dlmm_sol_is_x: false,
            dlmm_token_x_mint: Some(pool.token_mint.clone()),
        };
        let buy_quote = quote_exact_in(
            &pool,
            Some(&vault),
            Some(&bins),
            NATIVE_SOL_MINT,
            &pool.token_mint,
            DLMM_PROBE_SOL_LAMPORTS,
        )
        .expect("buy");
        let sell_quote = quote_exact_in(
            &pool,
            Some(&vault),
            Some(&bins),
            &pool.token_mint,
            NATIVE_SOL_MINT,
            buy_quote.amount_out,
        );
        assert!(
            sell_quote.is_some(),
            "large DLMM sell must not fail marginal probe gate"
        );
    }

    #[test]
    fn diagnose_quote_not_fresh_trade_age_exceeded() {
        let config = QuoteFreshnessConfig::default();
        let quote = PoolQuote {
            pool_address: "p".into(),
            dex: "orca".into(),
            kind: QuoteKind::LastTradeMid,
            side: QuoteSide::Sell,
            as_of_slot: 0,
            as_of_ts: Instant::now() - Duration::from_secs(60),
            fresh: false,
            state_fingerprint: 0,
            amount_in: 1,
            amount_out: 2,
        };
        let diagnosis = diagnose_quote_not_fresh(&quote, &config, None, Instant::now())
            .expect("stale trade quote");
        assert_eq!(diagnosis.kind, QuoteNotFreshKind::LastTradeMid);
        assert_eq!(diagnosis.cause, QuoteNotFreshCause::AgeExceeded);
    }

    #[test]
    fn diagnose_quote_not_fresh_executable_fingerprint_mismatch() {
        let config = QuoteFreshnessConfig::default();
        let vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        let fingerprint = state_fingerprint(&vault);
        let quote = PoolQuote {
            pool_address: "p".into(),
            dex: "orca".into(),
            kind: QuoteKind::ExecutableMarginal,
            side: QuoteSide::Sell,
            as_of_slot: 1,
            as_of_ts: Instant::now(),
            fresh: true,
            state_fingerprint: fingerprint,
            amount_in: 1,
            amount_out: 2,
        };
        let mut changed_vault = vault.clone();
        changed_vault.reserve_base += 1;
        let diagnosis =
            diagnose_quote_not_fresh(&quote, &config, Some(&changed_vault), Instant::now())
                .expect("fingerprint mismatch");
        assert_eq!(diagnosis.kind, QuoteNotFreshKind::ExecutableMarginal);
        assert_eq!(diagnosis.cause, QuoteNotFreshCause::FingerprintMismatch);
    }

    #[test]
    fn freshness_age_bucket_boundaries() {
        assert_eq!(freshness_age_bucket(15_000), FreshnessAgeBucket::Le30s);
        assert_eq!(freshness_age_bucket(60_000), FreshnessAgeBucket::Le120s);
        assert_eq!(freshness_age_bucket(200_000), FreshnessAgeBucket::Le300s);
        assert_eq!(freshness_age_bucket(400_000), FreshnessAgeBucket::Gt300s);
    }

    #[test]
    fn diagnose_no_fresh_buy_quote_state_stale_vault() {
        let pool = sample_pool("orca", "staleBuy");
        let mut vault = sample_vault(1_000_000_000_000, 1_000_000_000);
        vault.updated_at = Instant::now() - Duration::from_secs(300);
        let sub = diagnose_no_fresh_buy_quote(
            &pool,
            Some(&vault),
            None,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
            Instant::now(),
        );
        assert_eq!(sub, NoFreshBuyQuoteSubreason::StateStale);
    }

    #[test]
    fn select_round_trip_pools_no_fresh_buy_records_subreason() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let mut vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        vault_a.updated_at = Instant::now() - Duration::from_secs(300);
        let mut vault_b = sample_vault(1_000_000_000_000, 1_100_000_000);
        vault_b.updated_at = Instant::now() - Duration::from_secs(300);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        if let RoundTripSelectFailure::InsufficientPools(insufficient) = err {
            assert_eq!(
                insufficient.subreason,
                RoundTripInsufficientSubreason::NoFreshBuyQuote
            );
            assert_eq!(
                insufficient.no_fresh_buy_quote_detail,
                Some(NoFreshBuyQuoteSubreason::StateStale)
            );
        } else {
            panic!("expected NoFreshBuyQuote");
        }
    }

    #[test]
    fn select_round_trip_pools_cross_dex_sell_with_capped_token_amount() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let vault_b = sample_vault(1_000, 1_100_000_000);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let selection = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .expect("capped sell should enable cross-dex pair");
        assert_eq!(selection.buy_pool_address, "poolA");
        assert_eq!(selection.sell_pool_address, "poolB");
        assert!(selection.sell_quote.amount_out > 0);
    }

    #[test]
    fn select_round_trip_pools_state_stale_sell_records_not_fresh_detail() {
        let pool_a = sample_pool("orca", "poolA");
        let pool_b = sample_pool("pump_amm", "poolB");
        let vault_a = sample_vault(1_000_000_000_000, 900_000_000);
        let mut vault_b = sample_vault(1_000_000_000_000, 1_100_000_000);
        vault_b.updated_at = Instant::now() - Duration::from_secs(300);
        let candidates = [
            RoundTripPoolCandidate {
                pool: &pool_a,
                vault: Some(&vault_a),
                dlmm_bins: None,
                dex: "orca",
            },
            RoundTripPoolCandidate {
                pool: &pool_b,
                vault: Some(&vault_b),
                dlmm_bins: None,
                dex: "pump_amm",
            },
        ];
        let err = select_round_trip_pools(
            &candidates,
            DLMM_PROBE_SOL_LAMPORTS,
            &QuoteFreshnessConfig::default(),
        )
        .unwrap_err();
        if let RoundTripSelectFailure::InsufficientPools(insufficient) = err {
            assert_eq!(
                insufficient.subreason,
                RoundTripInsufficientSubreason::NoCrossDexSell
            );
            assert_eq!(
                insufficient.no_cross_dex_sell_detail,
                Some(NoCrossDexSellDetailReason::SellNotFresh)
            );
            let detail = insufficient
                .sell_not_fresh_detail_counts
                .as_ref()
                .and_then(|m| {
                    m.get(&QuoteNotFreshDiagnosis {
                        kind: QuoteNotFreshKind::ExecutableMarginal,
                        cause: QuoteNotFreshCause::AgeExceeded,
                    })
                })
                .copied()
                .unwrap_or(0);
            assert!(
                detail > 0,
                "StateStale sell path must increment sell_not_fresh_detail"
            );
        } else {
            panic!("expected NoCrossDexSell");
        }
    }
}
