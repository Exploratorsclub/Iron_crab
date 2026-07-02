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

use crate::ipc::BinData;
use crate::solana::dex::meteora_bin_walker::{dlmm_fee_bps, walker_from_bins};

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
    match quote.kind {
        QuoteKind::LastTradeMid => {
            now.duration_since(quote.as_of_ts) <= Duration::from_millis(config.trade_ttl_ms)
        }
        QuoteKind::ExecutableMarginal => {
            if now.duration_since(quote.as_of_ts) > Duration::from_millis(config.state_ttl_ms) {
                return false;
            }
            match current_vault {
                Some(vault) => state_fingerprint(vault) == quote.state_fingerprint,
                None => true,
            }
        }
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
        if !dlmm_marginal_price_plausible(marginal_price, reserve_mid, trade_mid)
            || !is_plausible_sol_per_token_price(&pool.token_mint, marginal_price)
        {
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

/// Failure reason for `select_round_trip_pools`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoundTripSelectFailure {
    InsufficientPools,
    IncompatibleQuoteKind,
    QuoteStale,
}

fn sol_per_token_from_buy_quote(quote: &PoolQuote, token_decimals: u8) -> Decimal {
    trade_implied_sol_per_token(quote.amount_in, quote.amount_out, token_decimals)
}

fn sol_per_token_from_sell_quote(quote: &PoolQuote, token_decimals: u8) -> Decimal {
    trade_implied_sol_per_token(quote.amount_out, quote.amount_in, token_decimals)
}

/// I-ARB-5: per-DEX best buy + cross-DEX best sell with matching QuoteKind.
pub fn select_round_trip_pools(
    candidates: &[RoundTripPoolCandidate<'_>],
    probe_lamports: u64,
    freshness: &QuoteFreshnessConfig,
) -> Result<RoundTripPoolSelection, RoundTripSelectFailure> {
    if candidates.len() < 2 {
        return Err(RoundTripSelectFailure::InsufficientPools);
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

    let mut best_buy: Option<BuyCandidate<'_>> = None;
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
        let replace = match &best_buy {
            None => true,
            Some(current) => sol_per_token < current.sol_per_token,
        };
        if replace {
            best_buy = Some(BuyCandidate {
                candidate,
                quote: buy_quote,
                sol_per_token,
            });
        }
    }

    let Some(best_buy) = best_buy else {
        return Err(RoundTripSelectFailure::InsufficientPools);
    };

    let mut best_sell: Option<BuyCandidate<'_>> = None;
    let mut saw_incompatible_kind = false;

    for candidate in candidates {
        if candidate.dex == best_buy.candidate.dex {
            continue;
        }
        let sell_quote = quote_exact_in_with_freshness(
            candidate.pool,
            candidate.vault,
            candidate.dlmm_bins,
            &candidate.pool.token_mint,
            NATIVE_SOL_MINT,
            best_buy.quote.amount_out,
            freshness,
        );
        let Some(sell_quote) = sell_quote else {
            continue;
        };
        if !is_quote_fresh(&sell_quote, freshness, candidate.vault, now) {
            continue;
        }
        if !quotes_pairable(&best_buy.quote, &sell_quote) {
            saw_incompatible_kind = true;
            continue;
        }
        let sol_per_token = sol_per_token_from_sell_quote(&sell_quote, token_decimals);
        if sol_per_token <= Decimal::ZERO {
            continue;
        }
        let replace = match &best_sell {
            None => true,
            Some(current) => sol_per_token > current.sol_per_token,
        };
        if replace {
            best_sell = Some(BuyCandidate {
                candidate,
                quote: sell_quote,
                sol_per_token,
            });
        }
    }

    let Some(best_sell) = best_sell else {
        if saw_incompatible_kind {
            return Err(RoundTripSelectFailure::IncompatibleQuoteKind);
        }
        return Err(RoundTripSelectFailure::InsufficientPools);
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
}
