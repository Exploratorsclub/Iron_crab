//! Core IPC Schema Types
//!
//! Per docs/STORAGE_CONVENTIONS.md, every record must have:
//! - schema_version (u32)
//! - ts_unix_ms (u64)
//! - component (string)
//! - build (string)
//! - run_id (string/uuid)

// Large IPC message constructors naturally need many params
#![allow(clippy::too_many_arguments)]

use rust_decimal::prelude::*;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Current schema version for all IPC types
pub const SCHEMA_VERSION: u32 = 1;

/// Native SOL mint address
pub const NATIVE_SOL_MINT: &str = "So11111111111111111111111111111111111111112";

/// Default quote mint for backward compatibility (SOL)
fn default_sol_mint() -> String {
    NATIVE_SOL_MINT.to_string()
}

// ============================================================================
// Common Header (embedded in each record type)
// ============================================================================

/// Common header fields required by STORAGE_CONVENTIONS.md
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordHeader {
    pub schema_version: u32,
    pub ts_unix_ms: u64,
    pub component: String,
    pub build: String,
    pub run_id: String,
}

impl RecordHeader {
    pub fn new(component: &str, build: &str, run_id: &str) -> Self {
        Self {
            schema_version: SCHEMA_VERSION,
            ts_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            component: component.to_string(),
            build: build.to_string(),
            run_id: run_id.to_string(),
        }
    }
}

// ============================================================================
// Amount with explicit units (P0 requirement: no implicit decimals)
// ============================================================================

/// Amount with explicit unit specification
///
/// P0 requirement (DoD P): Units/Decimals explizit – keine impliziten UI/raw Konventionen.
///
/// All monetary amounts in the system MUST use this type to prevent unit confusion.
/// - `raw`: on-chain value (lamports for SOL, smallest unit for tokens)
/// - `decimals`: token decimals (9 for SOL, 6 for USDC, 8 for BONK, etc.)
/// - `ui`: human-readable value (computed as raw / 10^decimals)
///
/// Example: 1 SOL = ExplicitAmount { raw: 1_000_000_000, decimals: 9, ui: Some(1.0) }
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExplicitAmount {
    /// Raw on-chain value (lamports for SOL, smallest unit for tokens)
    pub raw: u64,
    /// Number of decimals for this token (e.g., 9 for SOL, 6 for USDC)
    pub decimals: u8,
    /// Optional: UI representation (raw / 10^decimals), for logging convenience only
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ui: Option<Decimal>,
}

impl ExplicitAmount {
    /// Create a new ExplicitAmount from raw value and decimals
    pub fn new(raw: u64, decimals: u8) -> Self {
        let ui = Decimal::from(raw) / Decimal::from(10u64.pow(decimals as u32));
        Self {
            raw,
            decimals,
            ui: Some(ui),
        }
    }

    /// Create zero amount with specified decimals
    pub fn zero(decimals: u8) -> Self {
        Self {
            raw: 0,
            decimals,
            ui: Some(Decimal::ZERO),
        }
    }

    /// Create from UI value (human-readable) and decimals
    pub fn from_ui(ui_value: Decimal, decimals: u8) -> Self {
        let multiplier = Decimal::from(10u64.pow(decimals as u32));
        let raw = (ui_value * multiplier).to_u64().unwrap_or(0);
        Self {
            raw,
            decimals,
            ui: Some(ui_value),
        }
    }

    /// Create SOL amount from lamports
    pub fn sol_from_lamports(lamports: u64) -> Self {
        Self::new(lamports, 9)
    }

    /// Create SOL amount from UI value (e.g., 1.5 SOL)
    pub fn sol_from_ui(sol: f64) -> Self {
        Self::from_ui(Decimal::from_f64_retain(sol).unwrap_or(Decimal::ZERO), 9)
    }

    /// Get UI value as f64 (for calculations)
    pub fn as_f64(&self) -> f64 {
        self.ui.and_then(|d| d.to_f64()).unwrap_or(0.0)
    }
}

// ============================================================================
// Meteora DLMM Bin Data (for BinArrayUpdate events)
// ============================================================================

/// Single bin data for Meteora DLMM (compact serialization)
///
/// Only bins with non-zero liquidity are included in BinArrayUpdate events.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BinData {
    /// Offset within the bin array (0-69)
    pub offset: u8,
    /// Amount of token X (base) in this bin
    pub amount_x: u64,
    /// Amount of token Y (quote) in this bin
    pub amount_y: u64,
}

// ============================================================================
// MarketEvent (produced by market-data)
// ============================================================================

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind")]
pub enum MarketEventKind {
    /// New pool/market discovered
    PoolCreated {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        dex: String,
        initial_liquidity_sol: Option<Decimal>,
    },
    /// DEX-specific static pool accounts needed for deterministic intent building.
    ///
    /// For Pump.fun AMM (PumpSwap), `accounts` is a v1 ordered list:
    /// [0] pool_market
    /// [1] global_config
    /// [2] base_mint
    /// [3] quote_mint
    /// [4] pool_base_vault
    /// [5] pool_quote_vault
    /// [6] protocol_fee_recipient
    /// [7] protocol_fee_recipient_ta
    /// [8] event_authority
    /// [9] coin_creator_vault_ata
    /// [10] coin_creator_vault_authority
    /// [11] global_volume_accumulator
    /// [12] fee_config
    /// [13] fee_program
    DexPoolAccounts {
        dex: String,
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        accounts: Vec<String>,
    },
    /// Trade/swap observed on-chain (detailed for 4-filter strategy)
    Trade {
        pool_address: String,
        mint: String,
        /// Quote mint (e.g., SOL or USDC) - critical for cross-DEX price comparison.
        /// Only trades with matching quote_mint should have prices compared.
        #[serde(default = "default_sol_mint")]
        quote_mint: String,
        trader: String,
        is_buy: bool,
        sol_amount: u64, // lamports (or quote amount in quote_mint units)
        token_amount: u64,
        token_decimals: u8, // e.g., 6 for USDC, 9 for most pump.fun tokens
        signature: Option<String>,
        /// DEX name (raydium, orca, meteora_dlmm, pumpfun, pump_amm)
        dex: String,
        /// Creator/dev wallet for PumpFun tokens (needed for intent building).
        /// Populated from creator_cache when available.
        #[serde(skip_serializing_if = "Option::is_none")]
        creator: Option<String>,
        /// Token program for the base mint (SPL Token or Token-2022).
        /// Extracted from PumpFun swap instruction accounts.
        /// Used for deterministic ATA creation without RPC lookup.
        #[serde(skip_serializing_if = "Option::is_none")]
        token_program: Option<String>,
    },
    /// Swap observed on-chain (legacy format)
    SwapObserved {
        pool_address: String,
        signature: String,
        side: String, // "buy" or "sell"
        amount_in: ExplicitAmount,
        amount_out: ExplicitAmount,
    },
    /// Price/quote update
    PriceUpdate {
        pool_address: String,
        base_mint: String,
        quote_mint: String,
        price: Decimal,
        liquidity_sol: Option<Decimal>,
    },
    /// Liquidity removed from pool (potential rug signal)
    LiquidityRemoved {
        pool_address: String,
        mint: String,
        sol_amount: u64, // lamports removed
        token_amount: u64,
        signature: Option<String>,
    },
    /// Dev wallet identified with supply percentage
    DevWalletIdentified {
        mint: String,
        dev_wallet: String,
        supply_percentage: f64,
    },
    /// Token mint account metadata (authority/decimals/supply) for risk filtering.
    ///
    /// Produced by market-data from Geyser account updates for tracked mints.
    /// This is intentionally a *state snapshot* (not TX-history dependent).
    TokenMintInfo {
        mint: String,
        /// SPL Token program owner of the mint account (Tokenkeg... or TokenzQd...)
        token_program: String,
        decimals: u8,
        supply: u64,
        /// Present if mint authority is still set (NOT renounced)
        mint_authority: Option<String>,
        /// Present if freeze authority is still set
        freeze_authority: Option<String>,
    },
    /// Slot progression (heartbeat)
    SlotUpdate { current_slot: u64 },
    /// Pool state update (vault balances changed via Geyser Account Subscription)
    ///
    /// Emitted by market-data when subscribed vault token accounts change.
    /// Consumers should update their local pool cache with these reserve values.
    /// This eliminates the need for RPC calls to fetch vault balances.
    PoolStateUpdate {
        pool_address: String,
        dex: String,
        /// Base token reserve (raw amount, use with base_decimals)
        reserve_base: u64,
        /// Quote token reserve (raw amount, use with quote_decimals)
        reserve_quote: u64,
        /// Base mint for context
        base_mint: String,
        /// Quote mint for context
        quote_mint: String,
        /// Slot when this update was observed
        update_slot: u64,
        // =====================================================================
        // DLMM-specific fields (Option D: Bin Array Traversierung)
        // =====================================================================
        /// Meteora DLMM: Active bin index (where current price is)
        /// For non-DLMM pools this is None.
        #[serde(skip_serializing_if = "Option::is_none")]
        active_id: Option<i32>,
        /// Meteora DLMM: Bin step (price increment per bin in bps)
        /// For non-DLMM pools this is None.
        #[serde(skip_serializing_if = "Option::is_none")]
        bin_step: Option<u16>,
    },
    /// Meteora DLMM Bin Array update (via Geyser Account Subscription)
    ///
    /// Emitted by market-data when subscribed Bin Array accounts change.
    /// Contains liquidity distribution data for accurate quoting.
    /// Eliminates the need for RPC calls to fetch bin arrays.
    BinArrayUpdate {
        pool_address: String,
        /// Index of this bin array (determines which bins it contains)
        bin_array_index: i64,
        /// Bin data (compact: only bins with liquidity)
        bins: Vec<BinData>,
        /// Slot when this update was observed
        update_slot: u64,
    },
    /// Raw account update from Geyser
    AccountUpdate {
        pubkey: String,
        owner: String,
        data_len: usize,
    },
    /// Transaction detected via Geyser
    TransactionDetected { signature: String, program: String },

    // =========================================================================
    // Wallet State Tracking (P0: Position Reconciliation)
    // =========================================================================
    /// Wallet token balance snapshot (published by market-data at startup)
    ///
    /// Used for position reconciliation after restarts to detect:
    /// - Manual sales via external wallets (Phantom, Jupiter, etc.)
    /// - Emergency liquidations via UI
    /// - External transfers
    ///
    /// Published once at market-data startup for all non-zero token accounts.
    /// NO periodic refresh (wallet changes come via ExecutionResults).
    WalletBalanceSnapshot {
        /// Token mint address
        mint: String,
        /// Current balance (raw units)
        balance_raw: u64,
        /// Token decimals for UI conversion
        decimals: u8,
        /// Token program (SPL Token or Token-2022)
        token_program: String,
    },

    // =========================================================================
    // Wallet Tracking Events (P1: Smart Money / Insider Detection)
    // =========================================================================
    /// Known wallet activity detected (smart money, insider, dev)
    WalletActivity {
        wallet: String,
        wallet_type: WalletType,
        action: WalletAction,
        mint: String,
        amount_sol: u64, // lamports
        amount_tokens: u64,
        signature: String,
        /// Historical win rate of this wallet (0.0-1.0), None if unknown
        wallet_win_rate: Option<f64>,
    },

    /// Early buyer detected for a token (first N minutes after pool creation)
    EarlyBuyerDetected {
        mint: String,
        buyer_wallet: String,
        buy_slot: u64,
        slots_after_creation: u64,
        amount_sol: u64,
        amount_tokens: u64,
        buyer_rank: u32, // 1 = first buyer, 2 = second, etc.
    },

    /// Insider alert: coordinated activity or known bad actor
    InsiderAlert {
        mint: String,
        alert_type: InsiderAlertType,
        wallets_involved: Vec<String>,
        description: String,
        severity: AlertSeverity,
    },
}

/// Type of tracked wallet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalletType {
    /// Known profitable trader ("smart money")
    SmartMoney,
    /// Token creator/dev wallet
    DevWallet,
    /// Early buyer (bought in first N slots)
    EarlyBuyer,
    /// Known insider/team wallet
    Insider,
    /// Known rug puller or scammer
    KnownBadActor,
    /// Whale (large holder)
    Whale,
}

/// Action performed by tracked wallet
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WalletAction {
    Buy,
    Sell,
    Transfer,
    /// Adding liquidity to pool
    AddLiquidity,
    /// Removing liquidity from pool
    RemoveLiquidity,
}

/// Type of insider alert
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum InsiderAlertType {
    /// Multiple wallets buying in coordinated pattern
    CoordinatedBuying,
    /// Dev wallet selling
    DevSelling,
    /// Known bad actor active
    BadActorActive,
    /// Same wallet cluster active on multiple new tokens
    WalletClusterActive,
    /// Large holder dumping
    WhaleDumping,
}

/// Severity level for alerts
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AlertSeverity {
    Info,
    Warning,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct MarketEvent {
    #[serde(flatten)]
    pub header: RecordHeader,
    pub event_id: String,
    pub source: String, // "geyser", "rpc", "ws"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<u64>,
    #[serde(flatten)]
    pub kind: MarketEventKind,
}

impl MarketEvent {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        event_id: String,
        source: &str,
        slot: Option<u64>,
        kind: MarketEventKind,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            event_id,
            source: source.to_string(),
            slot,
            kind,
        }
    }
}

// ============================================================================
// P2: Dynamic Priority Fee (from Geyser transactions, NO RPC calls)
// ============================================================================

/// Priority fee sample from a Geyser transaction
///
/// Published by market-data to NATS for execution-engine consumption.
/// Allows dynamic fee estimation without additional RPC calls.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PriorityFeeSample {
    #[serde(flatten)]
    pub header: RecordHeader,
    /// Slot when transaction was processed
    pub slot: u64,
    /// Total fee in lamports
    pub fee_lamports: u64,
    /// Compute units consumed
    pub compute_units: u64,
    /// Calculated priority fee in micro-lamports per CU
    pub priority_fee_micro_lamports: u64,
}

impl PriorityFeeSample {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        slot: u64,
        fee_lamports: u64,
        compute_units: u64,
        priority_fee_micro_lamports: u64,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            slot,
            fee_lamports,
            compute_units,
            priority_fee_micro_lamports,
        }
    }
}

/// Fee percentiles from recent Geyser samples
///
/// Periodically published by market-data with aggregated statistics.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PriorityFeePercentiles {
    #[serde(flatten)]
    pub header: RecordHeader,
    /// Number of samples used for calculation
    pub sample_count: usize,
    /// Last slot in sample window
    pub last_slot: u64,
    /// 25th percentile (micro-lamports per CU)
    pub p25: u64,
    /// 50th percentile / median (micro-lamports per CU)
    pub p50: u64,
    /// 75th percentile (micro-lamports per CU)
    pub p75: u64,
    /// 90th percentile (micro-lamports per CU)
    pub p90: u64,
    /// Recommended fee for Tier0 (P90 × 1.5)
    pub tier0_recommended: u64,
    /// Recommended fee for Tier1 (P50 × 1.2)
    pub tier1_recommended: u64,
    /// Recommended fee for Arb (P75 × 1.3)
    pub arb_recommended: u64,
}

impl PriorityFeePercentiles {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        sample_count: usize,
        last_slot: u64,
        p25: u64,
        p50: u64,
        p75: u64,
        p90: u64,
        tier0_recommended: u64,
        tier1_recommended: u64,
        arb_recommended: u64,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            sample_count,
            last_slot,
            p25,
            p50,
            p75,
            p90,
            tier0_recommended,
            tier1_recommended,
            arb_recommended,
        }
    }
}

// ============================================================================
// P1: Fee/Compute Policy (owned by execution-engine)
// ============================================================================

/// Fee policy configuration for execution engine
///
/// P1 requirement: Engine owns compute budget, priority fee, and tip policies.
/// Strategies can provide hints but engine has final authority.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FeePolicy {
    // === Compute Budget ===
    /// Default compute unit limit for transactions
    pub default_compute_units: u32,

    /// Maximum compute units allowed (hard limit)
    pub max_compute_units: u32,

    /// Compute units for arbitrage (multi-hop, higher complexity)
    pub arb_compute_units: u32,

    // === Priority Fees ===
    /// Default priority fee (micro-lamports per CU)
    pub default_priority_fee_micro_lamports: u64,

    /// Maximum priority fee allowed (hard limit)
    pub max_priority_fee_micro_lamports: u64,

    /// Priority fee for Tier0 urgent intents
    pub tier0_priority_fee_micro_lamports: u64,

    /// Multiplier for elevated urgency (urgency=1)
    pub urgency_multiplier_elevated: f64,

    /// Multiplier for urgent priority (urgency=2)
    pub urgency_multiplier_urgent: f64,

    // === Cost Limits ===
    /// Maximum total transaction cost (base + priority, lamports)
    pub max_tx_cost_lamports: u64,

    /// Minimum expected profit after fees to proceed (basis points)
    pub min_profit_after_fees_bps: i32,
}

impl Default for FeePolicy {
    fn default() -> Self {
        Self {
            // Compute budget
            default_compute_units: 200_000,
            max_compute_units: 1_400_000, // Solana max is 1.4M
            arb_compute_units: 400_000,   // Multi-hop arb needs more

            // Priority fees (micro-lamports per CU)
            default_priority_fee_micro_lamports: 1_000, // 0.001 lamports/CU
            max_priority_fee_micro_lamports: 100_000,   // 0.1 lamports/CU
            tier0_priority_fee_micro_lamports: 10_000,  // 0.01 lamports/CU

            // Urgency multipliers
            urgency_multiplier_elevated: 2.0,
            urgency_multiplier_urgent: 5.0,

            // Cost limits
            max_tx_cost_lamports: 50_000_000, // 0.05 SOL max total cost
            min_profit_after_fees_bps: 10,    // Must profit at least 0.1% after fees
        }
    }
}

impl FeePolicy {
    /// Calculate effective compute units for an intent
    pub fn compute_units_for_intent(&self, intent: &TradeIntent) -> u32 {
        // Use hint if provided and within limits
        if let Some(hint) = intent.hint_compute_units {
            return hint.min(self.max_compute_units);
        }

        // Use arb CU for bundle/atomic intents (likely multi-hop)
        if intent.requires_bundle() {
            return self.arb_compute_units;
        }

        self.default_compute_units
    }

    /// Calculate effective priority fee for an intent
    pub fn priority_fee_for_intent(&self, intent: &TradeIntent) -> u64 {
        let base_fee = match intent.tier {
            IntentTier::Tier0 => self.tier0_priority_fee_micro_lamports,
            IntentTier::Tier1 => self.default_priority_fee_micro_lamports,
            // Arb uses elevated fee (between Tier1 and Tier0)
            IntentTier::Arb => {
                (self.default_priority_fee_micro_lamports + self.tier0_priority_fee_micro_lamports)
                    / 2
            }
        };

        // Apply urgency multiplier
        let multiplier = match intent.urgency() {
            0 => 1.0,
            1 => self.urgency_multiplier_elevated,
            _ => self.urgency_multiplier_urgent, // 2+
        };

        let scaled_fee = (base_fee as f64 * multiplier) as u64;

        // Override with hint if provided (but cap at max)
        intent
            .hint_priority_fee_micro_lamports
            .unwrap_or(scaled_fee)
            .min(self.max_priority_fee_micro_lamports)
    }

    /// Estimate total transaction cost (base fee + priority fee)
    /// Returns (base_fee_lamports, priority_fee_lamports, total_lamports)
    pub fn estimate_tx_cost(&self, intent: &TradeIntent) -> (u64, u64, u64) {
        let compute_units = self.compute_units_for_intent(intent);
        let priority_fee_per_cu = self.priority_fee_for_intent(intent);

        // Base fee is 5000 lamports per signature (typically 1 signature)
        let base_fee = 5_000u64;

        // Priority fee = (price_per_cu * compute_units) / 1_000_000
        // (micro-lamports to lamports)
        let priority_fee = (priority_fee_per_cu as u128 * compute_units as u128 / 1_000_000) as u64;

        let total = base_fee + priority_fee;
        (base_fee, priority_fee, total)
    }

    /// Check if intent's fees would be profitable
    /// Returns (is_profitable, profit_bps_after_fees)
    pub fn is_profitable_after_fees(&self, intent: &TradeIntent) -> (bool, i32) {
        let (_, _, total_cost) = self.estimate_tx_cost(intent);

        // Convert cost to basis points of required capital
        let capital = intent.required_capital.raw;
        if capital == 0 {
            return (false, 0);
        }

        let cost_bps = ((total_cost as u128 * 10_000) / capital as u128) as i32;
        let profit_after_fees = intent.expected_roi_bps - cost_bps;

        (
            profit_after_fees >= self.min_profit_after_fees_bps,
            profit_after_fees,
        )
    }
}

// ============================================================================
// P1: Fairness/Starvation Policy (DoD D.1)
// ============================================================================

/// Fairness policy to prevent one strategy from monopolizing execution capacity
///
/// P1 requirement: Dauerhafte Verdrängung wird begrenzt (max preemptions pro Worker/Slot)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FairnessPolicy {
    /// Maximum preemptions allowed per source within the time window
    pub max_preemptions_per_source: u32,

    /// Time window for preemption tracking (seconds)
    pub preemption_window_secs: u64,

    /// Block duration after max preemptions reached (seconds)
    /// During this time, the starved source's intents get elevated priority
    pub starvation_block_secs: u64,

    /// Enable fairness tracking
    pub enabled: bool,

    /// Log preemption events for debugging/tuning
    pub log_preemptions: bool,
}

impl Default for FairnessPolicy {
    fn default() -> Self {
        Self {
            max_preemptions_per_source: 5, // Max 5 preemptions
            preemption_window_secs: 60,    // Within 60 seconds
            starvation_block_secs: 30,     // 30s elevated priority after starvation
            enabled: true,
            log_preemptions: true,
        }
    }
}

// ============================================================================
// TradeIntent (produced by strategy bots)
// ============================================================================

/// Intent tier for arbitration priority
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IntentTier {
    /// Tier 0: highest priority (e.g., emergency exits, MEV)
    Tier0 = 0,
    /// Tier 1: normal priority (e.g., momentum trades)
    Tier1 = 1,
    /// Arbitrage: time-sensitive but not emergency
    Arb = 2,
}

/// Origin type for Typ A vs Typ B classification (DoD D.1)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum IntentOrigin {
    /// Typ A: market-driven strategy (e.g., momentum, strategy arbitrage)
    StrategyA,
    /// Typ B: reactive/tx-dependent MEV (e.g., backrun, bundle optimization)
    ExecutionMevB,
}

/// Regime classification for momentum policies
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradingRegime {
    /// Early phase: thin data, high manipulation risk, strict filters
    Early,
    /// Established phase: more data, classic momentum signals
    Established,
    /// Not applicable (e.g., pure arbitrage)
    NotApplicable,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TradeExecutionConstraints {
    /// Minimum acceptable output amount for deterministic tx planning.
    ///
    /// - For token->SOL sells: lamports (decimals=9)
    /// - For SOL->token buys: raw token units (token decimals)
    /// - For Orca: raw output mint units
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub min_out: Option<ExplicitAmount>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TradeIntent {
    #[serde(flatten)]
    pub header: RecordHeader,

    // === Required fields per DoD B) ===
    pub intent_id: String,
    pub source: String, // "momentum-bot", "arb-strategy", "execution-worker"
    pub tier: IntentTier,
    pub origin_type: IntentOrigin,

    /// Deadline: slot number or None if TTL-based
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deadline_slot: Option<u64>,
    /// TTL in milliseconds from creation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_ms: Option<u64>,

    /// Amount-in for this trade, with explicit units.
    ///
    /// Semantics:
    /// - For `side=Buy`: `resources.input_mint` is SOL ("So111...") and this is the SOL amount-in
    ///   in lamports (`decimals=9`).
    /// - For `side=Sell`: `resources.input_mint` is the token mint and this is the token amount-in
    ///   in raw units (`decimals=token_decimals`).
    ///
    /// This field is intentionally *not* named `amount_in` for historical reasons, but execution
    /// treats it as the amount-in.
    pub required_capital: ExplicitAmount,

    /// Resources: mints/pools/accounts involved
    pub resources: TradeResources,

    /// Expected value / ROI in basis points
    pub expected_roi_bps: i32,

    /// Maximum acceptable slippage in basis points
    pub max_slippage_bps: u32,

    /// Trade direction
    pub side: TradeSide,

    /// Regime classification
    pub regime: TradingRegime,

    /// Optional: trigger event that caused this intent
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trigger_event_id: Option<String>,

    /// P1: Require atomic bundle execution (Jito) - for arbitrage
    /// If true, execution-engine MUST use Jito bundle submission.
    /// If bundle submission fails, intent is rejected (atomic guarantee).
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub require_bundle: Option<bool>,

    /// P1: Custom tip amount for Jito bundle (overrides default)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub bundle_tip_lamports: Option<u64>,

    // === P1: Fee Hints (Engine has final authority) ===
    /// Hint: suggested compute unit limit (engine may override)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hint_compute_units: Option<u32>,

    /// Hint: suggested priority fee in micro-lamports per CU (engine may override)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hint_priority_fee_micro_lamports: Option<u64>,

    /// Hint: urgency level for fee scaling (0=normal, 1=elevated, 2=urgent)
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub hint_urgency: Option<u8>,

    /// P1: Strategy-specific metadata for decision records and debugging
    /// Key-value pairs set by strategy, consumed by execution-engine
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub metadata: std::collections::HashMap<String, String>,

    /// Typed execution constraints (preferred over stringly metadata).
    ///
    /// Backward compatible: older producers omit this field.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub execution: Option<TradeExecutionConstraints>,

    // === Multi-Hop Arbitrage ===
    /// Multi-hop swap path for N-hop arbitrage cycles.
    ///
    /// If present, execution-engine should execute hops sequentially (or atomically via Jito bundle).
    /// If None, legacy 2-hop routing via `resources.pools` is used.
    ///
    /// Semantics:
    /// - `swap_path[0].input_mint` == base token (usually WSOL)
    /// - `swap_path[N-1].output_mint` == base token (cycle returns to start)
    /// - Each hop has `pool_alternatives` for fallback routing
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub swap_path: Option<Vec<SwapHop>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TradeResources {
    pub input_mint: String,
    pub output_mint: String,
    pub pools: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub accounts: Vec<String>,
    /// Token program for output_mint (SPL Token or Token-2022).
    /// If set, execution-engine uses this directly (NO cache lookup needed).
    /// Format: base58-encoded pubkey string.
    /// - SPL Token: "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
    /// - Token-2022: "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub token_program: Option<String>,
}

// ============================================================================
// Multi-Hop Arbitrage Types (for N-hop swap paths)
// ============================================================================

/// Alternative pool for fallback routing in multi-hop swaps.
///
/// When the primary pool fails (slippage, insufficient liquidity, etc.),
/// execution-engine can try these alternatives in order.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PoolAlternative {
    /// Pool address (base58)
    pub pool_address: String,
    /// DEX type identifier (e.g., "raydium_amm_v4", "orca", "meteora_dlmm")
    pub dex: String,
    /// Pre-computed expected output for this alternative (may differ from primary).
    /// Set to 0 if not pre-computed; execution-engine will quote on-the-fly.
    #[serde(default)]
    pub expected_output: u64,
}

/// Single hop in a multi-hop swap path.
///
/// Each hop represents one swap operation. For N-hop arbitrage:
/// - Hop 0: WSOL → Token_A
/// - Hop 1: Token_A → Token_B
/// - ...
/// - Hop N-1: Token_X → WSOL (return to base)
///
/// Design notes (from external review):
/// - `pool_alternatives` contains backup pools for execution fallback
/// - `expected_output` is from probe quotes, NOT spot price
/// - execution-engine should re-quote before executing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SwapHop {
    /// Primary pool address (best option from cycle finder)
    pub pool_address: String,
    /// DEX type identifier
    pub dex: String,
    /// Input mint for this hop (base58)
    pub input_mint: String,
    /// Output mint for this hop (base58)
    pub output_mint: String,
    /// Expected output amount from probe quote (for slippage estimation).
    /// This is indicative; execution-engine will re-quote with actual amount.
    pub expected_output: u64,
    /// Alternative pools for this hop (sorted by preference, best first).
    /// Used for fallback if primary pool fails.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub pool_alternatives: Vec<PoolAlternative>,
}

impl SwapHop {
    /// Create a new swap hop with primary pool only
    pub fn new(
        pool_address: String,
        dex: String,
        input_mint: String,
        output_mint: String,
        expected_output: u64,
    ) -> Self {
        Self {
            pool_address,
            dex,
            input_mint,
            output_mint,
            expected_output,
            pool_alternatives: Vec::new(),
        }
    }

    /// Create with alternatives
    pub fn with_alternatives(
        pool_address: String,
        dex: String,
        input_mint: String,
        output_mint: String,
        expected_output: u64,
        alternatives: Vec<PoolAlternative>,
    ) -> Self {
        Self {
            pool_address,
            dex,
            input_mint,
            output_mint,
            expected_output,
            pool_alternatives: alternatives,
        }
    }

    /// Check if this hop has fallback options
    pub fn has_alternatives(&self) -> bool {
        !self.pool_alternatives.is_empty()
    }

    /// Get total pool count (primary + alternatives)
    pub fn pool_count(&self) -> usize {
        1 + self.pool_alternatives.len()
    }
}

// ============================================================================
// Control Plane Requests (control-plane -> execution-engine)
// ============================================================================

/// Versioned control request kind.
///
/// Published on `ironcrab.v1.control_requests`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ControlRequestKind {
    /// Toggle kill switch.
    ///
    /// Semantics:
    /// - When `active=true`, execution-engine must reject new BUY intents.
    /// - If `liquidate_positions=true`, execution-engine should attempt to sell all token balances.
    KillSwitch {
        active: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        #[serde(default)]
        liquidate_positions: bool,
        /// Optional liquidation parameters.
        #[serde(skip_serializing_if = "Option::is_none")]
        max_slippage_bps: Option<u32>,
        #[serde(skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
    },

    /// Reset kill switch (reactivate BUY processing).
    ResetKillSwitch,

    /// MANUAL operator action: burn tokens from specific token accounts and (optionally) close them.
    ///
    /// Safety semantics:
    /// - This must never be triggered by UI liquidation flows.
    /// - execution-engine must re-validate that no sell route exists before burning.
    BurnTokenAccounts {
        /// Wallet owner pubkey (engine must verify it matches configured wallet).
        owner_pubkey: String,
        /// Token account pubkeys to burn/close.
        token_accounts: Vec<String>,
        /// Close token accounts after burn to recover rent (default: true).
        #[serde(default = "default_true")]
        close_accounts: bool,
        /// Optional operator reason for audit logs.
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

fn default_true() -> bool {
    true
}

/// Control request wrapper with required storage header.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ControlRequest {
    #[serde(flatten)]
    pub header: RecordHeader,

    /// Unique request ID for correlation.
    pub request_id: String,

    /// Target component (e.g., "execution-engine").
    pub target: String,

    #[serde(flatten)]
    pub kind: ControlRequestKind,
}

impl ControlRequest {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        request_id: String,
        target: &str,
        kind: ControlRequestKind,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            request_id,
            target: target.to_string(),
            kind,
        }
    }
}

impl TradeIntent {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        intent_id: String,
        source: &str,
        tier: IntentTier,
        origin_type: IntentOrigin,
        required_capital: ExplicitAmount,
        resources: TradeResources,
        expected_roi_bps: i32,
        max_slippage_bps: u32,
        side: TradeSide,
        regime: TradingRegime,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            intent_id,
            source: source.to_string(),
            tier,
            origin_type,
            deadline_slot: None,
            ttl_ms: Some(5000), // default 5s TTL
            required_capital,
            resources,
            expected_roi_bps,
            max_slippage_bps,
            side,
            regime,
            trigger_event_id: None,
            require_bundle: None,
            bundle_tip_lamports: None,
            hint_compute_units: None,
            hint_priority_fee_micro_lamports: None,
            hint_urgency: None,
            metadata: std::collections::HashMap::new(),
            execution: None,
            swap_path: None,
        }
    }

    /// Helper constructor for SELL intents.
    ///
    /// This matches execution-engine semantics: `required_capital` is the amount-in of
    /// `resources.input_mint`.
    #[allow(clippy::too_many_arguments)]
    pub fn new_sell(
        component: &str,
        build: &str,
        run_id: &str,
        intent_id: String,
        source: &str,
        tier: IntentTier,
        origin_type: IntentOrigin,
        input_mint: String,
        input_decimals: u8,
        output_mint: String,
        amount_in_raw: u64,
        expected_roi_bps: i32,
        max_slippage_bps: u32,
        regime: TradingRegime,
    ) -> Self {
        Self::new(
            component,
            build,
            run_id,
            intent_id,
            source,
            tier,
            origin_type,
            ExplicitAmount::new(amount_in_raw, input_decimals),
            TradeResources {
                input_mint,
                output_mint,
                pools: vec![],
                accounts: vec![],
                token_program: None,
            },
            expected_roi_bps,
            max_slippage_bps,
            TradeSide::Sell,
            regime,
        )
    }

    pub fn with_deadline_slot(mut self, slot: u64) -> Self {
        self.deadline_slot = Some(slot);
        self.ttl_ms = None;
        self
    }

    pub fn with_ttl_ms(mut self, ttl: u64) -> Self {
        self.ttl_ms = Some(ttl);
        self.deadline_slot = None;
        self
    }

    pub fn with_trigger(mut self, event_id: String) -> Self {
        self.trigger_event_id = Some(event_id);
        self
    }

    /// P1: Mark intent as requiring atomic bundle execution (Jito)
    pub fn with_bundle(mut self, tip_lamports: Option<u64>) -> Self {
        self.require_bundle = Some(true);
        self.bundle_tip_lamports = tip_lamports;
        self
    }

    /// Check if this intent requires atomic bundle execution
    pub fn requires_bundle(&self) -> bool {
        self.require_bundle.unwrap_or(false)
    }

    /// P1: Add fee hints for execution engine
    /// Engine has final authority and may override these hints based on policy.
    pub fn with_fee_hints(
        mut self,
        compute_units: Option<u32>,
        priority_fee_micro_lamports: Option<u64>,
        urgency: Option<u8>,
    ) -> Self {
        self.hint_compute_units = compute_units;
        self.hint_priority_fee_micro_lamports = priority_fee_micro_lamports;
        self.hint_urgency = urgency;
        self
    }

    /// Get effective urgency level (0=normal if not specified)
    pub fn urgency(&self) -> u8 {
        self.hint_urgency.unwrap_or(0)
    }

    // === Multi-Hop Helpers ===

    /// Set multi-hop swap path for N-hop arbitrage
    pub fn with_swap_path(mut self, path: Vec<SwapHop>) -> Self {
        self.swap_path = Some(path);
        self
    }

    /// Check if this is a multi-hop intent
    pub fn is_multi_hop(&self) -> bool {
        self.swap_path
            .as_ref()
            .map(|p| p.len() > 1)
            .unwrap_or(false)
    }

    /// Get hop count (1 for legacy, N for multi-hop)
    pub fn hop_count(&self) -> usize {
        self.swap_path.as_ref().map(|p| p.len()).unwrap_or(1)
    }

    /// Validate multi-hop path consistency
    ///
    /// Returns Err if:
    /// - Path is empty
    /// - Hops don't chain correctly (output != next input)
    /// - Path doesn't form a cycle (first input != last output) when expected
    pub fn validate_swap_path(&self) -> Result<(), String> {
        let Some(path) = &self.swap_path else {
            return Ok(()); // Legacy intent, no path to validate
        };

        if path.is_empty() {
            return Err("swap_path is empty".to_string());
        }

        // Validate hop chaining
        for i in 0..path.len() - 1 {
            if path[i].output_mint != path[i + 1].input_mint {
                return Err(format!(
                    "hop {} output ({}) != hop {} input ({})",
                    i,
                    path[i].output_mint,
                    i + 1,
                    path[i + 1].input_mint
                ));
            }
        }

        // For arbitrage cycles, validate that it returns to start
        if path.len() > 1 && path.first().unwrap().input_mint != path.last().unwrap().output_mint {
            return Err(format!(
                "path doesn't form cycle: first input ({}) != last output ({})",
                path.first().unwrap().input_mint,
                path.last().unwrap().output_mint
            ));
        }

        Ok(())
    }
}

// ============================================================================
// DecisionRecord (produced by execution-engine)
// ============================================================================

/// Check result for decision audit trail
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CheckResult {
    pub check_name: String,
    pub passed: bool,
    pub reason_code: Option<String>,
    pub details: Option<String>,
}

/// Kill switch context attached to a decision (optional).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KillSwitchContext {
    /// Reason provided by control-plane (if any).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// Origin of the kill switch activation (e.g., "control-plane", "auto").
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// Whether liquidation was requested.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidate_positions: Option<bool>,
    /// Control request ID for correlation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
}

/// Simulation result summary
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SimulationResult {
    pub success: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub logs_preview: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compute_units_consumed: Option<u64>,
}

/// Send result summary
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SendResult {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
    pub sent_at_ms: u64,
    /// Method used to send: "tpu", "jito", "rpc"
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_method: Option<String>,
}

/// Final decision outcome
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum DecisionOutcome {
    /// Intent rejected before planning
    Rejected,
    /// Intent expired (TTL/deadline)
    Expired,
    /// Simulation failed
    SimFailed,
    /// Transaction sent (awaiting confirmation)
    Sent,
    /// Transaction confirmed on-chain
    Confirmed,
    /// Transaction failed after send
    FailedConfirmed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DecisionRecord {
    #[serde(flatten)]
    pub header: RecordHeader,

    pub decision_id: String,
    pub intent_id: String,
    /// P1: Source strategy/worker for attribution (e.g., "momentum-bot", "arb-strategy")
    pub source: String,
    pub origin_type: IntentOrigin,
    pub regime: TradingRegime,

    /// All checks performed with pass/fail and reason codes
    pub checks: Vec<CheckResult>,

    /// Primary rejection reason (if rejected)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_reject_reason: Option<String>,

    /// Optional kill switch context for audit/debugging.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kill_switch: Option<KillSwitchContext>,

    /// Hash of the transaction plan (for correlation)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan_hash: Option<String>,

    /// Simulation result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub simulate: Option<SimulationResult>,

    /// Send result
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send: Option<SendResult>,

    /// Final outcome
    pub outcome: DecisionOutcome,

    /// Config snapshot ID (for replay)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub config_snapshot_id: Option<String>,

    /// Input snapshots for replay
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    pub input_snapshots: HashMap<String, String>,
}

impl DecisionRecord {
    #[allow(clippy::too_many_arguments)]
    pub fn new_rejected(
        component: &str,
        build: &str,
        run_id: &str,
        decision_id: String,
        intent_id: String,
        source: String,
        origin_type: IntentOrigin,
        regime: TradingRegime,
        checks: Vec<CheckResult>,
        primary_reject_reason: String,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            decision_id,
            intent_id,
            source,
            origin_type,
            regime,
            checks,
            primary_reject_reason: Some(primary_reject_reason),
            kill_switch: None,
            plan_hash: None,
            simulate: None,
            send: None,
            outcome: DecisionOutcome::Rejected,
            config_snapshot_id: None,
            input_snapshots: HashMap::new(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_sim_failed(
        component: &str,
        build: &str,
        run_id: &str,
        decision_id: String,
        intent_id: String,
        source: String,
        origin_type: IntentOrigin,
        regime: TradingRegime,
        checks: Vec<CheckResult>,
        plan_hash: String,
        sim_result: SimulationResult,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            decision_id,
            intent_id,
            source,
            origin_type,
            regime,
            checks,
            primary_reject_reason: sim_result.error_code.clone(),
            kill_switch: None,
            plan_hash: Some(plan_hash),
            simulate: Some(sim_result),
            send: None,
            outcome: DecisionOutcome::SimFailed,
            config_snapshot_id: None,
            input_snapshots: HashMap::new(),
        }
    }

    pub fn with_config_snapshot(mut self, snapshot_id: String) -> Self {
        self.config_snapshot_id = Some(snapshot_id);
        self
    }

    pub fn with_input_snapshot(mut self, key: String, value: String) -> Self {
        self.input_snapshots.insert(key, value);
        self
    }
}

// ============================================================================
// ExecutionResult (produced by execution-engine after send/confirm)
// ============================================================================

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionStatus {
    Sent,
    Confirmed,
    Failed,
    Timeout,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FillStatus {
    /// Both legs (fill_in and fill_out) are present.
    Complete,
    /// At least one of fill_in / fill_out is present, but not both.
    Partial,
    /// Neither fill_in nor fill_out could be determined.
    Unavailable,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FillUnavailableReason {
    /// RPC fetch of confirmed tx failed.
    RpcTxFetchFailed,
    /// Transaction meta missing.
    TxMetaMissing,
    /// Wallet pubkey not found in message account keys.
    WalletAccountIndexMissing,
    /// SOL lamport delta inference was gated because the tx showed account create/close noise.
    LamportDeltaGatedAccountLifecycleNoise,
    /// Token balance deltas were missing / insufficient to compute fills.
    TokenBalanceDeltaMissing,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionFees {
    /// Network fee in lamports
    pub network_fee_lamports: u64,
    /// Priority fee tip in lamports
    pub tip_lamports: u64,
    /// Compute units consumed
    pub compute_units: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionPnl {
    /// Gross PnL (before fees) in lamports
    pub gross_lamports: i64,
    /// Net PnL (after fees) in lamports
    pub net_lamports: i64,
    /// Units are always lamports (9 decimals for SOL)
    pub decimals: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ExecutionResult {
    #[serde(flatten)]
    pub header: RecordHeader,

    pub execution_id: String,
    pub decision_id: String,
    pub intent_id: String,
    /// P1: Source strategy/worker for attribution (e.g., "momentum-bot", "arb-strategy")
    pub source: String,

    /// Token mint being traded (for BUY: output_mint, for SELL: input_mint)
    /// This allows downstream consumers (Grafana, analytics) to identify trades
    /// without joining with decision records.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token_mint: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,

    pub status: ExecutionStatus,

    /// Actual fills computed from confirmed transaction meta (best-effort).
    ///
    /// `fill_in` is the actual amount-in for the intent (positive raw units).
    /// `fill_out` is the actual amount-out for the intent (positive raw units).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_in: Option<ExplicitAmount>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_out: Option<ExplicitAmount>,

    /// Fill diagnostics for downstream consumers.
    ///
    /// Backward compatible: older producers omit these fields.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_status: Option<FillStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fill_unavailable_reason: Option<FillUnavailableReason>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub confirmed_slot: Option<u64>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub fees: Option<ExecutionFees>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub pnl: Option<ExecutionPnl>,

    /// Wallet SOL balance delta (lamports)
    ///
    /// This is the actual change in the wallet's SOL balance from pre to post,
    /// including all fees, rent, and protocol costs.
    /// Positive = wallet gained SOL, Negative = wallet lost SOL
    ///
    /// Note: This differs from fill_in/fill_out which track token/WSOL amounts.
    /// For SELL: wallet_sol_delta shows the net SOL received after all fees
    /// For BUY: wallet_sol_delta shows the total SOL spent including fees
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wallet_sol_delta_lamports: Option<i128>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,

    /// Duration from intent received to confirmation (ms)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
}

impl ExecutionResult {
    #[allow(clippy::too_many_arguments)]
    pub fn new_sent(
        component: &str,
        build: &str,
        run_id: &str,
        execution_id: String,
        decision_id: String,
        intent_id: String,
        source: String,
        token_mint: Option<String>,
        signature: Option<String>,
        bundle_id: Option<String>,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            execution_id,
            decision_id,
            intent_id,
            source,
            token_mint,
            signature,
            bundle_id,
            status: ExecutionStatus::Sent,
            fill_in: None,
            fill_out: None,
            fill_status: None,
            fill_unavailable_reason: None,
            wallet_sol_delta_lamports: None,
            confirmed_slot: None,
            fees: None,
            pnl: None,
            error_message: None,
            latency_ms: None,
        }
    }

    pub fn with_fills(
        mut self,
        fill_in: Option<ExplicitAmount>,
        fill_out: Option<ExplicitAmount>,
    ) -> Self {
        self.fill_in = fill_in;
        self.fill_out = fill_out;
        self
    }

    pub fn with_fill_diagnostics(
        mut self,
        fill_status: FillStatus,
        fill_unavailable_reason: Option<FillUnavailableReason>,
    ) -> Self {
        self.fill_status = Some(fill_status);
        self.fill_unavailable_reason = fill_unavailable_reason;
        self
    }

    pub fn with_sol_delta(mut self, delta_lamports: i128) -> Self {
        self.wallet_sol_delta_lamports = Some(delta_lamports);
        self
    }

    pub fn mark_confirmed(
        mut self,
        slot: u64,
        fees: ExecutionFees,
        pnl: ExecutionPnl,
        latency_ms: u64,
    ) -> Self {
        self.status = ExecutionStatus::Confirmed;
        self.confirmed_slot = Some(slot);
        self.fees = Some(fees);
        self.pnl = Some(pnl);
        self.latency_ms = Some(latency_ms);
        self
    }

    pub fn mark_failed(mut self, error: String) -> Self {
        self.status = ExecutionStatus::Failed;
        self.error_message = Some(error);
        self
    }
}

// ============================================================================
// ConfigUpdate (from control-plane for runtime config changes)
// ============================================================================

/// Configuration update request from control-plane
///
/// Allows runtime parameter changes without restarting services.
/// Per ROLE_SEPARATION.md: only admin role can send these via control-plane.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfigUpdate {
    #[serde(flatten)]
    pub header: RecordHeader,

    /// Command type (always "config_update" from control-plane)
    pub command: String,

    /// Target component: "execution-engine", "momentum-bot", "market-data"
    /// Renamed from "component" to avoid collision with RecordHeader.component (sender)
    #[serde(rename = "target_component")]
    pub target_component: String,

    /// Key-value pairs to update
    pub config: HashMap<String, serde_json::Value>,

    /// ISO timestamp from control-plane
    pub timestamp: String,
}

impl ConfigUpdate {
    pub fn new(
        component: &str,
        build: &str,
        run_id: &str,
        target: &str,
        config: HashMap<String, serde_json::Value>,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            command: "config_update".to_string(),
            target_component: target.to_string(),
            config,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Get a config value as u64
    pub fn get_u64(&self, key: &str) -> Option<u64> {
        self.config.get(key).and_then(|v| v.as_u64())
    }

    /// Get a config value as f64
    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.config.get(key).and_then(|v| v.as_f64())
    }

    /// Get a config value as bool
    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.config.get(key).and_then(|v| v.as_bool())
    }

    /// Get a config value as string
    pub fn get_string(&self, key: &str) -> Option<&str> {
        self.config.get(key).and_then(|v| v.as_str())
    }
}

/// Response to a config update request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConfigUpdateResponse {
    pub status: ConfigUpdateStatus,
    pub applied_keys: Vec<String>,
    pub rejected_keys: Vec<(String, String)>, // (key, reason)
    pub new_snapshot_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConfigUpdateStatus {
    Applied,
    PartiallyApplied,
    Rejected,
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_explicit_amount() {
        let amt = ExplicitAmount::new(1_000_000_000, 9); // 1 SOL
        assert_eq!(amt.raw, 1_000_000_000);
        assert_eq!(amt.decimals, 9);
        assert_eq!(amt.ui, Some(Decimal::from(1)));
    }

    #[test]
    fn test_market_event_serialization() {
        let event = MarketEvent::new(
            "market-data",
            "v0.1.0",
            "run-123",
            "evt-001".to_string(),
            "geyser",
            Some(12345),
            MarketEventKind::SlotUpdate {
                current_slot: 12345,
            },
        );

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("schema_version"));
        assert!(json.contains("event_id"));
        assert!(json.contains("SlotUpdate"));

        let parsed: MarketEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.event_id, "evt-001");
    }

    #[test]
    fn test_trade_intent_serialization() {
        let intent = TradeIntent::new(
            "momentum-bot",
            "v0.1.0",
            "run-456",
            "intent-001".to_string(),
            "momentum-bot",
            IntentTier::Tier1,
            IntentOrigin::StrategyA,
            ExplicitAmount::new(100_000_000, 9), // 0.1 SOL
            TradeResources {
                input_mint: "So11111111111111111111111111111111111111112".to_string(),
                output_mint: "TokenMint123".to_string(),
                pools: vec!["Pool123".to_string()],
                accounts: vec![],
                token_program: None,
            },
            50,  // 0.5% expected ROI
            100, // 1% max slippage
            TradeSide::Buy,
            TradingRegime::Early,
        );

        let json = serde_json::to_string(&intent).unwrap();
        assert!(json.contains("intent_id"));
        assert!(json.contains("required_capital"));
        assert!(json.contains("StrategyA"));

        let parsed: TradeIntent = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.intent_id, "intent-001");
        assert_eq!(parsed.tier, IntentTier::Tier1);
    }

    #[test]
    fn test_decision_record_rejected() {
        let checks = vec![
            CheckResult {
                check_name: "ttl_valid".to_string(),
                passed: true,
                reason_code: None,
                details: None,
            },
            CheckResult {
                check_name: "risk_limit".to_string(),
                passed: false,
                reason_code: Some("RISK_LIMIT_EXCEEDED".to_string()),
                details: Some("Daily loss limit reached".to_string()),
            },
        ];

        let record = DecisionRecord::new_rejected(
            "execution-engine",
            "v0.1.0",
            "run-789",
            "dec-001".to_string(),
            "intent-001".to_string(),
            "momentum-bot".to_string(),
            IntentOrigin::StrategyA,
            TradingRegime::Early,
            checks,
            "RISK_LIMIT_EXCEEDED".to_string(),
        );

        let json = serde_json::to_string(&record).unwrap();
        assert!(json.contains("decision_id"));
        assert!(json.contains("RISK_LIMIT_EXCEEDED"));
        assert!(json.contains("Rejected"));

        let parsed: DecisionRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.outcome, DecisionOutcome::Rejected);
    }

    #[test]
    fn test_execution_result_serialization() {
        let result = ExecutionResult::new_sent(
            "execution-engine",
            "v0.1.0",
            "run-789",
            "exec-001".to_string(),
            "dec-001".to_string(),
            "intent-001".to_string(),
            "momentum-bot".to_string(),
            Some("EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v".to_string()),
            Some("5abc123...".to_string()),
            None,
        );

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("execution_id"));
        assert!(json.contains("Sent"));

        let parsed: ExecutionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.status, ExecutionStatus::Sent);
    }
}

// ============================================================================
// Pool Cache Updates (for market-data → execution-engine propagation)
// ============================================================================

/// Pool cache update type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum PoolCacheUpdateType {
    /// New pool discovered
    PoolDiscovered,
    /// Existing pool balance updated
    BalanceUpdated,
    /// Pool removed/closed
    PoolRemoved,
}

/// Pool metadata for cache propagation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolCacheUpdate {
    #[serde(flatten)]
    pub header: RecordHeader,

    /// Type of update
    pub update_type: PoolCacheUpdateType,

    /// Pool address (base58 string)
    pub pool_address: String,

    /// DEX identifier (raydium, orca, meteora_dlmm, meteora_cpmm, pump_amm)
    pub dex: String,

    /// Base mint address
    pub base_mint: String,

    /// Quote mint address
    pub quote_mint: String,

    /// Base token reserve (raw amount)
    pub base_reserve: u64,

    /// Quote token reserve (raw amount)
    pub quote_reserve: u64,

    /// Liquidity in lamports (for filtering/sorting)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liquidity_lamports: Option<u64>,

    /// Geyser slot when update occurred
    pub geyser_slot: u64,

    /// Additional DEX-specific metadata (JSON string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<HashMap<String, String>>,
}

impl PoolCacheUpdate {
    pub fn new_pool_discovered(
        component: &str,
        build: &str,
        run_id: &str,
        pool_address: String,
        dex: String,
        base_mint: String,
        quote_mint: String,
        base_reserve: u64,
        quote_reserve: u64,
        liquidity_lamports: Option<u64>,
        geyser_slot: u64,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            update_type: PoolCacheUpdateType::PoolDiscovered,
            pool_address,
            dex,
            base_mint,
            quote_mint,
            base_reserve,
            quote_reserve,
            liquidity_lamports,
            geyser_slot,
            metadata: None,
        }
    }

    pub fn new_balance_updated(
        component: &str,
        build: &str,
        run_id: &str,
        pool_address: String,
        dex: String,
        base_mint: String,
        quote_mint: String,
        base_reserve: u64,
        quote_reserve: u64,
        geyser_slot: u64,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            update_type: PoolCacheUpdateType::BalanceUpdated,
            pool_address,
            dex,
            base_mint,
            quote_mint,
            base_reserve,
            quote_reserve,
            liquidity_lamports: None,
            geyser_slot,
            metadata: None,
        }
    }

    pub fn new_pool_removed(
        component: &str,
        build: &str,
        run_id: &str,
        pool_address: String,
        dex: String,
        geyser_slot: u64,
    ) -> Self {
        Self {
            header: RecordHeader::new(component, build, run_id),
            update_type: PoolCacheUpdateType::PoolRemoved,
            pool_address,
            dex,
            base_mint: String::new(),
            quote_mint: String::new(),
            base_reserve: 0,
            quote_reserve: 0,
            liquidity_lamports: None,
            geyser_slot,
            metadata: None,
        }
    }
}
