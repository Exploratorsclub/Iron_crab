import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import type { ComponentConfig, MetricsData, ConfigValue } from '../types'

const CONTROL_PLANE = 'http://127.0.0.1:8080'

async function fetchJson<T>(path: string): Promise<T> {
  const apiKey = localStorage.getItem('ironcrab_api_key') || ''
  const response = await fetch(path, {
    headers: {
      Accept: 'application/json',
      ...(apiKey ? { 'X-API-Key': apiKey } : {}),
    },
  })

  if (!response.ok) {
    const text = await response.text().catch(() => '')
    throw new Error(`${response.status} ${response.statusText}${text ? `: ${text}` : ''}`)
  }

  return (await response.json()) as T
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const apiKey = localStorage.getItem('ironcrab_api_key') || ''
  const response = await fetch(path, {
    method: 'POST',
    headers: {
      Accept: 'application/json',
      'Content-Type': 'application/json',
      ...(apiKey ? { 'X-API-Key': apiKey } : {}),
    },
    body: JSON.stringify(body),
  })

  if (!response.ok) {
    const text = await response.text().catch(() => '')
    throw new Error(`${response.status} ${response.statusText}${text ? `: ${text}` : ''}`)
  }

  return (await response.json()) as T
}

function parsePrometheusMetrics(text: string): MetricsData {
  const lines = text.split('\n')
  const result: MetricsData = {}

  for (const line of lines) {
    if (line.startsWith('#') || line.trim() === '') continue

    const match = line.match(/^([a-zA-Z_][a-zA-Z0-9_]*(?:\{[^}]*\})?) (.+)$/)
    if (!match) continue

    const key = match[1]
    const value = parseFloat(match[2])
    result[key] = Number.isNaN(value) ? match[2] : value
  }

  return result
}

// Component-specific relevant metrics
const RELEVANT_METRICS: Record<string, string[]> = {
  'market-data': [
    'market_events_published_total',
    'tokens_tracked_total',
    'pools_tracked_total',
    'nats_messages_published_total',
  ],
  'momentum-bot': [
    'intents_generated_total',
    'filter_passed_total',
    'filter_rejected_total',
    'filter_rejection_by_reason',
  ],
  'arb-strategy': [
    'arb_triangle_opportunities_total',
    'intents_generated_total',
    'market_events_consumed_total',
    'nats_messages_received_total',
    'nats_messages_published_total',
    'pools_tracked_gauge',
    'tokens_tracked_gauge',
  ],
  'execution-engine': [
    'intents_received_total',
    'intents_executed_total',
    'intents_rejected_total',
    'tx_confirmed_total',
    'simulation_failures_total',
    // WSOL Manager metrics
    'wsol_balance_lamports',
    'wsol_wrap_total',
    'wsol_unwrap_total',
    // Account Janitor metrics
    'janitor_close_ata_total',
    'janitor_sol_recovered_lamports',
    'janitor_merge_dust_total',
    'janitor_swap_dust_total',
    'janitor_swap_dust_sol_recovered_lamports',
  ],
}

// Config field descriptions (for tooltips/help text)
const CONFIG_DESCRIPTIONS: Record<string, Record<string, string>> = {
  'market-data': {
    // DEX Discovery Toggles
    enable_raydium: 'DEX: Enable Raydium AMM V4 pool discovery',
    enable_raydium_cpmm: 'DEX: Enable Raydium CPMM (concentrated liquidity) discovery',
    enable_orca: 'DEX: Enable Orca Whirlpool discovery',
    enable_pumpfun: 'DEX: Enable PumpFun bonding curve discovery',
    enable_pumpswap: 'DEX: Enable PumpSwap AMM (graduated tokens) discovery',
    enable_meteora_dlmm: 'DEX: Enable Meteora DLMM (dynamic AMM) discovery',
    enable_meteora_cpmm: 'DEX: Enable Meteora CPMM discovery',
    // Rate Limiting
    max_events_per_sec: 'RATE: Max MarketEvents per second (throttle)',
  },
  'momentum-bot': {
    // Entry Filters
    early_min_liquidity_sol: 'ENTRY: Min liquidity for early-stage tokens (SOL)',
    established_min_liquidity_sol: 'ENTRY: Min liquidity for established tokens (SOL)',
    early_slot_threshold: 'ENTRY: Slots until token considered "established"',
    early_max_slippage_bps: 'ENTRY: Max slippage for early volatile stage (bps, 100=1%)',
    established_max_slippage_bps: 'ENTRY: Max slippage for stable pools (bps)',
    default_position_lamports: 'ENTRY: Default position size (lamports, 1e9=1 SOL)',
    
    // Momentum v2: Probe + Scale-In
    probe_buy_pct: 'V2 ENTRY: Probe-buy size as fraction (0.25 = 25% of position)',
    scale_in_confirm_window_secs: 'V2 ENTRY: Seconds after probe to allow scale-in',
    
    // Token Safety
    require_mint_authority_renounced: 'SAFETY: Require mint authority = None before entry',
    require_freeze_authority_none: 'SAFETY: Require freeze authority = None before entry',
    
    // Filter 1: Liquidity
    max_dev_supply_pct: 'FILTER 1: Max dev supply percentage (0-100)',
    lp_removal_window_secs: 'FILTER 1: Track LP removals for N seconds',
    
    // Filter 2: Buyer Velocity
    min_unique_buyers: 'FILTER 2: Min unique buyers in window',
    buyer_window_secs: 'FILTER 2: Time window for buyer tracking (seconds)',
    min_trades_per_sec: 'FILTER 2: Min trades per second for momentum',
    min_buy_dominance: 'FILTER 2: Min buy ratio (0.0-1.0, 0.45=45%)',
    
    // Filter 3: SOL Inflow
    min_sol_inflow_lamports: 'FILTER 3: Min net SOL inflow (lamports, 1e9=1 SOL)',
    inflow_window_secs: 'FILTER 3: Time window for SOL inflow tracking (seconds)',
    max_single_dump_lamports: 'FILTER 3: Max allowed single sell (lamports)',
    
    // Filter 4: Dev Behavior
    dev_early_sell_window_secs: 'FILTER 4: Dev sells in first N secs = bad signal',
    dev_rebuy_positive: 'FILTER 4: Dev rebuy is positive signal (true/false)',
    
    // Buyer Quality (Anti-Bot)
    top1_buyer_share_cap: 'ANTI-BOT: Max share for top-1 buyer (0.35 = 35%)',
    top3_buyer_share_cap: 'ANTI-BOT: Max combined share for top-3 buyers (0.60 = 60%)',
    repeat_buyer_min_ratio: 'ANTI-BOT: Min ratio of repeat buyers (0.05 = 5%)',
    
    // Micro-Buy Spam Detection
    min_trade_size_lamports: 'MICRO-BUY: Min trade size to count as real (lamports)',
    small_buy_ratio_cap: 'MICRO-BUY: Max ratio of small buys (0.85 = 85%)',
    
    // Dump-Recovery Gate
    dump_recovery_window_secs: 'DUMP: Time window to detect dump (seconds)',
    dump_recovery_min_buy_dominance: 'DUMP: Min buy dominance for recovery (0.55 = 55%)',
    dump_recovery_min_net_inflow_lamports: 'DUMP: Min net inflow for recovery (lamports)',
    dump_recovery_min_recovery_secs: 'DUMP: Min seconds of recovery before entry',
    
    // CTO Mode (Community Takeover)
    cto_enabled: 'CTO: Enable CTO mode (wait for recovery after dev sells)',
    cto_entry_delay_secs: 'CTO: Delay before allowing entry after dev sell (seconds)',
    cto_confirm_window_secs: 'CTO: Window to confirm recovery (seconds)',
    cto_min_unique_buyers: 'CTO: Min unique buyers for recovery confirmation',
    cto_min_buy_dominance: 'CTO: Min buy dominance for recovery (0.55 = 55%)',
    cto_min_net_inflow_lamports: 'CTO: Min net inflow for recovery (lamports)',
    
    // Exit Strategy
    hard_stop_loss_pct: 'EXIT: Hard stop-loss from entry (%, 15=15%)',
    trailing_stop_pct: 'EXIT: Trailing stop from ATH (%, 20=20%)',
    trailing_activation_pct: 'EXIT: Min profit to activate trailing (%, 10=10%)',
    take_profit_pct: 'EXIT: Take profit target (%, 100=2x)',
    max_hold_time_secs: 'EXIT: Max hold time before forced exit (seconds)',
    momentum_exit_buy_ratio: 'EXIT: Min buy ratio to stay in (0.0-1.0)',
    momentum_exit_window_secs: 'EXIT: Check last N seconds for momentum',
    momentum_exit_min_trades: 'EXIT: Min trades needed to evaluate exit',
  },
  'arb-strategy': {
    // 2-Hop Arbitrage
    two_hop_enabled: '2-HOP: Enable 2-hop arbitrage (A→B on DEX1, B→A on DEX2)',
    min_spread_bps: '2-HOP: Min spread between DEX prices (bps, 100=1%)',
    min_profit_lamports: '2-HOP: Min net profit after tx cost (lamports)',
    max_position_lamports: '2-HOP: Max notional per intent (lamports)',
    est_tx_cost_lamports: '2-HOP: Estimated tx cost for profit calc (lamports)',
    max_slippage_bps: '2-HOP: Max slippage included in intent (bps)',
    intent_cooldown_ms: '2-HOP: Cooldown per pair before next intent (ms)',
    intent_ttl_ms: '2-HOP: Intent time-to-live (ms)',
    // Multi-Hop Arbitrage
    multi_hop_enabled: 'MULTI-HOP: Enable multi-hop cycle detection (3+ hops)',
    multi_hop_shadow_mode: 'MULTI-HOP: Shadow mode (log opportunities, no intents)',
    multi_hop_max_hops: 'MULTI-HOP: Maximum hops in a cycle (3-5)',
    multi_hop_beam_width: 'MULTI-HOP: Beam width for search algorithm',
    multi_hop_min_profit_bps: 'MULTI-HOP: Min profit to emit intent (bps, 30=0.3%)',
    multi_hop_max_cycles: 'MULTI-HOP: Max cycles to return per search',
    multi_hop_pool_alternatives: 'MULTI-HOP: Pool alternatives to keep per hop',
    multi_hop_min_liquidity_usd: 'MULTI-HOP: Min pool liquidity (USD)',
    multi_hop_input_lamports: 'MULTI-HOP: Input amount for arb (lamports)',
    multi_hop_min_price_change_bps: 'MULTI-HOP: Min price change to trigger search (bps)',
    multi_hop_token_cooldown_ms: 'MULTI-HOP: Cooldown per token before re-search (ms)',
  },
  'execution-engine': {
    max_position_size_lamports: 'RISK: Max single position size (lamports, 1e9=1 SOL)',
    daily_loss_limit_lamports: 'RISK: Kill-switch daily loss limit (lamports)',
    max_open_positions: 'RISK: Max concurrent open positions',
    max_slippage_bps: 'RISK: Max allowed slippage (bps)',
    simulation_timeout_ms: 'Ops: Simulation timeout (ms)',
    confirmation_timeout_ms: 'Ops: Confirmation timeout (ms)',
    send_enabled: 'Ops: If true, engine sends txs; if false, simulate-gated only',
    // WSOL Manager
    wsol_enabled: 'WSOL: Enable automatic WSOL management',
    wsol_min_wsol_sol: 'WSOL: Wrap trigger - wrap when WSOL below this (SOL)',
    wsol_target_wsol_sol: 'WSOL: Target WSOL balance after wrap (SOL)',
    wsol_max_wsol_sol: 'WSOL: Unwrap trigger - unwrap when WSOL above this (SOL)',
    wsol_min_native_sol: 'WSOL: Reserve native SOL for rent (SOL)',
    wsol_cooldown_secs: 'WSOL: Cooldown between wrap/unwrap actions (seconds)',
    wsol_dry_run: 'WSOL: Dry-run mode (log only, no TX)',
    // Account Janitor
    janitor_enabled: 'JANITOR: Enable account cleanup background task',
    janitor_dry_run: 'JANITOR: Dry-run mode (log only, no TX)',
    // Close ATAs
    janitor_close_ata_interval_secs: 'CLOSE ATA: Interval for closing empty ATAs (seconds)',
    janitor_close_ata_min_age_secs: 'CLOSE ATA: Min age before closing empty ATA (seconds)',
    janitor_close_ata_max_per_run: 'CLOSE ATA: Max ATAs to close per run',
    // Merge Dust
    janitor_merge_dust_enabled: 'MERGE: Enable merging duplicate ATAs for same token',
    janitor_merge_dust_interval_secs: 'MERGE: Interval for merge runs (seconds)',
    janitor_merge_dust_max_per_run: 'MERGE: Max tokens to merge per run',
    // Swap Dust
    janitor_swap_dust_enabled: 'SWAP: Enable swapping dust tokens to SOL',
    janitor_swap_dust_interval_secs: 'SWAP: Interval for swap runs (seconds)',
    janitor_swap_dust_min_value_sol: 'SWAP: Min token value to swap (SOL)',
    janitor_swap_dust_max_slippage_bps: 'SWAP: Max slippage for dust swaps (bps)',
    janitor_swap_dust_max_per_run: 'SWAP: Max tokens to swap per run',
  },
}

// Config parameter grouping (for better UI structure)
const CONFIG_GROUPS: Record<string, Record<string, string[]>> = {
  'market-data': {
    'DEX Discovery': [
      'enable_raydium',
      'enable_raydium_cpmm',
      'enable_orca',
      'enable_pumpfun',
      'enable_pumpswap',
      'enable_meteora_dlmm',
      'enable_meteora_cpmm',
    ],
    'Rate Limiting': [
      'max_events_per_sec',
    ],
  },
  'momentum-bot': {
    'Entry Filters': [
      'early_min_liquidity_sol',
      'established_min_liquidity_sol',
      'early_slot_threshold',
      'early_max_slippage_bps',
      'established_max_slippage_bps',
      'default_position_lamports',
    ],
    'V2 Entry (Probe + Scale-In)': [
      'probe_buy_pct',
      'scale_in_confirm_window_secs',
    ],
    'Token Safety': [
      'require_mint_authority_renounced',
      'require_freeze_authority_none',
    ],
    'Filter 1: Liquidity': [
      'max_dev_supply_pct',
      'lp_removal_window_secs',
    ],
    'Filter 2: Buyer Velocity': [
      'min_unique_buyers',
      'buyer_window_secs',
      'min_trades_per_sec',
      'min_buy_dominance',
    ],
    'Filter 3: SOL Inflow': [
      'min_sol_inflow_lamports',
      'inflow_window_secs',
      'max_single_dump_lamports',
    ],
    'Filter 4: Dev Behavior': [
      'dev_early_sell_window_secs',
      'dev_rebuy_positive',
    ],
    'Anti-Bot (Buyer Quality)': [
      'top1_buyer_share_cap',
      'top3_buyer_share_cap',
      'repeat_buyer_min_ratio',
    ],
    'Micro-Buy Spam Detection': [
      'min_trade_size_lamports',
      'small_buy_ratio_cap',
    ],
    'Dump-Recovery Gate': [
      'dump_recovery_window_secs',
      'dump_recovery_min_buy_dominance',
      'dump_recovery_min_net_inflow_lamports',
      'dump_recovery_min_recovery_secs',
    ],
    'CTO Mode (Community Takeover)': [
      'cto_enabled',
      'cto_entry_delay_secs',
      'cto_confirm_window_secs',
      'cto_min_unique_buyers',
      'cto_min_buy_dominance',
      'cto_min_net_inflow_lamports',
    ],
    'Exit Strategy': [
      'hard_stop_loss_pct',
      'trailing_stop_pct',
      'trailing_activation_pct',
      'take_profit_pct',
      'max_hold_time_secs',
      'momentum_exit_buy_ratio',
      'momentum_exit_window_secs',
      'momentum_exit_min_trades',
    ],
  },
  'arb-strategy': {
    '2-Hop Arbitrage': [
      'two_hop_enabled',
      'min_spread_bps',
      'min_profit_lamports',
      'est_tx_cost_lamports',
      'max_position_lamports',
      'max_slippage_bps',
      'intent_cooldown_ms',
      'intent_ttl_ms',
    ],
    'Multi-Hop Arbitrage (3+ Hops)': [
      'multi_hop_enabled',
      'multi_hop_shadow_mode',
      'multi_hop_max_hops',
      'multi_hop_beam_width',
      'multi_hop_min_profit_bps',
      'multi_hop_max_cycles',
      'multi_hop_pool_alternatives',
      'multi_hop_min_liquidity_usd',
      'multi_hop_input_lamports',
      'multi_hop_min_price_change_bps',
      'multi_hop_token_cooldown_ms',
    ],
  },
  'execution-engine': {
    'Risk (Enforced)': [
      'max_position_size_lamports',
      'daily_loss_limit_lamports',
      'max_open_positions',
      'max_slippage_bps',
    ],
    'Operational': [
      'simulation_timeout_ms',
      'confirmation_timeout_ms',
      'send_enabled',
    ],
    'WSOL Manager': [
      'wsol_enabled',
      'wsol_min_wsol_sol',
      'wsol_target_wsol_sol',
      'wsol_max_wsol_sol',
      'wsol_min_native_sol',
      'wsol_cooldown_secs',
      'wsol_dry_run',
    ],
    'Janitor: Close ATAs': [
      'janitor_enabled',
      'janitor_close_ata_interval_secs',
      'janitor_close_ata_min_age_secs',
      'janitor_close_ata_max_per_run',
    ],
    'Janitor: Merge Dust': [
      'janitor_merge_dust_enabled',
      'janitor_merge_dust_interval_secs',
      'janitor_merge_dust_max_per_run',
    ],
    'Janitor: Swap Dust → SOL': [
      'janitor_swap_dust_enabled',
      'janitor_swap_dust_interval_secs',
      'janitor_swap_dust_min_value_sol',
      'janitor_swap_dust_max_slippage_bps',
      'janitor_swap_dust_max_per_run',
      'janitor_dry_run',
    ],
  },
}

// Default config values for each component (from server config)
const DEFAULT_CONFIGS: Record<string, ComponentConfig> = {
  'momentum-bot': {
    // Entry Filters
    early_min_liquidity_sol: 3.0,
    established_min_liquidity_sol: 10.0,
    early_slot_threshold: 1000,
    early_max_slippage_bps: 500,
    established_max_slippage_bps: 200,
    default_position_lamports: 5000000, // 0.005 SOL
    
    // V2 Entry (Probe + Scale-In)
    probe_buy_pct: 0.25,
    scale_in_confirm_window_secs: 30,
    
    // Token Safety
    require_mint_authority_renounced: false,
    require_freeze_authority_none: false,
    
    // Filter 1: Liquidity
    max_dev_supply_pct: 95.0,
    lp_removal_window_secs: 60,
    
    // Filter 2: Buyer Velocity
    min_unique_buyers: 3,
    buyer_window_secs: 120,
    min_trades_per_sec: 0.02,
    min_buy_dominance: 0.45,
    
    // Filter 3: SOL Inflow
    min_sol_inflow_lamports: 500000000, // 0.5 SOL
    inflow_window_secs: 60,
    max_single_dump_lamports: 20000000000, // 20 SOL
    
    // Filter 4: Dev Behavior
    dev_early_sell_window_secs: 90,
    dev_rebuy_positive: true,
    
    // Anti-Bot (Buyer Quality)
    top1_buyer_share_cap: 0.35,
    top3_buyer_share_cap: 0.60,
    repeat_buyer_min_ratio: 0.05,
    
    // Micro-Buy Spam Detection
    min_trade_size_lamports: 10000000, // 0.01 SOL
    small_buy_ratio_cap: 0.85,
    
    // Dump-Recovery Gate
    dump_recovery_window_secs: 30,
    dump_recovery_min_buy_dominance: 0.55,
    dump_recovery_min_net_inflow_lamports: 1000000000, // 1 SOL
    dump_recovery_min_recovery_secs: 10,
    
    // CTO Mode (Community Takeover)
    cto_enabled: false,
    cto_entry_delay_secs: 30,
    cto_confirm_window_secs: 30,
    cto_min_unique_buyers: 5,
    cto_min_buy_dominance: 0.55,
    cto_min_net_inflow_lamports: 1000000000, // 1 SOL
    
    // Exit Strategy
    hard_stop_loss_pct: 15.0,
    trailing_stop_pct: 20.0,
    trailing_activation_pct: 10.0,
    take_profit_pct: 100.0,
    max_hold_time_secs: 300,
    momentum_exit_buy_ratio: 0.4,
    momentum_exit_window_secs: 30,
    momentum_exit_min_trades: 5,
  },
  'arb-strategy': {
    // 2-Hop Arbitrage
    two_hop_enabled: true,
    min_spread_bps: 50,
    min_profit_lamports: 10_000_000,
    max_position_lamports: 1_000_000_000,
    est_tx_cost_lamports: 50_000,
    max_slippage_bps: 100,
    intent_cooldown_ms: 5_000,
    intent_ttl_ms: 3_000,
    // Multi-Hop Arbitrage
    multi_hop_enabled: true,
    multi_hop_shadow_mode: true,
    multi_hop_max_hops: 4,
    multi_hop_beam_width: 50,
    multi_hop_min_profit_bps: 30,
    multi_hop_max_cycles: 3,
    multi_hop_pool_alternatives: 3,
    multi_hop_min_liquidity_usd: 1000,
    multi_hop_input_lamports: 100_000_000,
    multi_hop_min_price_change_bps: 10,
    multi_hop_token_cooldown_ms: 100,
  },
  'market-data': {
    // DEX Discovery Toggles
    enable_raydium: true,
    enable_raydium_cpmm: true,
    enable_orca: true,
    enable_pumpfun: true,
    enable_pumpswap: true,
    enable_meteora_dlmm: true,
    enable_meteora_cpmm: true,
    // Rate Limiting
    max_events_per_sec: 10_000,
  },
  'execution-engine': {
    max_position_size_lamports: 500_000_000,
    daily_loss_limit_lamports: 5_000_000_000,
    max_open_positions: 5,
    max_slippage_bps: 500,
    simulation_timeout_ms: 2_000,
    confirmation_timeout_ms: 30_000,
    send_enabled: false,
    // WSOL Manager defaults
    wsol_enabled: false,
    wsol_min_wsol_sol: 0.5,
    wsol_target_wsol_sol: 1.0,
    wsol_max_wsol_sol: 2.0,
    wsol_min_native_sol: 0.1,
    wsol_cooldown_secs: 30,
    wsol_dry_run: false,
    // Account Janitor defaults
    janitor_enabled: false,
    janitor_dry_run: false,
    janitor_close_ata_interval_secs: 3600,
    janitor_close_ata_min_age_secs: 86400,
    janitor_close_ata_max_per_run: 10,
    janitor_merge_dust_enabled: false,
    janitor_merge_dust_interval_secs: 300,
    janitor_merge_dust_max_per_run: 5,
    janitor_swap_dust_enabled: false,
    janitor_swap_dust_interval_secs: 86400,
    janitor_swap_dust_min_value_sol: 0.001,
    janitor_swap_dust_max_slippage_bps: 500,
    janitor_swap_dust_max_per_run: 5,
  },
}

export function ComponentDetail() {
  const { component } = useParams<{ component: string }>()
  const [metrics, setMetrics] = useState<MetricsData | null>(null)
  const [config, setConfig] = useState<ComponentConfig | null>(null)
  const [configDraft, setConfigDraft] = useState<ComponentConfig>({})
  const [lamportsDisplayDraft, setLamportsDisplayDraft] = useState<Record<string, string>>({})
  const [newKey, setNewKey] = useState('')
  const [newValue, setNewValue] = useState('')
  const [isLoadingMetrics, setIsLoadingMetrics] = useState(false)
  const [isLoadingConfig, setIsLoadingConfig] = useState(false)
  const [isSavingConfig, setIsSavingConfig] = useState(false)
  const [metricsError, setMetricsError] = useState<string | null>(null)
  const [configError, setConfigError] = useState<string | null>(null)
  const [saveStatus, setSaveStatus] = useState<string | null>(null)

  useEffect(() => {
    if (!component) return
    void loadMetrics()
    void loadConfig()

    const interval = setInterval(() => {
      void loadMetrics()
    }, 5000) // Refresh metrics every 5s

    return () => clearInterval(interval)
  }, [component])

  async function loadMetrics() {
    if (!component) return

    setIsLoadingMetrics(true)
    setMetricsError(null)

    try {
      // Use Control-Plane proxy endpoint
      const response = await fetch(`${CONTROL_PLANE}/metrics/${component}`)
      if (!response.ok) {
        throw new Error(`Failed to fetch metrics: ${response.status}`)
      }

      const text = await response.text()
      const parsed = parsePrometheusMetrics(text)
      setMetrics(parsed)
    } catch (err) {
      setMetricsError(err instanceof Error ? err.message : String(err))
    } finally {
      setIsLoadingMetrics(false)
    }
  }

  async function loadConfig() {
    if (!component) return

    setIsLoadingConfig(true)
    setConfigError(null)

    try {
      const data = await fetchJson<{ config: ComponentConfig }>(`${CONTROL_PLANE}/config/${component}`)
      
      // Always merge server config with defaults to show all available fields
      // Server values take precedence, defaults fill in missing fields
      const defaults = DEFAULT_CONFIGS[component] || {}
      const serverConfig = data.config || {}
      const mergedConfig = { ...defaults, ...serverConfig }
      
      setConfig(mergedConfig)
      setConfigDraft(mergedConfig)
      setLamportsDisplayDraft({})
    } catch (err) {
      setConfigError(err instanceof Error ? err.message : String(err))
      
      // Fallback to defaults on error
      const fallback = DEFAULT_CONFIGS[component] || {}
      setConfig(fallback)
      setConfigDraft(fallback)
      setLamportsDisplayDraft({})
    } finally {
      setIsLoadingConfig(false)
    }
  }

  async function saveConfig() {
    if (!component) return

    setIsSavingConfig(true)
    setSaveStatus(null)

    try {
      // Resolve any in-progress *_lamports edits (user might not have blurred the input yet)
      const resolved: ComponentConfig = { ...configDraft }
      for (const [key, raw] of Object.entries(lamportsDisplayDraft)) {
        if (!isLamportsField(key)) continue
        resolved[key] = solToLamports(raw)
      }

      await postJson(`${CONTROL_PLANE}/config`, {
        component,
        config: resolved,
      })
      setSaveStatus('✅ Config saved successfully!')
      setConfig(resolved)
      setConfigDraft(resolved)
      setLamportsDisplayDraft({})
      setTimeout(() => setSaveStatus(null), 3000)
    } catch (err) {
      setSaveStatus(`❌ Error: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      setIsSavingConfig(false)
    }
  }

  function normalizeNumberString(raw: string): string {
    // Allow German decimal comma input (e.g. "0,5")
    return raw.trim().replace(',', '.')
  }

  function parseConfigValue(raw: string): ConfigValue {
    const trimmed = raw.trim()
    if (trimmed === '') return ''
    if (trimmed === 'null') return null
    if (trimmed === 'true') return true
    if (trimmed === 'false') return false
    const normalized = normalizeNumberString(trimmed)
    const num = parseFloat(normalized)
    if (!Number.isNaN(num) && /^-?\d+(\.\d+)?$/.test(normalized)) return num
    return trimmed
  }

  function updateConfigValue(key: string, value: string) {
    setConfigDraft({ ...configDraft, [key]: parseConfigValue(value) })
  }

  function addNewConfigKey() {
    if (!newKey.trim()) return
    setConfigDraft({ ...configDraft, [newKey]: parseConfigValue(newValue) })
    setNewKey('')
    setNewValue('')
  }

  function deleteConfigKey(key: string) {
    const { [key]: _, ...rest } = configDraft
    setConfigDraft(rest)
    setLamportsDisplayDraft((prev) => {
      const { [key]: __, ...next } = prev
      return next
    })
  }

  // Convert lamports to SOL for display (1e9 lamports = 1 SOL)
  function lamportsToSol(lamports: number): string {
    const sol = lamports / 1_000_000_000
    // Keep small values readable (e.g. 15,000 lamports = 0.000015 SOL)
    const fixed = sol.toFixed(9)
    return fixed.replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1')
  }

  // Convert SOL to lamports (1 SOL = 1e9 lamports)
  function solToLamports(sol: string): number {
    const normalized = normalizeNumberString(sol)
    if (normalized === '') return 0
    const parsed = parseFloat(normalized)
    if (!Number.isFinite(parsed) || parsed < 0) return 0
    return Math.floor(parsed * 1_000_000_000)
  }

  // Check if a key represents lamports (for automatic conversion)
  function isLamportsField(key: string): boolean {
    return key.endsWith('_lamports')
  }

  // Get display value (converts lamports to SOL if applicable)
  function getDisplayValue(key: string, val: ConfigValue): string {
    if (typeof val === 'number' && isLamportsField(key)) {
      const draft = lamportsDisplayDraft[key]
      if (draft !== undefined) return draft
      return lamportsToSol(val)
    }
    return String(val ?? '')
  }

  // Update value from display.
  // NOTE: For *_lamports we keep raw text while typing and only convert on blur/enter.
  function updateFromDisplay(key: string, displayValue: string) {
    if (isLamportsField(key)) {
      setLamportsDisplayDraft((prev) => ({ ...prev, [key]: displayValue }))
      return
    }
    setConfigDraft({ ...configDraft, [key]: parseConfigValue(displayValue) })
  }

  function commitLamportsDisplay(key: string) {
    if (!isLamportsField(key)) return
    const raw = lamportsDisplayDraft[key]
    if (raw === undefined) return
    const lamports = solToLamports(raw)
    setConfigDraft((prev) => ({ ...prev, [key]: lamports }))
    setLamportsDisplayDraft((prev) => {
      const { [key]: _, ...rest } = prev
      return rest
    })
  }

  // Filter relevant metrics
  const relevantKeys = RELEVANT_METRICS[component || ''] || []
  const filteredMetrics: MetricsData = {}
  if (metrics) {
    for (const [key, val] of Object.entries(metrics)) {
      const isRelevant = relevantKeys.some((pattern) => key.startsWith(pattern))
      if (isRelevant) {
        filteredMetrics[key] = val
      }
    }
  }

  return (
    <div>
      <div className="breadcrumb">
        <Link to="/">← Back to Overview</Link>
      </div>

      <h2>{component}</h2>

      {/* Key Metrics */}
      <section>
        <div className="sectionHeader">
          <strong>Key Metrics</strong>
          <button onClick={() => void loadMetrics()} disabled={isLoadingMetrics}>
            Refresh
          </button>
        </div>
        {metricsError && <div className="error">Error: {metricsError}</div>}
        {!metricsError && (
          <div className="card">
            {Object.keys(filteredMetrics).length === 0 && <div>No metrics available</div>}
            {Object.entries(filteredMetrics).map(([key, val]) => (
              <div key={key} className="kvRow">
                <span>{key}</span>
                <span>{String(val)}</span>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* Configuration Editor */}
      <section>
        <div className="sectionHeader">
          <strong>Configuration</strong>
          <button onClick={() => void loadConfig()} disabled={isLoadingConfig}>
            Reload
          </button>
        </div>
        {configError && <div className="error">Note: {configError} (showing defaults)</div>}
        <div className="card">
          {Object.keys(configDraft).length === 0 && <div>No configuration available</div>}
          
          {/* Grouped config parameters */}
          {component && CONFIG_GROUPS[component] ? (
            Object.entries(CONFIG_GROUPS[component]).map(([groupName, keys]) => (
              <div key={groupName} style={{ marginBottom: '2rem' }}>
                <h3 style={{ margin: '1rem 0 0.5rem 0', fontSize: '16px', color: '#0ea5e9', borderBottom: '1px solid #ddd', paddingBottom: '0.25rem' }}>
                  {groupName}
                </h3>
                {keys.map((key) => {
                  const val = configDraft[key]
                  if (val === undefined) return null
                  const description = CONFIG_DESCRIPTIONS[component || '']?.[key]
                  const displayValue = getDisplayValue(key, val)
                  const unit = isLamportsField(key) ? ' SOL' : ''
                  
                  return (
                    <div key={key} className="kvRow" style={{ alignItems: 'center' }}>
                      <span title={description || key} style={{ fontWeight: '500', flex: '0 0 auto', minWidth: '250px' }}>{key}</span>
                      <div style={{ display: 'flex', alignItems: 'center', flex: '1 1 auto', gap: '0.5rem' }}>
                        <input
                          type="text"
                          value={displayValue}
                          onChange={(e) => updateFromDisplay(key, e.target.value)}
                          onBlur={() => commitLamportsDisplay(key)}
                          onKeyDown={(e) => {
                            if (e.key === 'Enter') {
                              commitLamportsDisplay(key)
                            }
                          }}
                          className="textInput"
                          title={description || ''}
                          style={{ flex: '1' }}
                        />
                        {unit && <span style={{ fontWeight: 'bold', color: '#666', minWidth: '40px' }}>{unit}</span>}
                      </div>
                    </div>
                  )
                })}
              </div>
            ))
          ) : (
            /* Fallback: Ungrouped display */
            Object.entries(configDraft).map(([key, val]) => {
              const description = CONFIG_DESCRIPTIONS[component || '']?.[key]
              return (
                <div key={key} className="kvRow">
                  <span title={description || key}>{key}</span>
                  <input
                    type="text"
                    value={String(val ?? '')}
                    onChange={(e) => updateConfigValue(key, e.target.value)}
                    className="textInput"
                    title={description || ''}
                  />
                  <button onClick={() => deleteConfigKey(key)} className="danger">
                    Delete
                  </button>
                </div>
              )
            })
          )}

          {/* Add new parameter */}
          <div style={{ marginTop: '2rem', paddingTop: '1rem', borderTop: '2px solid #ddd' }}>
            <h3 style={{ margin: '0 0 0.5rem 0', fontSize: '14px' }}>Add Custom Parameter</h3>
            <div className="kvRow">
              <input
                type="text"
                placeholder="new_config_key"
                value={newKey}
                onChange={(e) => setNewKey(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && addNewConfigKey()}
                className="textInput"
              />
              <input
                type="text"
                placeholder="value"
                value={newValue}
                onChange={(e) => setNewValue(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && addNewConfigKey()}
                className="textInput"
              />
              <button onClick={addNewConfigKey}>Add</button>
            </div>
          </div>

          <div className="actions" style={{ marginTop: '1rem' }}>
            <button onClick={() => void saveConfig()} disabled={isSavingConfig} style={{ fontSize: '16px', padding: '10px 20px', fontWeight: 'bold' }}>
              {isSavingConfig ? 'Saving...' : '💾 Save Config'}
            </button>
            <button onClick={() => setConfigDraft(config || {})}>Reset Changes</button>
          </div>
          {saveStatus && <div style={{ marginTop: '0.5rem', fontSize: '14px', fontWeight: 'bold' }}>{saveStatus}</div>}
          <div className="small" style={{ marginTop: '0.5rem', background: '#f0f9ff', padding: '8px', borderRadius: '4px', border: '1px solid #0ea5e9' }}>
            <strong>💡 How it works:</strong><br/>
            • <strong>Lamports fields</strong> (ending in _lamports) are automatically converted to SOL for easier editing<br/>
            • Click <strong>"Save Config"</strong> to apply changes to the selected component (takes effect immediately if the component supports hot-reload)<br/>
            • Click <strong>"Reload"</strong> (top right) to fetch current server config<br/>
            • Click <strong>"Reset Changes"</strong> to discard unsaved edits
          </div>
        </div>
      </section>

      <section>
        <div className="small">
          For detailed metrics analysis, use <a href="http://127.0.0.1:3000" target="_blank" rel="noreferrer">Grafana</a>
        </div>
      </section>
    </div>
  )
}
