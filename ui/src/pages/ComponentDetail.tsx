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
  ],
}

// Config field descriptions (for tooltips/help text)
const CONFIG_DESCRIPTIONS: Record<string, Record<string, string>> = {
  'momentum-bot': {
    // Entry Filters
    early_min_liquidity_sol: 'ENTRY: Min liquidity for early-stage tokens (SOL)',
    established_min_liquidity_sol: 'ENTRY: Min liquidity for established tokens (SOL)',
    early_slot_threshold: 'ENTRY: Slots until token considered "established"',
    early_max_slippage_bps: 'ENTRY: Max slippage for early volatile stage (bps, 100=1%)',
    established_max_slippage_bps: 'ENTRY: Max slippage for stable pools (bps)',
    default_position_lamports: 'ENTRY: Default position size (lamports, 1e9=1 SOL)',
    
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
    min_spread_bps: 'Min spread between DEX prices (bps, 100=1%)',
    min_profit_lamports: 'Min net profit after estimated tx cost (lamports, 1e9=1 SOL)',
    max_position_lamports: 'Max notional per arb intent (lamports, 1e9=1 SOL)',
    est_tx_cost_lamports: 'Estimated tx cost used for net profit gating (lamports)',
    max_slippage_bps: 'Max slippage included in intent (bps)',
    intent_cooldown_ms: 'Cooldown per mint/pair before emitting another intent (ms)',
    intent_ttl_ms: 'Intent time-to-live (ms)',
  },
  'execution-engine': {
    max_position_size_lamports: 'RISK: Max single position size (lamports, 1e9=1 SOL)',
    daily_loss_limit_lamports: 'RISK: Kill-switch daily loss limit (lamports)',
    max_open_positions: 'RISK: Max concurrent open positions',
    max_slippage_bps: 'RISK: Max allowed slippage (bps)',
    simulation_timeout_ms: 'Ops: Simulation timeout (ms)',
    confirmation_timeout_ms: 'Ops: Confirmation timeout (ms)',
    send_enabled: 'Ops: If true, engine sends txs; if false, simulate-gated only',
  },
}

// Config parameter grouping (for better UI structure)
const CONFIG_GROUPS: Record<string, Record<string, string[]>> = {
  'momentum-bot': {
    'Entry Filters': [
      'early_min_liquidity_sol',
      'established_min_liquidity_sol',
      'early_slot_threshold',
      'early_max_slippage_bps',
      'established_max_slippage_bps',
      'default_position_lamports',
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
    'Arbitrage (Active Knobs)': [
      'min_spread_bps',
      'min_profit_lamports',
      'est_tx_cost_lamports',
      'max_position_lamports',
      'max_slippage_bps',
      'intent_cooldown_ms',
      'intent_ttl_ms',
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
    min_spread_bps: 50,
    min_profit_lamports: 10_000_000,
    max_position_lamports: 1_000_000_000,
    est_tx_cost_lamports: 50_000,
    max_slippage_bps: 100,
    intent_cooldown_ms: 5_000,
    intent_ttl_ms: 3_000,
  },
  'market-data': {},
  'execution-engine': {
    max_position_size_lamports: 500_000_000,
    daily_loss_limit_lamports: 5_000_000_000,
    max_open_positions: 5,
    max_slippage_bps: 500,
    simulation_timeout_ms: 2_000,
    confirmation_timeout_ms: 30_000,
    send_enabled: false,
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
      
      // Merge with defaults if config is empty
      const loadedConfig = data.config && Object.keys(data.config).length > 0
        ? data.config
        : DEFAULT_CONFIGS[component] || {}
      
      setConfig(loadedConfig)
      setConfigDraft(loadedConfig)
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
