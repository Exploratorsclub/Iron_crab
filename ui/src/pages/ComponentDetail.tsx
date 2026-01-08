import { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import type { ComponentConfig, MetricsData } from '../types'

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

export function ComponentDetail() {
  const { component } = useParams<{ component: string }>()
  const [metrics, setMetrics] = useState<MetricsData | null>(null)
  const [config, setConfig] = useState<ComponentConfig | null>(null)
  const [configDraft, setConfigDraft] = useState<ComponentConfig>({})
  const [isLoadingMetrics, setIsLoadingMetrics] = useState(false)
  const [isLoadingConfig, setIsLoadingConfig] = useState(false)
  const [isSavingConfig, setIsSavingConfig] = useState(false)
  const [metricsError, setMetricsError] = useState<string | null>(null)
  const [configError, setConfigError] = useState<string | null>(null)
  const [saveStatus, setSaveStatus] = useState<string | null>(null)

  const metricsPortMap: Record<string, number> = {
    'market-data': 9801,
    'momentum-bot': 9802,
    'arb-strategy': 9803,
    'execution-engine': 9804,
  }

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
    const port = metricsPortMap[component]
    if (!port) {
      setMetricsError('Unknown component')
      return
    }

    setIsLoadingMetrics(true)
    setMetricsError(null)

    try {
      // Use control-plane proxy endpoint for CORS
      const response = await fetch(`http://127.0.0.1:8080/metrics/${component}`)
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
      // Try to get current config from control-plane
      const data = await fetchJson<{ config: ComponentConfig }>(`http://127.0.0.1:8080/config/${component}`)
      setConfig(data.config)
      setConfigDraft(data.config)
    } catch (err) {
      setConfigError(err instanceof Error ? err.message : String(err))
      setConfig({})
      setConfigDraft({})
    } finally {
      setIsLoadingConfig(false)
    }
  }

  async function saveConfig() {
    if (!component) return

    setIsSavingConfig(true)
    setSaveStatus(null)

    try {
      await postJson(`http://127.0.0.1:8080/config/${component}`, {
        component,
        config: configDraft,
      })
      setSaveStatus('Config saved successfully!')
      setConfig(configDraft)
      setTimeout(() => setSaveStatus(null), 3000)
    } catch (err) {
      setSaveStatus(`Error: ${err instanceof Error ? err.message : String(err)}`)
    } finally {
      setIsSavingConfig(false)
    }
  }

  function parsePrometheusMetrics(text: string): MetricsData {
    const lines = text.split('\n')
    const result: MetricsData = {}

    for (const line of lines) {
      if (line.startsWith('#') || !line.trim()) continue

      const parts = line.split(' ')
      if (parts.length >= 2) {
        const key = parts[0]
        const value = parts[1]
        const numValue = parseFloat(value)
        result[key] = Number.isNaN(numValue) ? value : numValue
      }
    }

    return result
  }

  function updateConfigValue(key: string, value: string) {
    let parsedValue: string | number | boolean | null = value

    // Try to parse as number
    if (value.match(/^-?\d+(\.\d+)?$/)) {
      parsedValue = parseFloat(value)
    }
    // Try to parse as boolean
    else if (value === 'true') {
      parsedValue = true
    } else if (value === 'false') {
      parsedValue = false
    } else if (value === 'null') {
      parsedValue = null
    }

    setConfigDraft({ ...configDraft, [key]: parsedValue })
  }

  function addConfigEntry(key: string) {
    if (!key || key in configDraft) return
    setConfigDraft({ ...configDraft, [key]: '' })
  }

  if (!component) {
    return <div>Component not specified</div>
  }

  const keyMetrics = getKeyMetricsForComponent(component, metrics)

  return (
    <div>
      <div className="breadcrumb">
        <Link to="/">← Overview</Link>
      </div>

      <h2>{component}</h2>

      <section>
        <div className="sectionHeader">
          <strong>Key Metrics</strong>
          <button onClick={() => void loadMetrics()} disabled={isLoadingMetrics}>
            {isLoadingMetrics ? 'Loading…' : 'Refresh'}
          </button>
        </div>
        {metricsError && <div className="error">Error: {metricsError}</div>}
        {keyMetrics.length === 0 && !metricsError && <div>No metrics available</div>}
        {keyMetrics.length > 0 && (
          <div className="card">
            {keyMetrics.map(({ label, value }) => (
              <div key={label} className="kvRow">
                <span>{label}</span>
                <span>{value}</span>
              </div>
            ))}
          </div>
        )}
      </section>

      <section>
        <div className="sectionHeader">
          <strong>All Metrics</strong>
        </div>
        {metrics && (
          <div className="metricsTable">
            <table>
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Value</th>
                </tr>
              </thead>
              <tbody>
                {Object.entries(metrics)
                  .sort(([a], [b]) => a.localeCompare(b))
                  .map(([key, value]) => (
                    <tr key={key}>
                      <td>
                        <code>{key}</code>
                      </td>
                      <td>{typeof value === 'number' ? value.toLocaleString() : value}</td>
                    </tr>
                  ))}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <section>
        <div>
          <strong>Configuration</strong>
        </div>
        {isLoadingConfig && <div>Loading config…</div>}
        {configError && <div className="error">Error: {configError}</div>}
        {config !== null && (
          <div className="card">
            {Object.entries(configDraft).map(([key, value]) => (
              <div key={key} className="kvRow">
                <span>{key}</span>
                <input
                  className="textInput"
                  value={String(value ?? '')}
                  onChange={(e) => updateConfigValue(key, e.target.value)}
                />
              </div>
            ))}
            <div className="kvRow">
              <span>
                <input
                  id="newConfigKey"
                  className="textInput"
                  placeholder="new_config_key"
                  onKeyDown={(e) => {
                    if (e.key === 'Enter') {
                      const input = e.target as HTMLInputElement
                      addConfigEntry(input.value)
                      input.value = ''
                    }
                  }}
                />
              </span>
              <span className="small">Press Enter to add</span>
            </div>
            <div className="actions">
              <button onClick={() => void saveConfig()} disabled={isSavingConfig}>
                {isSavingConfig ? 'Saving…' : 'Save Config'}
              </button>
              <button onClick={() => void loadConfig()}>Reset</button>
            </div>
            {saveStatus && <div className={saveStatus.startsWith('Error') ? 'error' : ''}>{saveStatus}</div>}
          </div>
        )}
      </section>
    </div>
  )
}

function getKeyMetricsForComponent(
  component: string,
  metrics: MetricsData | null
): Array<{ label: string; value: string }> {
  if (!metrics) return []

  const common = [
    { key: 'nats_messages_published_total', label: 'NATS Messages Published' },
    { key: 'nats_messages_received_total', label: 'NATS Messages Received' },
    { key: 'market_events_published_total', label: 'Market Events Published' },
    { key: 'market_events_consumed_total', label: 'Market Events Consumed' },
  ]

  const componentSpecific: Record<string, Array<{ key: string; label: string }>> = {
    'market-data': [
      { key: 'geyser_transactions_received', label: 'Geyser Transactions' },
      { key: 'geyser_accounts_received', label: 'Geyser Accounts' },
      { key: 'pools_discovered_total', label: 'Pools Discovered' },
    ],
    'momentum-bot': [
      { key: 'tokens_tracked', label: 'Tokens Tracked' },
      { key: 'tokens_blacklisted', label: 'Tokens Blacklisted' },
      { key: 'intents_generated', label: 'Intents Generated' },
      { key: 'exits_generated', label: 'Exits Generated' },
    ],
    'execution-engine': [
      { key: 'transactions_sent', label: 'Transactions Sent' },
      { key: 'transactions_confirmed', label: 'Transactions Confirmed' },
      { key: 'transactions_failed', label: 'Transactions Failed' },
    ],
  }

  const allKeys = [...common, ...(componentSpecific[component] || [])]

  return allKeys
    .map(({ key, label }) => {
      const value = metrics[key]
      if (value === undefined) return null
      return {
        label,
        value: typeof value === 'number' ? value.toLocaleString() : String(value),
      }
    })
    .filter((item): item is { label: string; value: string } => item !== null)
}
