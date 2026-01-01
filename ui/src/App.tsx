import { useEffect, useMemo, useState } from 'react'

type LoadState<T> =
  | { status: 'idle' | 'loading' }
  | { status: 'error'; error: string }
  | { status: 'loaded'; data: T }

type HealthStatus = {
  status: 'ok' | 'degraded' | 'unhealthy' | string
  timestamp: string
  version?: string
  uptime_seconds: number
  checks: Record<string, boolean>
  details?: Record<string, unknown> | null
}

type ComponentStatus = {
  name: string
  healthy: boolean
  metrics_url: string
  last_check: string
  details?: Record<string, unknown> | null
}

type SystemStatus = {
  timestamp: string
  overall_healthy: boolean
  components: ComponentStatus[]
  kill_switch_active: boolean
}

type RbacInfo = {
  auth_required: boolean
}

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

export default function App() {
  const [health, setHealth] = useState<LoadState<HealthStatus>>({ status: 'idle' })
  const [systemStatus, setSystemStatus] = useState<LoadState<SystemStatus>>({ status: 'idle' })
  const [rbacInfo, setRbacInfo] = useState<RbacInfo | null>(null)
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false)
  const [apiKeyDraft, setApiKeyDraft] = useState<string>(() => localStorage.getItem('ironcrab_api_key') || '')
  const [killReason, setKillReason] = useState<string>('')
  const [liquidatePositions, setLiquidatePositions] = useState<boolean>(true)
  const [actionState, setActionState] = useState<
    | { status: 'idle' }
    | { status: 'busy'; message: string }
    | { status: 'error'; message: string }
    | { status: 'ok'; message: string }
  >({ status: 'idle' })

  const nowIso = useMemo(() => new Date().toISOString(), [])

  async function loadAll(opts?: { background?: boolean }) {
    const background = opts?.background === true

    if (!background) {
      setHealth({ status: 'loading' })
      setSystemStatus({ status: 'loading' })
    } else {
      setIsRefreshing(true)
    }

    try {
      const [healthJson, statusJson, rbacJson] = await Promise.all([
        fetchJson<HealthStatus>('/api/health'),
        fetchJson<SystemStatus>('/api/status'),
        fetchJson<RbacInfo>('/api/rbac/info').catch(() => null),
      ])

      setHealth({ status: 'loaded', data: healthJson })
      setSystemStatus({ status: 'loaded', data: statusJson })
      setRbacInfo(rbacJson)

      if (background) {
        setActionState((prev) => {
          if (prev.status === 'error' && prev.message.startsWith('Auto-refresh failed:')) {
            return { status: 'idle' }
          }
          return prev
        })
      }
    } catch (e) {
      const message = e instanceof Error ? e.message : String(e)
      if (!background) {
        setHealth({ status: 'error', error: message })
        setSystemStatus({ status: 'error', error: message })
      } else {
        setActionState({ status: 'error', message: `Auto-refresh failed: ${message}` })
      }
    } finally {
      if (background) setIsRefreshing(false)
    }
  }

  useEffect(() => {
    let cancelled = false
    const run = async () => {
      if (cancelled) return
      await loadAll()
    }
    void run()
    const id = window.setInterval(() => {
      if (cancelled) return
      void loadAll({ background: true })
    }, 5000)
    return () => {
      cancelled = true
      window.clearInterval(id)
    }
  }, [])

  function saveApiKey() {
    const value = apiKeyDraft.trim()
    if (value.length === 0) localStorage.removeItem('ironcrab_api_key')
    else localStorage.setItem('ironcrab_api_key', value)
    setActionState({ status: 'ok', message: 'API key saved (local).' })
    void loadAll()
  }

  async function runSystemdAction(component: string, action: 'start' | 'stop' | 'restart') {
    setActionState({ status: 'busy', message: `${action} ${component}…` })
    try {
      const res = await postJson<{ ok: boolean; stderr?: string; stdout?: string }>(
        `/api/systemd/${encodeURIComponent(component)}/${encodeURIComponent(action)}`,
        {},
      )
      if (!res.ok) {
        setActionState({ status: 'error', message: res.stderr || res.stdout || 'systemd action failed' })
      } else {
        setActionState({ status: 'ok', message: `${action} ${component}: ok` })
      }
    } catch (e) {
      setActionState({ status: 'error', message: e instanceof Error ? e.message : String(e) })
    } finally {
      void loadAll()
    }
  }

  async function activateKillSwitch() {
    const reason = killReason.trim()
    if (!reason) {
      setActionState({ status: 'error', message: 'Kill reason is required.' })
      return
    }
    setActionState({ status: 'busy', message: 'Activating kill switch…' })
    try {
      await postJson('/api/kill', { reason, liquidate_positions: liquidatePositions })
      setActionState({ status: 'ok', message: 'Kill switch activated.' })
    } catch (e) {
      setActionState({ status: 'error', message: e instanceof Error ? e.message : String(e) })
    } finally {
      void loadAll()
    }
  }

  async function resetKillSwitch() {
    setActionState({ status: 'busy', message: 'Resetting kill switch…' })
    try {
      await postJson('/api/kill/reset', {})
      setActionState({ status: 'ok', message: 'Kill switch reset.' })
    } catch (e) {
      setActionState({ status: 'error', message: e instanceof Error ? e.message : String(e) })
    } finally {
      void loadAll()
    }
  }

  const controlPlaneBaseUrl = 'http://127.0.0.1:8080'
  const grafanaUrl = 'http://127.0.0.1:3000'
  const prometheusUrl = 'http://127.0.0.1:9090'

  const componentRows = useMemo((): ComponentStatus[] => {
    if (systemStatus.status !== 'loaded') return []

    const actualByName = new Map((systemStatus.data.components ?? []).map((c) => [c.name, c]))
    const now = new Date().toISOString()

    const expected: ComponentStatus[] = [
      {
        name: 'control-plane',
        healthy: true,
        metrics_url: `${controlPlaneBaseUrl}/metrics`,
        last_check: now,
        details: { note: 'UI connected' },
      },
      {
        name: 'market-data',
        healthy: false,
        metrics_url: 'http://127.0.0.1:9801/metrics',
        last_check: now,
        details: { error: 'missing from /status' },
      },
      {
        name: 'momentum-bot',
        healthy: false,
        metrics_url: 'http://127.0.0.1:9802/metrics',
        last_check: now,
        details: { error: 'missing from /status' },
      },
      {
        name: 'arb-strategy',
        healthy: false,
        metrics_url: 'http://127.0.0.1:9803/metrics',
        last_check: now,
        details: { error: 'missing from /status' },
      },
      {
        name: 'execution-engine',
        healthy: false,
        metrics_url: 'http://127.0.0.1:9804/metrics',
        last_check: now,
        details: { error: 'missing from /status' },
      },
    ]

    return expected.map((fallback) => actualByName.get(fallback.name) ?? fallback)
  }, [systemStatus])

  function formatUptime(seconds: number): string {
    if (!Number.isFinite(seconds) || seconds < 0) return '-'
    const minutes = Math.floor(seconds / 60)
    const hours = Math.floor(minutes / 60)
    const mins = minutes % 60
    if (hours <= 0) return `${mins}m`
    return `${hours}h ${mins}m`
  }

  function formatIso(iso: string): string {
    const d = new Date(iso)
    if (Number.isNaN(d.getTime())) return iso
    return d.toLocaleString()
  }

  function componentDetailText(details: ComponentStatus['details']): string {
    if (!details) return ''
    const error = details['error']
    if (typeof error === 'string' && error.length > 0) return error
    const response = details['response']
    if (typeof response === 'string' && response.length > 0) return response
    return ''
  }

  return (
    <div>
      <h1>IronCrab UI</h1>
      <div className="small">
        Local UI (Vite) + SSH tunnel to the server-side control-plane. Built at <code>{nowIso}</code>.
      </div>

      <section>
        <div className="kv">
          <div>
            <strong>Links</strong>
          </div>
          <div className="kvRow">
            <span>Control-plane</span>
            <a href={controlPlaneBaseUrl} target="_blank" rel="noreferrer">
              {controlPlaneBaseUrl}
            </a>
          </div>
          <div className="kvRow">
            <span>Grafana</span>
            <a href={grafanaUrl} target="_blank" rel="noreferrer">
              {grafanaUrl}
            </a>
          </div>
          <div className="kvRow">
            <span>Prometheus</span>
            <a href={prometheusUrl} target="_blank" rel="noreferrer">
              {prometheusUrl}
            </a>
          </div>
        </div>
      </section>

      <section>
        <div>
          <strong>Operator</strong>
        </div>
        <div className="card">
          {isRefreshing && <div className="small">Refreshing…</div>}
          <div className="kvRow">
            <span>API key (optional)</span>
            <input
              className="textInput"
              value={apiKeyDraft}
              onChange={(e) => setApiKeyDraft(e.target.value)}
              placeholder="X-API-Key (admin for start/stop/kill)"
              type="password"
            />
          </div>
          <div className="actions">
            <button onClick={saveApiKey}>Save</button>
            <button onClick={() => void loadAll()}>Refresh now</button>
          </div>
          {actionState.status === 'busy' && <div>Working: {actionState.message}</div>}
          {actionState.status === 'ok' && <div>{actionState.message}</div>}
          {actionState.status === 'error' && <div className="error">{actionState.message}</div>}
          <div className="small">
            {rbacInfo?.auth_required
              ? 'Auth enabled on control-plane: admin API key required for start/stop/kill.'
              : 'Auth disabled on control-plane (dev mode): API key not required.'}
          </div>
        </div>
      </section>

      <section>
        <div>
          <strong>Control-plane health</strong>
        </div>
        {health.status === 'loading' && <div>Loading…</div>}
        {health.status === 'error' && <div className="error">Error: {health.error}</div>}
        {health.status === 'loaded' && (
          <div className="card">
            <div className="kvRow">
              <span>Status</span>
              <span>{health.data.status}</span>
            </div>
            <div className="kvRow">
              <span>Uptime</span>
              <span>{formatUptime(health.data.uptime_seconds)}</span>
            </div>
            <div className="kvRow">
              <span>Timestamp</span>
              <span>{formatIso(health.data.timestamp)}</span>
            </div>
            <div className="kvRow">
              <span>Checks</span>
              <span>
                {Object.entries(health.data.checks)
                  .map(([key, ok]) => `${key}=${ok ? 'ok' : 'fail'}`)
                  .join(' · ')}
              </span>
            </div>
          </div>
        )}
      </section>

      <section>
        <div>
          <strong>System status</strong>
        </div>
        {systemStatus.status === 'loading' && <div>Loading…</div>}
        {systemStatus.status === 'error' && <div className="error">Error: {systemStatus.error}</div>}
        {systemStatus.status === 'loaded' && (
          <>
            <div className="card">
              <div className="kvRow">
                <span>Overall</span>
                <span>{systemStatus.data.overall_healthy ? 'healthy' : 'degraded/unhealthy'}</span>
              </div>
              <div className="kvRow">
                <span>Kill switch</span>
                <span>{systemStatus.data.kill_switch_active ? 'ACTIVE' : 'inactive'}</span>
              </div>
              <div className="kvRow">
                <span>Timestamp</span>
                <span>{formatIso(systemStatus.data.timestamp)}</span>
              </div>
            </div>

            <table className="statusTable">
              <thead>
                <tr>
                  <th>Component</th>
                  <th>Healthy</th>
                  <th>Last check</th>
                  <th>Metrics</th>
                  <th>Actions</th>
                  <th>Details</th>
                </tr>
              </thead>
              <tbody>
                {componentRows.map((c) => {
                  const details = componentDetailText(c.details)
                  return (
                    <tr key={c.name}>
                      <td>{c.name}</td>
                      <td>{c.healthy ? 'ok' : 'fail'}</td>
                      <td>{formatIso(c.last_check)}</td>
                      <td>
                        <a href={c.metrics_url} target="_blank" rel="noreferrer">
                          /metrics
                        </a>
                      </td>
                      <td>
                        {c.name === 'control-plane' ? (
                          <span>-</span>
                        ) : (
                          <div className="actions">
                            <button onClick={() => void runSystemdAction(c.name, 'start')}>Start</button>
                            <button onClick={() => void runSystemdAction(c.name, 'stop')}>Stop</button>
                            <button onClick={() => void runSystemdAction(c.name, 'restart')}>Restart</button>
                          </div>
                        )}
                      </td>
                      <td className={c.healthy ? '' : 'error'}>{details}</td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </>
        )}
      </section>

      <section>
        <div>
          <strong>Kill switch</strong>
        </div>
        <div className="card">
          <div className="kvRow">
            <span>Reason</span>
            <input
              className="textInput"
              value={killReason}
              onChange={(e) => setKillReason(e.target.value)}
              placeholder="e.g. unexpected behavior / operator stop"
            />
          </div>
          <label className="checkboxRow">
            <input
              type="checkbox"
              checked={liquidatePositions}
              onChange={(e) => setLiquidatePositions(e.target.checked)}
            />
            <span>Liquidate positions</span>
          </label>
          <div className="actions">
            <button className="danger" onClick={() => void activateKillSwitch()}>
              Activate
            </button>
            <button onClick={() => void resetKillSwitch()}>Reset</button>
          </div>
          <div className="small">
            Current state:{' '}
            {systemStatus.status === 'loaded' ? (systemStatus.data.kill_switch_active ? 'ACTIVE' : 'inactive') : '-'}
          </div>
        </div>
      </section>
    </div>
  )
}
