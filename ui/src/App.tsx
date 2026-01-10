import { useEffect, useState } from 'react'
import { BrowserRouter, Routes, Route, Link } from 'react-router-dom'
import { Overview } from './pages/Overview'
import { ComponentDetail } from './pages/ComponentDetail'
import { KillSwitch } from './pages/KillSwitch'
import type { HealthStatus, SystemStatus, RbacInfo } from './types'

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
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [systemStatus, setSystemStatus] = useState<SystemStatus | null>(null)
  const [rbacInfo, setRbacInfo] = useState<RbacInfo | null>(null)
  const [isRefreshing, setIsRefreshing] = useState<boolean>(false)
  const [apiKeyDraft, setApiKeyDraft] = useState<string>(() => localStorage.getItem('ironcrab_api_key') || '')
  const [actionState, setActionState] = useState<
    | { status: 'idle' }
    | { status: 'busy'; message: string }
    | { status: 'error'; message: string }
    | { status: 'ok'; message: string }
  >({ status: 'idle' })

  async function loadAll() {
    setIsRefreshing(true)

    try {
      const [h, s, r] = await Promise.all([
        fetchJson<HealthStatus>('http://127.0.0.1:8080/health'),
        fetchJson<SystemStatus>('http://127.0.0.1:8080/status'),
        fetchJson<RbacInfo>('http://127.0.0.1:8080/rbac'),
      ])

      setHealth(h)
      setSystemStatus(s)
      setRbacInfo(r)
    } catch (err) {
      console.error('Failed to load data:', err)
    } finally {
      setIsRefreshing(false)
    }
  }

  useEffect(() => {
    void loadAll()

    const interval = setInterval(() => {
      void loadAll()
    }, 10_000) // Poll every 10s

    return () => clearInterval(interval)
  }, [])

  function saveApiKey() {
    localStorage.setItem('ironcrab_api_key', apiKeyDraft)
    setActionState({ status: 'ok', message: 'API key saved to localStorage' })
    setTimeout(() => setActionState({ status: 'idle' }), 3000)
  }

  async function runSystemdAction(component: string, action: string) {
    setActionState({ status: 'busy', message: `${action} ${component}…` })

    try {
      await postJson('http://127.0.0.1:8080/systemd', { component, action })
      setActionState({ status: 'ok', message: `${action} ${component} success!` })
      setTimeout(() => setActionState({ status: 'idle' }), 3000)
    } catch (err) {
      setActionState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    } finally {
      void loadAll()
    }
  }

  return (
    <BrowserRouter>
      <div>
        <header>
          <h1>
            <Link to="/">IronCrab UI</Link>
          </h1>
          <nav>
            <Link to="/">Overview</Link>
            <Link to="/market-data">Market Data</Link>
            <Link to="/momentum-bot">Momentum Bot</Link>
            <Link to="/arb-strategy">Arb Strategy</Link>
            <Link to="/execution-engine">Execution Engine</Link>
            <Link to="/kill-switch">Kill Switch</Link>
          </nav>
        </header>

        <section>
          <div>
            <strong>Operator</strong>
          </div>
          <div className="card">
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
            {actionState.status === 'busy' && <div>⏳ {actionState.message}</div>}
            {actionState.status === 'ok' && <div>✅ {actionState.message}</div>}
            {actionState.status === 'error' && <div className="error">❌ {actionState.message}</div>}
            <div className="small">
              {rbacInfo?.auth_required
                ? 'Auth enabled on control-plane: admin API key required for start/stop/kill.'
                : 'Auth disabled on control-plane (dev mode): API key not required.'}
            </div>
          </div>
        </section>

        <Routes>
          <Route
            path="/"
            element={
              <Overview
                health={health}
                systemStatus={systemStatus}
                isRefreshing={isRefreshing}
                onRefresh={() => void loadAll()}
                onSystemdAction={(c, a) => void runSystemdAction(c, a)}
              />
            }
          />
          <Route path="/:component" element={<ComponentDetail />} />
          <Route
            path="/kill-switch"
            element={
              <KillSwitch
                killSwitchActive={systemStatus?.kill_switch_active ?? false}
                onAction={() => void loadAll()}
              />
            }
          />
        </Routes>
      </div>
    </BrowserRouter>
  )
}
