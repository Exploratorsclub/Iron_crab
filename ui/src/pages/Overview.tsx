import { useMemo } from 'react'
import type { ComponentStatus, SystemStatus, HealthStatus } from '../types'

interface OverviewProps {
  health: HealthStatus | null
  systemStatus: SystemStatus | null
  isRefreshing: boolean
  onRefresh: () => void
  onSystemdAction: (component: string, action: string) => void
}

export function Overview({ health, systemStatus, isRefreshing, onRefresh, onSystemdAction }: OverviewProps) {
  const controlPlaneBaseUrl = 'http://127.0.0.1:8080'
  const grafanaUrl = 'http://127.0.0.1:3000'
  const prometheusUrl = 'http://127.0.0.1:9090'

  const componentRows = useMemo((): ComponentStatus[] => {
    if (!systemStatus) return []

    const actualByName = new Map((systemStatus.components ?? []).map((c) => [c.name, c]))
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
        <div className="sectionHeader">
          <strong>Control-plane health</strong>
          <button onClick={onRefresh} disabled={isRefreshing}>
            {isRefreshing ? 'Refreshing…' : 'Refresh'}
          </button>
        </div>
        {!health && <div>Loading…</div>}
        {health && (
          <div className="card">
            <div className="kvRow">
              <span>Status</span>
              <span>{health.status}</span>
            </div>
            <div className="kvRow">
              <span>Uptime</span>
              <span>{formatUptime(health.uptime_seconds)}</span>
            </div>
            <div className="kvRow">
              <span>Timestamp</span>
              <span>{formatIso(health.timestamp)}</span>
            </div>
            <div className="kvRow">
              <span>Checks</span>
              <span>
                {Object.entries(health.checks)
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
        {!systemStatus && <div>Loading…</div>}
        {systemStatus && (
          <>
            <div className="card">
              <div className="kvRow">
                <span>Overall</span>
                <span>{systemStatus.overall_healthy ? 'healthy' : 'degraded/unhealthy'}</span>
              </div>
              <div className="kvRow">
                <span>Kill switch</span>
                <span>{systemStatus.kill_switch_active ? 'ACTIVE' : 'inactive'}</span>
              </div>
              <div className="kvRow">
                <span>Timestamp</span>
                <span>{formatIso(systemStatus.timestamp)}</span>
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
                      <td>
                        {c.name === 'control-plane' ? (
                          c.name
                        ) : (
                          <a href={`/${c.name}`}>{c.name}</a>
                        )}
                      </td>
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
                            <button onClick={() => onSystemdAction(c.name, 'start')}>Start</button>
                            <button onClick={() => onSystemdAction(c.name, 'stop')}>Stop</button>
                            <button onClick={() => onSystemdAction(c.name, 'restart')}>Restart</button>
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
    </div>
  )
}
