import { useState } from 'react'
import { Link } from 'react-router-dom'

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

interface KillSwitchProps {
  killSwitchActive: boolean
  onAction: () => void
}

export function KillSwitch({ killSwitchActive, onAction }: KillSwitchProps) {
  const [killReason, setKillReason] = useState<string>('')
  const [liquidatePositions, setLiquidatePositions] = useState<boolean>(true)
  const [actionState, setActionState] = useState<
    | { status: 'idle' }
    | { status: 'busy'; message: string }
    | { status: 'error'; message: string }
    | { status: 'ok'; message: string }
  >({ status: 'idle' })

  async function activateKillSwitch() {
    if (!killReason.trim()) {
      setActionState({ status: 'error', message: 'Reason required' })
      return
    }

    setActionState({ status: 'busy', message: 'Activating kill switch…' })

    try {
      await postJson('http://127.0.0.1:8080/kill', {
        reason: killReason,
        liquidate_positions: liquidatePositions,
      })
      setActionState({ status: 'ok', message: 'Kill switch activated!' })
      onAction()
    } catch (err) {
      setActionState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }

  async function resetKillSwitch() {
    setActionState({ status: 'busy', message: 'Resetting kill switch…' })

    try {
      await postJson('http://127.0.0.1:8080/kill/reset', {})
      setActionState({ status: 'ok', message: 'Kill switch reset!' })
      setKillReason('')
      onAction()
    } catch (err) {
      setActionState({
        status: 'error',
        message: err instanceof Error ? err.message : String(err),
      })
    }
  }

  return (
    <div>
      <div className="breadcrumb">
        <Link to="/">← Overview</Link>
      </div>

      <h2>Kill Switch</h2>

      <section>
        <div>
          <strong>Current Status</strong>
        </div>
        <div className="card">
          <div className="kvRow">
            <span>Kill Switch</span>
            <span className={killSwitchActive ? 'error' : ''}>{killSwitchActive ? 'ACTIVE ⚠️' : 'Inactive'}</span>
          </div>
        </div>
      </section>

      <section>
        <div>
          <strong>Emergency Stop</strong>
        </div>
        <div className="card">
          <div className="small" style={{ marginBottom: '1rem' }}>
            The kill switch immediately stops all trading activity across all components. Use this in case of unexpected
            behavior or manual intervention.
          </div>

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
            <span>Liquidate all open positions</span>
          </label>

          <div className="actions">
            <button className="danger" onClick={() => void activateKillSwitch()} disabled={killSwitchActive}>
              {killSwitchActive ? 'Already Active' : 'Activate Kill Switch'}
            </button>
            <button onClick={() => void resetKillSwitch()} disabled={!killSwitchActive}>
              Reset
            </button>
          </div>

          {actionState.status === 'busy' && <div>⏳ {actionState.message}</div>}
          {actionState.status === 'ok' && <div>✅ {actionState.message}</div>}
          {actionState.status === 'error' && <div className="error">❌ {actionState.message}</div>}
        </div>
      </section>
    </div>
  )
}
