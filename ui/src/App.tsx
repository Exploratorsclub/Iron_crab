import { useEffect, useMemo, useState } from 'react'

type LoadState<T> =
  | { status: 'idle' | 'loading' }
  | { status: 'error'; error: string }
  | { status: 'loaded'; data: T }

async function fetchJson<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: {
      Accept: 'application/json',
    },
  })

  if (!response.ok) {
    const text = await response.text().catch(() => '')
    throw new Error(`${response.status} ${response.statusText}${text ? `: ${text}` : ''}`)
  }

  return (await response.json()) as T
}

export default function App() {
  const [health, setHealth] = useState<LoadState<unknown>>({ status: 'idle' })
  const [status, setStatus] = useState<LoadState<unknown>>({ status: 'idle' })

  const nowIso = useMemo(() => new Date().toISOString(), [])

  useEffect(() => {
    let cancelled = false

    async function load() {
      setHealth({ status: 'loading' })
      setStatus({ status: 'loading' })

      try {
        const [healthJson, statusJson] = await Promise.all([
          fetchJson<unknown>('/api/health'),
          fetchJson<unknown>('/api/status'),
        ])

        if (cancelled) return
        setHealth({ status: 'loaded', data: healthJson })
        setStatus({ status: 'loaded', data: statusJson })
      } catch (e) {
        if (cancelled) return
        const message = e instanceof Error ? e.message : String(e)
        setHealth({ status: 'error', error: message })
        setStatus({ status: 'error', error: message })
      }
    }

    void load()
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <div>
      <h1>IronCrab UI</h1>
      <div className="small">
        This UI expects an SSH tunnel to the Control Plane at <code>127.0.0.1:8080</code>. Built at{' '}
        <code>{nowIso}</code>.
      </div>

      <section>
        <div>
          <strong>/health</strong>
        </div>
        {health.status === 'loading' && <div>Loading…</div>}
        {health.status === 'error' && <div className="error">Error: {health.error}</div>}
        {health.status === 'loaded' && <pre>{JSON.stringify(health.data, null, 2)}</pre>}
      </section>

      <section>
        <div>
          <strong>/status</strong>
        </div>
        {status.status === 'loading' && <div>Loading…</div>}
        {status.status === 'error' && <div className="error">Error: {status.error}</div>}
        {status.status === 'loaded' && <pre>{JSON.stringify(status.data, null, 2)}</pre>}
      </section>
    </div>
  )
}
