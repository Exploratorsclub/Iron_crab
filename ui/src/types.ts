export type HealthStatus = {
  status: 'ok' | 'degraded' | 'unhealthy' | string
  timestamp: string
  version?: string
  uptime_seconds: number
  checks: Record<string, boolean>
  details?: Record<string, unknown> | null
}

export type ComponentStatus = {
  name: string
  healthy: boolean
  metrics_url: string
  last_check: string
  details?: Record<string, unknown> | null
}

export type SystemStatus = {
  timestamp: string
  overall_healthy: boolean
  components: ComponentStatus[]
  kill_switch_active: boolean
}

export type RbacInfo = {
  auth_required: boolean
}

export type MetricsData = Record<string, string | number>

export type ConfigValue = string | number | boolean | null

export type ComponentConfig = Record<string, ConfigValue>
