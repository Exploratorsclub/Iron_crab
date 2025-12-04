# Orca Reserve Cache Configuration Example
# Add this section to your my_config.server.toml to enable persistent caching

[orca]
# Enable SQLite-based persistent cache for vault reserves
# This reduces RPC load and improves latency by caching balances across restarts
enable_reserve_cache = true

# Path to SQLite database (relative to config directory)
# Default: "orca_reserves.db"
cache_path = "orca_reserves.db"

# Number of top pools to prefetch reserves for on startup
# Higher values = better cache warmth but slower startup
# Recommended: 100-500 depending on your network and timeout settings
prefetch_top_pools = 100
