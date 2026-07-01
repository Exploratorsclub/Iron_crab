#!/usr/bin/env bash
# Arb price pipeline forensics — Geyser → market-data → NATS → arb-strategy
# Usage: METRICS_URL=http://127.0.0.1:9090/metrics ./tools/tmp_arb_price_pipeline_forensics.sh

set -euo pipefail

METRICS_URL="${METRICS_URL:-http://127.0.0.1:9090/metrics}"

fetch_metric() {
  local pattern="$1"
  curl -fsS "$METRICS_URL" 2>/dev/null | grep -E "^${pattern}" || true
}

section() {
  echo ""
  echo "=== $1 ==="
}

section "JetStream SLAVE sync (H1)"
fetch_metric 'arb_strategy_pool_cache_updates_seen_total'
fetch_metric 'arb_strategy_pool_cache_updates_seeded_total'
fetch_metric 'arb_pool_cache_sync_messages_total'
fetch_metric 'arb_pool_cache_sync_fetch_empty_total'
fetch_metric 'arb_strategy_bootstrap_known_pools_seeded'

section "MD publish (L1)"
fetch_metric 'market_data_pool_state_publish_total'
fetch_metric 'market_data_bin_array_publish_total'
fetch_metric 'market_data_geyser_to_publish_ms_other'

section "NATS ingress (L2 arb)"
fetch_metric 'market_events_consumed_total'
fetch_metric 'arb_subscriber_high_processed_total'
fetch_metric 'arb_subscriber_low_processed_total'

section "Arb tracker / freshness (L3)"
fetch_metric 'arb_tracker_write_processed_total\{job_type="pool_state_update"\}'
fetch_metric 'arb_two_hop_rejected_total\{reason="stale_price"\}'
fetch_metric 'arb_two_hop_rejected_total\{reason="insufficient_pools"\}'
fetch_metric 'arb_price_freshness_age_ms_count'
fetch_metric 'arb_price_freshness_age_ms_bucket'

section "Pool cache / known_pools"
fetch_metric 'known_pools'
fetch_metric 'pools_tracked'
fetch_metric 'market_data_arb_pinned_pools'

echo ""
echo "Done. Compare stale_price vs pool_cache_updates_seen / MD publish rates."
