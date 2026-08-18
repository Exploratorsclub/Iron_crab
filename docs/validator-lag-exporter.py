#!/usr/bin/env python3
"""Prometheus exporter: real validator slot lag (local RPC vs public mainnet RPC)."""

from __future__ import annotations

import json
import os
import threading
import time
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LOCAL_RPC = os.environ.get("VALIDATOR_LAG_LOCAL_RPC", "http://127.0.0.1:8899")
REF_RPC = os.environ.get(
    "VALIDATOR_LAG_REFERENCE_RPC", "https://api.mainnet-beta.solana.com"
)
LISTEN = os.environ.get("VALIDATOR_LAG_LISTEN", "127.0.0.1:9180")
INTERVAL = float(os.environ.get("VALIDATOR_LAG_INTERVAL_SEC", "10"))

_lock = threading.Lock()
_metrics = {
    "ironcrab_validator_local_slot": 0,
    "ironcrab_validator_reference_slot": 0,
    "ironcrab_validator_slots_behind": 0,
    "ironcrab_validator_lag_scrape_success": 0,
    "ironcrab_validator_lag_scrape_errors_total": 0,
    "ironcrab_validator_lag_last_scrape_timestamp": 0,
}


def get_slot(rpc_url: str) -> int:
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []}
    ).encode()
    req = urllib.request.Request(
        rpc_url,
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode())
    if "error" in data:
        raise RuntimeError(data["error"])
    return int(data["result"])


def poll_loop() -> None:
    while True:
        try:
            local = get_slot(LOCAL_RPC)
            reference = get_slot(REF_RPC)
            with _lock:
                _metrics["ironcrab_validator_local_slot"] = local
                _metrics["ironcrab_validator_reference_slot"] = reference
                _metrics["ironcrab_validator_slots_behind"] = max(0, reference - local)
                _metrics["ironcrab_validator_lag_scrape_success"] = 1
                _metrics["ironcrab_validator_lag_last_scrape_timestamp"] = int(
                    time.time()
                )
        except Exception:
            with _lock:
                _metrics["ironcrab_validator_slots_behind"] = -1
                _metrics["ironcrab_validator_lag_scrape_success"] = 0
                _metrics["ironcrab_validator_lag_scrape_errors_total"] += 1
        time.sleep(INTERVAL)


class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path != "/metrics":
            self.send_response(404)
            self.end_headers()
            return

        with _lock:
            lines = [
                "# HELP ironcrab_validator_local_slot Local validator slot from getSlot",
                "# TYPE ironcrab_validator_local_slot gauge",
                f"ironcrab_validator_local_slot {_metrics['ironcrab_validator_local_slot']}",
                "# HELP ironcrab_validator_reference_slot Reference mainnet slot from getSlot",
                "# TYPE ironcrab_validator_reference_slot gauge",
                f"ironcrab_validator_reference_slot {_metrics['ironcrab_validator_reference_slot']}",
                "# HELP ironcrab_validator_slots_behind Real slot lag vs reference RPC",
                "# TYPE ironcrab_validator_slots_behind gauge",
                f"ironcrab_validator_slots_behind {_metrics['ironcrab_validator_slots_behind']}",
                "# HELP ironcrab_validator_lag_scrape_success 1 if last scrape succeeded",
                "# TYPE ironcrab_validator_lag_scrape_success gauge",
                f"ironcrab_validator_lag_scrape_success {_metrics['ironcrab_validator_lag_scrape_success']}",
                "# HELP ironcrab_validator_lag_scrape_errors_total Failed scrapes",
                "# TYPE ironcrab_validator_lag_scrape_errors_total counter",
                f"ironcrab_validator_lag_scrape_errors_total {_metrics['ironcrab_validator_lag_scrape_errors_total']}",
                "# HELP ironcrab_validator_lag_last_scrape_timestamp Unix time of last successful scrape",
                "# TYPE ironcrab_validator_lag_last_scrape_timestamp gauge",
                f"ironcrab_validator_lag_last_scrape_timestamp {_metrics['ironcrab_validator_lag_last_scrape_timestamp']}",
            ]

        body = "\n".join(lines) + "\n"
        encoded = body.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def log_message(self, fmt: str, *args) -> None:
        return


def main() -> None:
    host, _, port_str = LISTEN.rpartition(":")
    if not host:
        host, port_str = "127.0.0.1", LISTEN
    port = int(port_str)

    threading.Thread(target=poll_loop, daemon=True, name="validator-lag-poller").start()
    server = ThreadingHTTPServer((host, port), MetricsHandler)
    server.serve_forever()


if __name__ == "__main__":
    main()
