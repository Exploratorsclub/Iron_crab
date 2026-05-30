#!/usr/bin/env python3
"""
Simple HTTP server that serves trade data from JSONL execution_results files.
Run this separately from the bot to keep trade history available even when bot is stopped.

Reads from: trade_logs/executions/execution_results-YYYYMMDD.jsonl
Grafana Infinity data source connects to this for "Recent Trades" table panel.

Usage: python3 trades_server.py [--port 9899]

Query params (GET /trades):
- mode: limit|run — same as before
- time_mode: relative|utc — controls `time_display` (default relative); `timestamp_ms` always for sorting
  Response adds: time_utc, time_age, time_display
"""

import http.server
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Optional
from urllib.parse import urlparse, parse_qs
import argparse

EXEC_LOG_DIR = os.environ.get("IRONCRAB_LOG_DIR", "trade_logs")
EXECUTIONS_DIR = Path(EXEC_LOG_DIR) / "executions"
DECISIONS_DIR = Path(EXEC_LOG_DIR) / "decisions"
# metrics::record_recent_trade uses IRONCRAB_TRADE_LOG_DIR (see append_trade_to_jsonl)
RECENT_TRADES_DIR = Path(
    os.environ.get("IRONCRAB_TRADE_LOG_DIR", "trade_logs")
)

class TradesHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path.rstrip('/')
        params = parse_qs(parsed.query)

        if path == "/trades":
            mode = params.get('mode', ['limit'])[0]
            time_mode = params.get('time_mode', ['relative'])[0]
            self.serve_trades(mode=mode, time_mode=time_mode)
        elif path == "/decisions":
            self.serve_decisions()
        elif path == "/pnl_24h":
            self.serve_pnl_24h()
        elif path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()
    
    def serve_trades(self, mode: str = "limit", time_mode: str = "relative"):
        if mode == "run":
            trades = self.read_trades_by_run()
        else:
            trades = self.read_recent_trades(20)

        self._apply_time_mode_fields(trades, time_mode)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(trades).encode())

    def _format_time_age_german(self, ts_ms: int) -> str:
        """Relative age in German, e.g. 'vor 25 Minuten' (Grafana table display)."""
        if not ts_ms:
            return "—"
        now = datetime.now(tz=timezone.utc)
        ts = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
        delta = now - ts
        total_secs = int(max(0, delta.total_seconds()))
        if total_secs < 60:
            s = max(1, total_secs)
            return f"vor {s} Sekunde{'n' if s != 1 else ''}"
        if total_secs < 3600:
            m = max(1, total_secs // 60)
            if m == 1:
                return "vor 1 Minute"
            return f"vor {m} Minuten"
        if total_secs < 86400:
            h = max(1, total_secs // 3600)
            if h == 1:
                return "vor 1 Stunde"
            return f"vor {h} Stunden"
        d = max(1, total_secs // 86400)
        if d == 1:
            return "vor 1 Tag"
        return f"vor {d} Tagen"

    def _apply_time_mode_fields(self, trades: list, time_mode: str) -> None:
        """Add time_utc, time_age, and time_display for dashboard toggles (keeps timestamp_ms for sort)."""
        if time_mode not in ("relative", "utc"):
            time_mode = "relative"
        for t in trades:
            ts_ms = t.get("timestamp_ms") or 0
            if ts_ms:
                utc_str = (
                    datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    )
                    + " UTC"
                )
            else:
                utc_str = "—"
            age_str = self._format_time_age_german(ts_ms)
            t["time_utc"] = utc_str
            t["time_age"] = age_str
            t["time_display"] = age_str if time_mode == "relative" else utc_str

    def serve_decisions(self):
        decisions = self.read_recent_decisions(200)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(decisions).encode())
    
    def read_recent_trades(self, limit: int) -> list:
        """Read last N trades from execution_results JSONL (fallback: recent_trades JSONL)."""
        trades = self.load_all_trades(days_ago_list=[0, 1, 2])

        # Compute running PnL.
        #
        # BUY cost: PREFER value_sol (fill_in = actual SOL spent on swap). Fallback: abs(wallet_delta).
        #   For BUY with WSOL: wallet_delta = native SOL only (rent/fees), NOT swap amount.
        #   Using value_sol ensures consistency with SELL (both use fills).
        #
        # SELL proceeds: PREFER value_sol (fill_out) over wallet_delta!
        #   For PumpSwap/PumpFun SELL: output is WSOL (token), NOT native SOL. wallet_delta
        #   only shows rent refund - fees (~0.002), NOT the actual swap proceeds.
        # value_sol = fill_in (BUY) or fill_out (SELL) = actual swap amount.
        # PnL = proceeds - proportional_cost
        trades.sort(key=lambda t: t.get('timestamp_ms', 0))
        positions = {}
        for trade in trades:
            action = trade.get('action')
            mint_full = trade.get('mint_full')
            amount_tokens = trade.get('amount_tokens')
            value_sol = trade.get('value_sol')
            wallet_delta = trade.get('wallet_sol_delta')

            if not mint_full or mint_full == "-" or not amount_tokens:
                continue

            if action == "BUY":
                # Cost: PREFER value_sol (fill_in) — actual swap amount. For WSOL BUY,
                # wallet_delta ≠ swap amount (only rent/fees). Fallback: abs(wallet_delta).
                cost_sol = value_sol if (value_sol is not None and value_sol > 0) else None
                if cost_sol is None and wallet_delta is not None:
                    cost_sol = abs(wallet_delta)
                if cost_sol is None or cost_sol <= 0:
                    continue
                pos = positions.get(mint_full, {"tokens": 0.0, "cost_sol": 0.0})
                pos["tokens"] += amount_tokens
                pos["cost_sol"] += cost_sol
                positions[mint_full] = pos
                trade["pnl_sol"] = 0.0
                trade["pnl_pct"] = None
            elif action == "SELL":
                # Proceeds: PREFER value_sol (fill_out = swap output) — for PumpSwap SELL,
                # output is WSOL, wallet_delta only has rent/fees, NOT the swap proceeds.
                proceeds_sol = value_sol if (value_sol is not None and value_sol > 0) else 0
                if proceeds_sol == 0:
                    proceeds_sol = wallet_delta if (wallet_delta is not None and wallet_delta > 0) else 0
                pos = positions.get(mint_full)
                if pos and pos["tokens"] > 0:
                    avg_cost = pos["cost_sol"] / pos["tokens"]
                    sold_cost = avg_cost * amount_tokens
                    pnl_sol = proceeds_sol - sold_cost
                    pos["tokens"] = max(0.0, pos["tokens"] - amount_tokens)
                    pos["cost_sol"] = max(0.0, pos["cost_sol"] - sold_cost)
                    positions[mint_full] = pos
                    trade["pnl_sol"] = round(pnl_sol, 9)
                    trade["pnl_pct"] = round((pnl_sol / sold_cost) * 100, 2) if sold_cost > 0 else None
                else:
                    trade["pnl_sol"] = None
                    trade["pnl_pct"] = None

        # Sort by timestamp descending, take last N
        trades.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
        return trades[:limit]

    def read_trades_by_run(self) -> list:
        """Read all trades from the current run + last 20 from the previous run.

        Identifies the latest run_id as the 'current run'. Returns all trades
        belonging to that run, plus up to 20 trades from the most recent
        previous run (for context). PnL calculation covers all returned trades.
        """
        all_trades = self.load_all_trades(days_ago_list=[0, 1, 2])

        if not all_trades:
            return []

        # Step 2: Identify run_ids by most recent trade timestamp
        run_last_ts = {}
        for t in all_trades:
            rid = t.get('run_id', '')
            ts = t.get('timestamp_ms', 0)
            if rid and ts > run_last_ts.get(rid, 0):
                run_last_ts[rid] = ts

        # Sort runs by last trade timestamp descending
        sorted_runs = sorted(run_last_ts.keys(), key=lambda r: run_last_ts[r], reverse=True)

        # Step 3: Collect trades for current run + last 20 from previous run
        if not sorted_runs:
            # recent_trades JSONL has no run_id — scope by calendar day of newest trade
            all_trades.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
            newest_ts = all_trades[0].get('timestamp_ms', 0)
            newest_day = datetime.fromtimestamp(
                newest_ts / 1000, tz=timezone.utc
            ).date()
            current_trades = [
                t
                for t in all_trades
                if datetime.fromtimestamp(
                    (t.get('timestamp_ms') or 0) / 1000, tz=timezone.utc
                ).date()
                == newest_day
            ]
            prev_candidates = [
                t
                for t in all_trades
                if datetime.fromtimestamp(
                    (t.get('timestamp_ms') or 0) / 1000, tz=timezone.utc
                ).date()
                != newest_day
            ]
            prev_candidates.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
            prev_trades = prev_candidates[:20]
            trades = current_trades + prev_trades
        else:
            current_run = sorted_runs[0]
            prev_run = sorted_runs[1] if len(sorted_runs) > 1 else None
            current_trades = [t for t in all_trades if t.get('run_id') == current_run]
            earliest_current = min(
                (t.get('timestamp_ms', 0) for t in current_trades), default=0
            )
            for t in all_trades:
                if not t.get('run_id') and t.get('timestamp_ms', 0) >= earliest_current:
                    current_trades.append(t)
            prev_trades = []
            if prev_run:
                prev_all = [t for t in all_trades if t.get('run_id') == prev_run]
                prev_all.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
                prev_trades = prev_all[:20]
            trades = current_trades + prev_trades

        # Step 4: Running PnL calculation (same logic as read_recent_trades)
        trades.sort(key=lambda t: t.get('timestamp_ms', 0))
        positions = {}
        for trade in trades:
            action = trade.get('action')
            mint_full = trade.get('mint_full')
            amount_tokens = trade.get('amount_tokens')
            value_sol = trade.get('value_sol')
            wallet_delta = trade.get('wallet_sol_delta')

            if not mint_full or mint_full == "-" or not amount_tokens:
                continue

            if action == "BUY":
                # Cost: PREFER value_sol (fill_in) — actual swap amount
                cost_sol = value_sol if (value_sol is not None and value_sol > 0) else None
                if cost_sol is None and wallet_delta is not None:
                    cost_sol = abs(wallet_delta)
                if cost_sol is None or cost_sol <= 0:
                    continue
                pos = positions.get(mint_full, {"tokens": 0.0, "cost_sol": 0.0})
                pos["tokens"] += amount_tokens
                pos["cost_sol"] += cost_sol
                positions[mint_full] = pos
                trade["pnl_sol"] = 0.0
                trade["pnl_pct"] = None
            elif action == "SELL":
                # Proceeds: PREFER value_sol (fill_out) — wallet_delta doesn't include
                # WSOL swap output for PumpSwap SELL.
                proceeds_sol = value_sol if (value_sol is not None and value_sol > 0) else 0
                if proceeds_sol == 0:
                    proceeds_sol = wallet_delta if (wallet_delta is not None and wallet_delta > 0) else 0
                pos = positions.get(mint_full)
                if pos and pos["tokens"] > 0:
                    avg_cost = pos["cost_sol"] / pos["tokens"]
                    sold_cost = avg_cost * amount_tokens
                    pnl_sol = proceeds_sol - sold_cost
                    pos["tokens"] = max(0.0, pos["tokens"] - amount_tokens)
                    pos["cost_sol"] = max(0.0, pos["cost_sol"] - sold_cost)
                    positions[mint_full] = pos
                    trade["pnl_sol"] = round(pnl_sol, 9)
                    trade["pnl_pct"] = round((pnl_sol / sold_cost) * 100, 2) if sold_cost > 0 else None
                else:
                    trade["pnl_sol"] = None
                    trade["pnl_pct"] = None

        # Sort by timestamp descending for display
        trades.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
        return trades

    def serve_pnl_24h(self):
        """Serve 24h realized PnL summary as JSON."""
        result = self.compute_pnl_24h()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(result).encode())

    def compute_pnl_24h(self) -> dict:
        """Compute realized PnL for trades in the last 24 hours.

        Returns summary with realized_pnl_sol, trade_count, wins, losses.
        Uses the same FIFO cost-basis logic as the trades endpoint.
        """
        cutoff_ms = int((datetime.now() - timedelta(hours=24)).timestamp() * 1000)

        all_trades = self.load_all_trades(days_ago_list=[0, 1, 2])

        if not all_trades:
            return {"realized_pnl_sol": 0.0, "trade_count": 0, "wins": 0, "losses": 0}

        # Running PnL calculation over ALL trades (chronological) to build cost basis
        all_trades.sort(key=lambda t: t.get('timestamp_ms', 0))
        positions = {}
        realized_pnl = 0.0
        trade_count = 0
        wins = 0
        losses = 0

        for trade in all_trades:
            action = trade.get('action')
            mint_full = trade.get('mint_full')
            amount_tokens = trade.get('amount_tokens')
            value_sol = trade.get('value_sol')
            wallet_delta = trade.get('wallet_sol_delta')
            ts_ms = trade.get('timestamp_ms', 0)

            if not mint_full or mint_full == "-" or not amount_tokens:
                # Arbitrage trades: count PnL directly if within window
                if action == "ARBITRAGE" and ts_ms >= cutoff_ms:
                    pnl = trade.get('pnl_sol', 0) or 0
                    realized_pnl += pnl
                    trade_count += 1
                    if pnl >= 0:
                        wins += 1
                    else:
                        losses += 1
                continue

            if action == "BUY":
                # Cost: PREFER value_sol (fill_in) — actual swap amount
                cost_sol = value_sol if (value_sol is not None and value_sol > 0) else None
                if cost_sol is None and wallet_delta is not None:
                    cost_sol = abs(wallet_delta)
                if cost_sol is None or cost_sol <= 0:
                    continue
                pos = positions.get(mint_full, {"tokens": 0.0, "cost_sol": 0.0})
                pos["tokens"] += amount_tokens
                pos["cost_sol"] += cost_sol
                positions[mint_full] = pos
            elif action == "SELL":
                # Proceeds: PREFER value_sol (fill_out) — wallet_delta doesn't include
                # WSOL swap output for PumpSwap SELL.
                proceeds_sol = value_sol if (value_sol is not None and value_sol > 0) else 0
                if proceeds_sol == 0:
                    proceeds_sol = wallet_delta if (wallet_delta is not None and wallet_delta > 0) else 0
                pos = positions.get(mint_full)
                if pos and pos["tokens"] > 0:
                    avg_cost = pos["cost_sol"] / pos["tokens"]
                    sold_cost = avg_cost * amount_tokens
                    pnl_sol = proceeds_sol - sold_cost
                    pos["tokens"] = max(0.0, pos["tokens"] - amount_tokens)
                    pos["cost_sol"] = max(0.0, pos["cost_sol"] - sold_cost)
                    positions[mint_full] = pos

                    # Only count trades within the 24h window for the summary
                    if ts_ms >= cutoff_ms:
                        realized_pnl += pnl_sol
                        trade_count += 1
                        if pnl_sol >= 0:
                            wins += 1
                        else:
                            losses += 1

        return {
            "realized_pnl_sol": round(realized_pnl, 9),
            "trade_count": trade_count,
            "wins": wins,
            "losses": losses,
        }

    def read_recent_decisions(self, limit: int) -> list:
        """Read last N decision records with a simple status field."""
        decisions = []
        execution_by_intent = {}

        # Preload execution results for signature/token_mint when available.
        for days_ago in [0, 1, 2]:
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            jsonl_path = EXECUTIONS_DIR / f"execution_results-{date}.jsonl"
            if not jsonl_path.exists():
                continue
            try:
                with open(jsonl_path, 'r') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            intent_id = record.get('intent_id')
                            if intent_id:
                                execution_by_intent[intent_id] = record
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")

        for days_ago in [0, 1, 2]:
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            jsonl_path = DECISIONS_DIR / f"decision_records-{date}.jsonl"
            if not jsonl_path.exists():
                continue
            try:
                with open(jsonl_path, 'r') as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            decision = self.parse_decision_record(record, execution_by_intent)
                            if decision:
                                decisions.append(decision)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")

        decisions.sort(key=lambda d: d.get('timestamp_ms', 0), reverse=True)
        return decisions[:limit]
    
    def load_all_trades(self, days_ago_list: Optional[List[int]] = None) -> list:
        """Load confirmed trades from execution_results; fallback to recent_trades JSONL (P166)."""
        if days_ago_list is None:
            days_ago_list = [0, 1, 2]
        trades = []
        used_fallback = False
        for days_ago in days_ago_list:
            day_trades = self._load_trades_from_execution_jsonl([days_ago])
            day_recent = self._load_trades_from_recent_jsonl([days_ago])
            if days_ago == 0 and day_recent:
                if not day_trades:
                    used_fallback = True
                    day_trades = day_recent
                else:
                    day_trades = self._merge_trades_by_tx_hash(day_trades, day_recent)
            elif not day_trades and day_recent:
                used_fallback = True
                day_trades = day_recent
            trades.extend(day_trades)
        if used_fallback:
            print(
                "trades_server: using recent_trades JSONL fallback "
                "(execution_results empty or unparseable for one or more days)"
            )
        return trades

    def _merge_trades_by_tx_hash(self, primary: list, secondary: list) -> list:
        """Merge trade lists; primary (execution_results) wins on duplicate tx_hash."""
        by_hash = {}
        no_hash = []
        for t in secondary:
            h = t.get("tx_hash") or ""
            if h:
                by_hash[h] = t
            else:
                no_hash.append(t)
        for t in primary:
            h = t.get("tx_hash") or ""
            if h:
                by_hash[h] = t
            else:
                no_hash.append(t)
        return list(by_hash.values()) + no_hash

    def _load_trades_from_execution_jsonl(self, days_ago_list: list) -> list:
        trades = []
        for days_ago in days_ago_list:
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            jsonl_path = EXECUTIONS_DIR / f"execution_results-{date}.jsonl"
            if not jsonl_path.exists():
                continue
            try:
                with open(jsonl_path, "r") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            status = str(record.get("status") or "").lower()
                            if status in ("confirmed", "failed_confirmed"):
                                trade = self.parse_execution_result(record)
                                if trade:
                                    trades.append(trade)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")
        return trades

    def _load_trades_from_recent_jsonl(self, days_ago_list: list) -> list:
        """metrics::RecentTrade lines — used when execution_results file is empty (buffered)."""
        trades = []
        for days_ago in days_ago_list:
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            jsonl_path = RECENT_TRADES_DIR / f"recent_trades-{date}.jsonl"
            if not jsonl_path.exists():
                continue
            try:
                with open(jsonl_path, "r") as f:
                    for line in f:
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                            trade = self.parse_recent_trade(record)
                            if trade:
                                trades.append(trade)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")
        return trades

    def parse_recent_trade(self, record: dict) -> Optional[dict]:
        """Parse metrics::RecentTrade JSONL into Grafana-compatible trade format."""
        try:
            ts_ms = int(record.get("timestamp_ms") or 0)
            mint = record.get("mint") or ""
            action = (record.get("action") or "").upper()
            if action not in ("BUY", "SELL", "ARBITRAGE"):
                return None
            if mint and len(mint) > 15:
                display_mint = mint[:8] + "..." + mint[-4:]
            else:
                display_mint = mint or "-"
            amount_tokens = record.get("amount_tokens")
            value_sol = record.get("value_sol")
            pnl_sol = record.get("pnl_sol")
            pnl_pct = record.get("pnl_pct")
            return {
                "timestamp_ms": ts_ms,
                "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "action": action,
                "mint": display_mint,
                "tx_hash": record.get("tx_hash") or "",
                "amount_tokens": round(float(amount_tokens), 6) if amount_tokens is not None else None,
                "value_sol": round(float(value_sol), 9) if value_sol is not None else None,
                "pnl_sol": round(float(pnl_sol), 9) if pnl_sol is not None else None,
                "pnl_pct": round(float(pnl_pct), 2) if pnl_pct is not None else None,
                "wallet_sol_delta": None,
                "mint_full": mint,
                "reason": None,
                "reason_detail": None,
                "exit_type": None,
                "exit_reason": None,
                "run_id": record.get("run_id") or "",
            }
        except Exception as e:
            print(f"Error parsing recent trade: {e}")
            return None

    def parse_execution_result(self, record: dict) -> dict:
        """Parse execution_results JSONL record into Grafana-compatible trade format"""
        try:
            # Extract data from execution result
            ts_ms = record.get('ts_unix_ms', 0)
            run_id = record.get('run_id', '')
            signature = record.get('signature', '')
            fill_in = record.get('fill_in', {})
            fill_out = record.get('fill_out', {})
            source = record.get('source', 'unknown')
            intent_id = record.get('intent_id', '')
            metadata = record.get('metadata', {}) or {}
            if not isinstance(metadata, dict):
                metadata = {}
            
            # Get actual wallet SOL delta (includes all fees)
            wallet_sol_delta_lamports = record.get('wallet_sol_delta_lamports')
            wallet_sol_delta_sol = (
                wallet_sol_delta_lamports / 1e9
                if wallet_sol_delta_lamports is not None
                else None
            )
            
            # Get token_mint directly from execution record
            token_mint = record.get('token_mint', '')
            
            # ARBITRAGE DETECTION: Check source or intent_id prefix
            is_arbitrage = (
                source == 'arb-strategy' or 
                intent_id.startswith('arb-') or
                intent_id.startswith('mh-') or  # multi-hop
                'arb' in source.lower()
            )
            
            # LIQUIDATION DETECTION: intent_id prefix or intent metadata hints
            is_liquidation = (
                intent_id.startswith('liquidation-')
                or metadata.get('purpose') == 'liquidation'
                or str(metadata.get('kill_switch', '')).lower() in ('1', 'true', 'yes', 'y')
            )
            
            # Determine action from fill amounts
            fill_in_raw = fill_in.get('raw', 0)
            fill_in_decimals = fill_in.get('decimals', 9)
            fill_out_raw = fill_out.get('raw', 0)
            fill_out_decimals = fill_out.get('decimals', 9)
            
            # ============ ARBITRAGE ============
            if is_arbitrage:
                action = "ARBITRAGE"
                # For arbitrage: no single token mint, show "-"
                display_mint = "-"
                # Amount: not meaningful for arb, show "-"
                amount_tokens = None
                
                # Value = SOL spent/received (wallet delta preferred)
                if wallet_sol_delta_sol is not None:
                    value_sol = abs(wallet_sol_delta_sol)
                elif fill_in_decimals == 9:
                    value_sol = fill_in_raw / 1e9
                else:
                    value_sol = 0
                
                # PnL = actual wallet delta (negative = loss from fees, positive = profit)
                if wallet_sol_delta_lamports is not None:
                    pnl_sol = wallet_sol_delta_lamports / 1e9  # Keep sign: negative = loss
                else:
                    pnl_sol = 0.0
                
                return {
                    "timestamp_ms": ts_ms,
                    "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                    "action": action,
                    "mint": display_mint,  # Grafana expects "mint"
                    "tx_hash": signature,  # Full signature for link
                    "amount_tokens": None,  # Grafana expects "amount_tokens"
                    "value_sol": round(value_sol, 9),
                    "pnl_sol": round(pnl_sol, 9),
                    "pnl_pct": None,  # Not applicable for arb
                    "wallet_sol_delta": wallet_sol_delta_sol,
                    "mint_full": token_mint,
                    "reason": "ARBITRAGE",
                    "exit_type": None,
                    "exit_reason": None,
                    "run_id": run_id,
                }
            
            # ============ BUY / SELL ============
            # Determine action (BUY or SELL) using the most reliable signal available:
            #   1. Explicit side from metadata (set by execution-engine from intent.side)
            #   2. wallet_sol_delta heuristic (positive = received SOL = SELL)
            #   3. Fill amount heuristics (which side is SOL?)
            #   4. Override for liquidation (always SELL)
            #
            # Determine value_sol (SOL amount of the trade) using FILLS, not wallet_delta:
            #   - BUY:  value_sol = fill_in (SOL spent on the swap)
            #   - SELL: value_sol = fill_out (SOL/WSOL received from the swap)
            #
            # IMPORTANT: wallet_sol_delta measures native SOL balance change, which does NOT
            # account for WSOL flows. For SELL trades via PumpSwap AMM, the output is WSOL
            # in an ATA (not native SOL). wallet_sol_delta shows the ATA rent cost (negative!),
            # not the actual sell proceeds. Using it for value_sol gives wrong results.

            # Step 1: Determine action
            explicit_side = metadata.get('side', '').upper()
            if explicit_side in ('BUY', 'SELL'):
                action = explicit_side
            elif wallet_sol_delta_lamports is not None and wallet_sol_delta_lamports != 0:
                action = "SELL" if wallet_sol_delta_lamports > 0 else "BUY"
            else:
                # Fallback: use fill decimals to guess which side is SOL
                fill_in_is_sol = fill_in_raw > 0 and fill_in_decimals == 9
                fill_out_is_sol = fill_out_raw > 0 and fill_out_decimals == 9
                if fill_out_is_sol and not fill_in_is_sol:
                    action = "SELL"
                elif fill_in_is_sol and not fill_out_is_sol:
                    action = "BUY"
                elif fill_out_raw > fill_in_raw * 100:
                    action = "BUY"
                else:
                    action = "SELL"

            # Override for liquidations: always SELL
            if is_liquidation:
                action = "SELL"

            # Step 2: Determine value_sol and amount_tokens from fills
            if action == "BUY":
                # BUY: spent SOL (fill_in), received tokens (fill_out)
                amount_tokens = (
                    fill_out_raw / (10 ** fill_out_decimals)
                    if fill_out_raw and fill_out_decimals is not None
                    else 0
                )
                # value_sol: prefer fill_in (actual SOL spent on swap), fallback to wallet_delta
                if fill_in_raw > 0 and fill_in_decimals == 9:
                    value_sol = fill_in_raw / 1e9
                elif wallet_sol_delta_sol is not None:
                    value_sol = abs(wallet_sol_delta_sol)
                else:
                    value_sol = 0
            else:
                # SELL: sent tokens (fill_in), received SOL/WSOL (fill_out)
                amount_tokens = (
                    fill_in_raw / (10 ** fill_in_decimals)
                    if fill_in_raw and fill_in_decimals is not None
                    else 0
                )
                # value_sol: prefer fill_out (actual SOL/WSOL received from swap),
                # fallback to positive wallet_delta, then 0
                if fill_out_raw > 0 and fill_out_decimals == 9:
                    value_sol = fill_out_raw / 1e9
                elif wallet_sol_delta_sol is not None and wallet_sol_delta_sol > 0:
                    value_sol = wallet_sol_delta_sol
                else:
                    value_sol = 0

            # Truncate token mint for display (keep first 8 + last 4 chars)
            if token_mint and len(token_mint) > 15:
                display_mint = token_mint[:8] + "..." + token_mint[-4:]
            else:
                display_mint = token_mint or "-"
            
            # Unified reason field for dashboard:
            # - BUY: short entry category (ENTER_PROBE / ENTER_SCALE_IN)
            # - SELL: exit category (exit_type), with liquidation fallback
            # reason_detail: full detail text for both BUY and SELL (shown in tooltip column)
            exit_type = metadata.get('exit_type')
            raw_reason_detail = metadata.get('reason_detail')
            reason_code = metadata.get('reason_code')
            entry_kind = metadata.get('entry_kind', '')

            reason = None
            detail = None
            if action == "BUY":
                # Short reason for display: extract category prefix
                if entry_kind == 'scale_in':
                    reason = "ENTER_SCALE_IN"
                elif entry_kind == 'probe':
                    reason = "ENTER_PROBE"
                elif raw_reason_detail:
                    # Extract prefix before ':' (e.g., "ENTER_PROBE_BUY: All filters..." → "ENTER_PROBE_BUY")
                    colon_idx = raw_reason_detail.find(':')
                    reason = raw_reason_detail[:colon_idx].strip() if colon_idx > 0 else raw_reason_detail[:30]
                else:
                    reason = reason_code or "BUY"
                # Full detail for tooltip
                detail = raw_reason_detail or reason_code
            elif action == "SELL":
                reason = exit_type
                if not reason:
                    if is_liquidation:
                        reason = "LIQUIDATION"
                    elif str(metadata.get('kill_switch', '')).lower() in ('1', 'true', 'yes', 'y'):
                        reason = "KILL_SWITCH"
                    elif reason_code and str(reason_code).startswith("EXIT_"):
                        reason = str(reason_code).removeprefix("EXIT_")
                    else:
                        reason = raw_reason_detail or reason_code
                # Full detail for tooltip
                detail = raw_reason_detail
            
            return {
                "timestamp_ms": ts_ms,
                "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "action": action,
                "mint": display_mint,  # Grafana expects "mint"
                "tx_hash": signature,  # Full signature for link
                "amount_tokens": round(amount_tokens, 6) if amount_tokens else None,
                "value_sol": round(value_sol, 9) if value_sol else None,
                "pnl_sol": None,  # Filled in during running PnL calculation
                "pnl_pct": None,
                "wallet_sol_delta": wallet_sol_delta_sol,
                "mint_full": token_mint,
                "reason": reason,
                "reason_detail": detail,
                "exit_type": exit_type,
                "exit_reason": raw_reason_detail,
                "run_id": run_id,
            }
        except Exception as e:
            print(f"Error parsing execution result: {e}")
            return None

    def parse_decision_record(self, record: dict, execution_by_intent: dict) -> dict:
        """Parse decision record into a simple status view for Grafana."""
        try:
            ts_ms = record.get('ts_unix_ms', 0)
            intent_id = record.get('intent_id', '')
            decision_id = record.get('decision_id', '')
            source = record.get('source', 'unknown')
            outcome = record.get('outcome', 'Rejected')
            reject_reason = record.get('primary_reject_reason')

            status_map = {
                "Confirmed": "confirmed",
                "Sent": "sent",
                "FailedConfirmed": "failed_confirmed",
                "SimFailed": "rejected",
                "Expired": "rejected",
                "Rejected": "rejected",
            }
            status = status_map.get(outcome, "rejected")

            exec_record = execution_by_intent.get(intent_id, {})
            signature = exec_record.get('signature')
            token_mint = exec_record.get('token_mint')

            if not signature:
                signature = (record.get('send') or {}).get('signature')

            return {
                "timestamp_ms": ts_ms,
                "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "decision_id": decision_id,
                "intent_id": intent_id,
                "source": source,
                "status": status,
                "outcome": outcome,
                "tx_hash": signature,
                "token_mint": token_mint,
                "reject_reason": reject_reason,
            }
        except Exception as e:
            print(f"Error parsing decision record: {e}")
            return None
    
    def log_message(self, format, *args):
        # Suppress default logging
        pass


def _self_check():
    """Quick sanity check for time_mode fields and recent_trades parser (no server)."""
    h = TradesHandler.__new__(TradesHandler)
    ts_ms = 1_700_000_000_000
    expected_utc = (
        datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        + " UTC"
    )
    row = {"timestamp_ms": ts_ms}
    TradesHandler._apply_time_mode_fields(h, [row], "relative")
    assert row.get("time_utc") == expected_utc
    assert row.get("time_display") == row.get("time_age")
    TradesHandler._apply_time_mode_fields(h, [row], "utc")
    assert row.get("time_display") == expected_utc
    parsed = h.parse_recent_trade(
        {
            "timestamp_ms": ts_ms,
            "mint": "So11111111111111111111111111111111111111112",
            "action": "BUY",
            "tx_hash": "sig",
            "amount_tokens": 1.5,
            "value_sol": 0.01,
        }
    )
    assert parsed and parsed["action"] == "BUY" and parsed["mint_full"]
    print("trades_server: self-check OK")


def main():
    parser = argparse.ArgumentParser(description='Trades history server')
    parser.add_argument('--port', type=int, default=9899, help='Port to listen on (default: 9899)')
    parser.add_argument(
        '--self-check',
        action='store_true',
        help='Run a quick local check of time_mode fields and exit',
    )
    args = parser.parse_args()

    if args.self_check:
        _self_check()
        return

    server = http.server.HTTPServer(('0.0.0.0', args.port), TradesHandler)
    print(f"Trades server running on http://0.0.0.0:{args.port}/trades")
    print(f"Reading from: {EXECUTIONS_DIR}")
    server.serve_forever()

if __name__ == '__main__':
    main()
