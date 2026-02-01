#!/usr/bin/env python3
"""
Simple HTTP server that serves trade data from JSONL execution_results files.
Run this separately from the bot to keep trade history available even when bot is stopped.

Reads from: trade_logs/executions/execution_results-YYYYMMDD.jsonl
Grafana Infinity data source connects to this for "Recent Trades" table panel.

Usage: python3 trades_server.py [--port 9899]
"""

import http.server
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
import argparse

TRADE_LOG_DIR = os.environ.get("IRONCRAB_LOG_DIR", "trade_logs")
EXECUTIONS_DIR = Path(TRADE_LOG_DIR) / "executions"
DECISIONS_DIR = Path(TRADE_LOG_DIR) / "decisions"

class TradesHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/trades" or self.path == "/trades/":
            self.serve_trades()
        elif self.path == "/decisions" or self.path == "/decisions/":
            self.serve_decisions()
        elif self.path == "/health":
            self.send_response(200)
            self.send_header("Content-Type", "text/plain")
            self.end_headers()
            self.wfile.write(b"ok")
        else:
            self.send_response(404)
            self.end_headers()
    
    def serve_trades(self):
        trades = self.read_recent_trades(20)
        
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(trades).encode())

    def serve_decisions(self):
        decisions = self.read_recent_decisions(200)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(decisions).encode())
    
    def read_recent_trades(self, limit: int) -> list:
        """Read last N trades from execution_results JSONL files (mint now included in execution records)"""
        trades = []
        
        # Read execution results directly (no join needed - mint is in the record)
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
                            # Only include confirmed/successful executions (snake_case or legacy)
                            status = str(record.get('status') or '').lower()
                            if status in ('confirmed', 'failed_confirmed'):
                                trade = self.parse_execution_result(record)
                                if trade:
                                    trades.append(trade)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")
        
        # Compute running PnL using wallet deltas (fees included)
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
                cost_sol = abs(wallet_delta) if wallet_delta is not None else value_sol
                if cost_sol is None:
                    continue
                pos = positions.get(mint_full, {"tokens": 0.0, "cost_sol": 0.0})
                pos["tokens"] += amount_tokens
                pos["cost_sol"] += cost_sol
                positions[mint_full] = pos
                trade["pnl_sol"] = 0.0
                trade["pnl_pct"] = None
            elif action == "SELL":
                proceeds_sol = wallet_delta if wallet_delta is not None else value_sol
                if proceeds_sol is None:
                    continue
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
    
    def parse_execution_result(self, record: dict) -> dict:
        """Parse execution_results JSONL record into Grafana-compatible trade format"""
        try:
            # Extract data from execution result
            ts_ms = record.get('ts_unix_ms', 0)
            signature = record.get('signature', '')
            fill_in = record.get('fill_in', {})
            fill_out = record.get('fill_out', {})
            source = record.get('source', 'unknown')
            intent_id = record.get('intent_id', '')
            
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
            
            # LIQUIDATION DETECTION: Check intent_id prefix
            is_liquidation = intent_id.startswith('liquidation-')
            
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
                }
            
            # ============ BUY / SELL ============
            # Check which side has tokens (6 decimals typically) vs SOL (9 decimals)
            fill_out_is_tokens = fill_out_raw > 0 and fill_out_decimals == 6
            fill_in_is_tokens = fill_in_raw > 0 and fill_in_decimals == 6
            
            if fill_in_is_tokens:
                # Sent tokens, received SOL = SELL
                action = "SELL"
                amount_tokens = fill_in_raw / (10 ** fill_in_decimals)
                # SOL received
                if wallet_sol_delta_sol is not None:
                    value_sol = abs(wallet_sol_delta_sol)
                elif fill_out_decimals == 9:
                    value_sol = fill_out_raw / 1e9
                else:
                    value_sol = 0
            elif fill_out_is_tokens:
                # Received tokens, paid SOL = BUY
                action = "BUY"
                amount_tokens = fill_out_raw / (10 ** fill_out_decimals)
                # SOL paid
                if wallet_sol_delta_sol is not None:
                    value_sol = abs(wallet_sol_delta_sol)
                elif fill_in_decimals == 9:
                    value_sol = fill_in_raw / 1e9
                else:
                    value_sol = 0
            else:
                # Fallback heuristics
                if fill_out_raw > fill_in_raw * 100:
                    action = "BUY"
                    amount_tokens = fill_out_raw / (10 ** fill_out_decimals)
                    value_sol = fill_in_raw / 1e9 if fill_in_decimals == 9 else 0
                else:
                    action = "SELL"
                    amount_tokens = fill_in_raw / (10 ** fill_in_decimals) if fill_in_raw > 0 else 0
                    value_sol = fill_out_raw / 1e9 if fill_out_decimals == 9 else 0
            
            # Truncate token mint for display (keep first 8 + last 4 chars)
            if token_mint and len(token_mint) > 15:
                display_mint = token_mint[:8] + "..." + token_mint[-4:]
            else:
                display_mint = token_mint or "-"
            
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


def main():
    parser = argparse.ArgumentParser(description='Trades history server')
    parser.add_argument('--port', type=int, default=9899, help='Port to listen on (default: 9899)')
    args = parser.parse_args()
    
    server = http.server.HTTPServer(('0.0.0.0', args.port), TradesHandler)
    print(f"Trades server running on http://0.0.0.0:{args.port}/trades")
    print(f"Reading from: {EXECUTIONS_DIR}")
    server.serve_forever()

if __name__ == '__main__':
    main()
