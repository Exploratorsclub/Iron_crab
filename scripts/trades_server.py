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

class TradesHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/trades" or self.path == "/trades/":
            self.serve_trades()
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
    
    def read_recent_trades(self, limit: int) -> list:
        """Read last N trades from execution_results JSONL files and join with decisions for mint"""
        trades = []
        decision_cache = {}  # Cache decision records by decision_id
        
        # Load decision records first (for mint lookup)
        for days_ago in [0, 1, 2]:
            date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y%m%d")
            decisions_path = Path(TRADE_LOG_DIR) / "decisions" / f"decision_records-{date}.jsonl"
            
            if decisions_path.exists():
                try:
                    with open(decisions_path, 'r') as f:
                        for line in f:
                            if not line.strip():
                                continue
                            try:
                                dec = json.loads(line)
                                decision_id = dec.get("decision_id")
                                if decision_id and dec.get("outcome") == "Confirmed":
                                    # Extract mint from capital_lock check details
                                    mint = None
                                    for check in dec.get("checks", []):
                                        if check.get("check_name") == "capital_lock":
                                            details = check.get("details", "")
                                            if details.startswith("token:"):
                                                mint = details[6:]  # Remove "token:" prefix
                                                break
                                    
                                    decision_cache[decision_id] = {
                                        "mint": mint
                                    }
                            except json.JSONDecodeError:
                                continue
                except Exception:
                    pass
        
        # Now read execution results
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
                            # Only include confirmed/successful executions
                            if record.get('status') == 'Confirmed':
                                decision_id = record.get('decision_id')
                                decision_data = decision_cache.get(decision_id, {})
                                trade = self.parse_execution_result(record, decision_data)
                                if trade:
                                    trades.append(trade)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")
        
        # Sort by timestamp descending, take last N
        trades.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
        return trades[:limit]
    
    def parse_execution_result(self, record: dict, decision_data: dict) -> dict:
        """Parse execution_results JSONL record into Grafana-compatible trade format"""
        try:
            # Extract data from execution result
            ts_ms = record.get('ts_unix_ms', 0)
            signature = record.get('signature', '')
            fill_in = record.get('fill_in', {})
            fill_out = record.get('fill_out', {})
            
            # NEW: Get actual wallet SOL delta (includes all fees)
            wallet_sol_delta_lamports = record.get('wallet_sol_delta_lamports')
            
            # Determine action from fill amounts
            # If fill_in has tokens (>1e6 raw), it's a SELL
            # If fill_out has tokens, it's a BUY
            fill_in_raw = fill_in.get('raw', 0)
            fill_in_decimals = fill_in.get('decimals', 9)
            fill_out_raw = fill_out.get('raw', 0)
            fill_out_decimals = fill_out.get('decimals', 9)
            
            # Heuristic: tokens have 6-9 decimals, SOL has 9
            # If fill_in has high decimals and large raw, it's tokens (SELL)
            is_sell = fill_in_decimals <= 9 and fill_in_raw > 1_000_000
            
            if is_sell:
                action = "SELL"
                amount_tokens = fill_in_raw / (10 ** fill_in_decimals)
                token_mint = decision_data.get("mint", "unknown")
            else:
                action = "BUY"
                amount_tokens = fill_out_raw / (10 ** fill_out_decimals)
                token_mint = decision_data.get("mint", "unknown")
            
            # Prefer wallet_sol_delta_lamports (actual SOL change) over WSOL token amounts
            if wallet_sol_delta_lamports is not None:
                # wallet_sol_delta_lamports: positive = gained SOL, negative = lost SOL
                # For price_sol, we want absolute amount
                sol_amount = abs(wallet_sol_delta_lamports) / 1e9
            else:
                # Fallback to WSOL token amounts (old behavior)
                if is_sell:
                    sol_amount = fill_out_raw / 1e9  # WSOL received
                else:
                    sol_amount = fill_in_raw / 1e9  # SOL paid
            
            price_sol = sol_amount
            
            return {
                "timestamp_ms": ts_ms,
                "time": datetime.fromtimestamp(ts_ms / 1000).strftime("%Y-%m-%d %H:%M:%S"),
                "action": action,
                "token_mint": token_mint[:20] + "..." if token_mint != "unknown" else token_mint,
                "tx_hash": signature,
                "amount": amount_tokens,
                "price_sol": round(price_sol, 9),
                "pnl_sol": 0.0,  # Would need position tracking
                "pnl_percent": 0.0  # Would need position tracking
            }
        except Exception as e:
            print(f"Error parsing execution result: {e}")
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
