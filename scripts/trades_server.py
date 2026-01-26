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
                            # Only include confirmed/successful executions
                            if record.get('status') == 'Confirmed':
                                trade = self.parse_execution_result(record)
                                if trade:
                                    trades.append(trade)
                        except json.JSONDecodeError:
                            continue
            except Exception as e:
                print(f"Error reading {jsonl_path}: {e}")
        
        # Sort by timestamp descending, take last N
        trades.sort(key=lambda t: t.get('timestamp_ms', 0), reverse=True)
        return trades[:limit]
    
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
                
                # Price = WSOL spent on buy leg (fill_in with 9 decimals = SOL)
                if fill_in_decimals == 9:
                    price_sol = fill_in_raw / 1e9
                else:
                    price_sol = 0
                
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
                    "price_sol": round(price_sol, 9),
                    "pnl_sol": round(pnl_sol, 9),
                    "pnl_pct": None  # Not applicable for arb
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
                if fill_out_decimals == 9:
                    price_sol = fill_out_raw / 1e9
                else:
                    price_sol = 0
            elif fill_out_is_tokens:
                # Received tokens, paid SOL = BUY
                action = "BUY"
                amount_tokens = fill_out_raw / (10 ** fill_out_decimals)
                # SOL paid
                if fill_in_decimals == 9:
                    price_sol = fill_in_raw / 1e9
                else:
                    price_sol = 0
            else:
                # Fallback heuristics
                if fill_out_raw > fill_in_raw * 100:
                    action = "BUY"
                    amount_tokens = fill_out_raw / (10 ** fill_out_decimals)
                    price_sol = fill_in_raw / 1e9 if fill_in_decimals == 9 else 0
                else:
                    action = "SELL"
                    amount_tokens = fill_in_raw / (10 ** fill_in_decimals) if fill_in_raw > 0 else 0
                    price_sol = fill_out_raw / 1e9 if fill_out_decimals == 9 else 0
            
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
                "price_sol": round(price_sol, 9) if price_sol else None,
                "pnl_sol": 0.0,  # Would need position tracking for momentum
                "pnl_pct": None
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
