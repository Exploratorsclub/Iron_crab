#!/usr/bin/env python3
"""
Simple HTTP server that serves trade data from CSV files.
Run this separately from the bot to keep trade history available even when bot is stopped.

Usage: python3 trades_server.py [--port 9899]
"""

import http.server
import json
import csv
import os
from datetime import datetime
from pathlib import Path
import argparse

TRADE_LOG_DIR = os.environ.get("IRONCRAB_TRADE_LOG_DIR", "trade_logs")

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
        """Read last N trades from today's CSV file"""
        today = datetime.now().strftime("%Y%m%d")
        csv_path = Path(TRADE_LOG_DIR) / f"trades-{today}.csv"
        
        if not csv_path.exists():
            # Try yesterday's file if today's doesn't exist
            yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
            csv_path = Path(TRADE_LOG_DIR) / f"trades-{yesterday}.csv"
            if not csv_path.exists():
                return []
        
        trades = []
        try:
            with open(csv_path, 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                # Take last N, reverse to show newest first
                for row in rows[-limit:][::-1]:
                    trade = self.parse_row(row)
                    if trade:
                        trades.append(trade)
        except Exception as e:
            print(f"Error reading CSV: {e}")
        
        return trades
    
    def parse_row(self, row: dict) -> dict:
        """Parse a CSV row into trade format"""
        try:
            ts = datetime.fromisoformat(row.get('timestamp_utc', '').replace('+00:00', '+00:00'))
            timestamp_ms = int(ts.timestamp() * 1000)
        except:
            timestamp_ms = 0
        
        action = row.get('side', '')
        lamports_in = int(row.get('lamports_in', 0) or 0)
        lamports_out = int(row.get('lamports_out', 0) or 0)
        tokens_in_raw = float(row.get('tokens_in', 0) or 0)
        tokens_out_raw = float(row.get('tokens_out', 0) or 0)
        expected_tokens_out_raw = float(row.get('expected_tokens_out', 0) or 0)
        
        # What matters: SOL amount of the trade (not price per token!)
        if action == 'BUY':
            amount_tokens_raw = tokens_out_raw if tokens_out_raw > 0 else expected_tokens_out_raw
            sol_amount = lamports_in / 1e9  # SOL spent
        else:  # SELL
            amount_tokens_raw = tokens_in_raw
            sol_amount = lamports_out / 1e9  # SOL received
        
        # Get PnL for sells (already in CSV)
        pnl_sol = None
        if action == 'SELL':
            try:
                pnl_sol = float(row.get('realized_pnl_sol', '') or 0)
            except:
                pass
        
        return {
            'timestamp_ms': timestamp_ms,
            'mint': row.get('mint', ''),
            'action': action,
            'tx_hash': row.get('signature', ''),
            'amount_tokens': amount_tokens_raw,  # Raw token amount (for reference)
            'price_sol': sol_amount,  # Actually: SOL amount of this trade!
            'pnl_sol': pnl_sol,
            'pnl_pct': None,
            'latency_ms': None
        }
    
    def log_message(self, format, *args):
        # Suppress default logging
        pass

from datetime import timedelta

def main():
    parser = argparse.ArgumentParser(description='Trades history server')
    parser.add_argument('--port', type=int, default=9899, help='Port to listen on (default: 9899)')
    args = parser.parse_args()
    
    server = http.server.HTTPServer(('0.0.0.0', args.port), TradesHandler)
    print(f"Trades server running on http://0.0.0.0:{args.port}/trades")
    print(f"Reading from: {TRADE_LOG_DIR}")
    server.serve_forever()

if __name__ == '__main__':
    main()
