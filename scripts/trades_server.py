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
        
        # Compute running PnL using fill-based value_sol.
        #
        # IMPORTANT: We use value_sol (derived from fill_in/fill_out) instead of
        # wallet_sol_delta for PnL calculation. wallet_sol_delta measures native SOL
        # balance change which does NOT account for WSOL flows (e.g., sells via
        # PumpSwap AMM receive WSOL in an ATA, not native SOL). Using wallet_delta
        # as "proceeds" gives wildly wrong PnL (>100% loss on successful sells).
        #
        # For BUY:  cost = value_sol (SOL spent on swap from fill_in)
        # For SELL: proceeds = value_sol (SOL/WSOL received from swap from fill_out)
        # PnL = proceeds - proportional_cost (can be negative but never < -100%)
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
                # Cost: prefer wallet_delta (includes all fees), fallback to value_sol
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
                # Proceeds: use value_sol (from fill_out, the actual swap output).
                # NOT wallet_delta which can be negative due to ATA rent for WSOL.
                proceeds_sol = value_sol
                if proceeds_sol is None or proceeds_sol == 0:
                    # Last resort: try positive wallet_delta
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
            # - BUY: entry reason (reason_detail when available)
            # - SELL: exit category (exit_type), with liquidation fallback
            exit_type = metadata.get('exit_type')
            reason_detail = metadata.get('reason_detail')
            reason_code = metadata.get('reason_code')

            reason = None
            if action == "BUY":
                reason = reason_detail or reason_code
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
                        reason = reason_detail or reason_code
            
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
                "exit_type": exit_type,
                "exit_reason": reason_detail,
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
