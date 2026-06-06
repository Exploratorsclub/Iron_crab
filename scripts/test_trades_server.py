"""Tests for trades_server JSONL loading (rotation, run fast-path, pre_confirm skip)."""

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
import trades_server as ts  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_cache():
    ts.clear_trades_cache()
    yield
    ts.clear_trades_cache()


@pytest.fixture
def log_dirs(tmp_path, monkeypatch):
    """Point EXECUTIONS_DIR and RECENT_TRADES_DIR at a temp tree."""
    exec_dir = tmp_path / "executions"
    recent_dir = tmp_path / "recent"
    exec_dir.mkdir()
    recent_dir.mkdir()
    monkeypatch.setattr(ts, "EXECUTIONS_DIR", exec_dir)
    monkeypatch.setattr(ts, "RECENT_TRADES_DIR", recent_dir)
    fixed_date = "20990606"
    monkeypatch.setattr(ts, "_utc_date_str", lambda days_ago: fixed_date)
    return exec_dir, recent_dir, fixed_date


def _minimal_buy_record(
    signature: str,
    *,
    run_id: str = "run-current",
    ts_ms: int = 1_800_000_000_000,
    side: str = "BUY",
    exit_type: str | None = None,
    phase: str | None = None,
    status: str = "confirmed",
) -> dict:
    metadata = {"side": side}
    if exit_type:
        metadata["exit_type"] = exit_type
    if phase:
        metadata["phase"] = phase
    return {
        "status": status,
        "ts_unix_ms": ts_ms,
        "run_id": run_id,
        "signature": signature,
        "token_mint": "Mint1111111111111111111111111111111111111",
        "fill_in": {"raw": 10_000_000, "decimals": 9},
        "fill_out": {"raw": 1_000_000, "decimals": 6},
        "metadata": metadata,
    }


def _minimal_sell_record(
    signature: str,
    *,
    run_id: str = "run-current",
    ts_ms: int = 1_800_000_100_000,
    exit_type: str = "TAKE_PROFIT",
) -> dict:
    return _minimal_buy_record(
        signature,
        run_id=run_id,
        ts_ms=ts_ms,
        side="SELL",
        exit_type=exit_type,
    )


def _recent_trade_line(
    signature: str,
    action: str,
    *,
    ts_ms: int = 1_800_000_100_000,
    run_id: str = "",
) -> dict:
    return {
        "timestamp_ms": ts_ms,
        "mint": "Mint1111111111111111111111111111111111111",
        "action": action,
        "tx_hash": signature,
        "amount_tokens": 1.0,
        "value_sol": 0.01,
        "run_id": run_id,
    }


class TestRotatedExecutionResults:
    def test_base_and_rotated_segment_both_loaded(self, log_dirs):
        exec_dir, _, date = log_dirs
        base = exec_dir / f"execution_results-{date}.jsonl"
        rotated = exec_dir / f"execution_results-{date}.5.jsonl"
        base.write_text(
            json.dumps(_minimal_buy_record("sig-base")) + "\n", encoding="utf-8"
        )
        rotated.write_text(
            json.dumps(_minimal_buy_record("sig-rotated", ts_ms=1_800_000_050_000))
            + "\n",
            encoding="utf-8",
        )

        handler = ts.TradesHandler.__new__(ts.TradesHandler)
        trades = handler._load_trades_from_execution_jsonl([0])
        hashes = {t["tx_hash"] for t in trades}

        assert hashes == {"sig-base", "sig-rotated"}


class TestPreConfirmSkip:
    def test_pre_confirm_track_excluded(self, log_dirs):
        exec_dir, _, date = log_dirs
        path = exec_dir / f"execution_results-{date}.jsonl"
        lines = [
            _minimal_buy_record("good", phase=None),
            _minimal_buy_record("noise", phase="pre_confirm_track"),
            _minimal_buy_record("pending", status="sent"),
        ]
        path.write_text(
            "\n".join(json.dumps(r) for r in lines) + "\n", encoding="utf-8"
        )

        handler = ts.TradesHandler.__new__(ts.TradesHandler)
        trades = handler._load_trades_from_execution_jsonl([0])

        assert len(trades) == 1
        assert trades[0]["tx_hash"] == "good"


class TestRunModeFastPath:
    def test_recent_sell_enriched_with_execution_reason(self, log_dirs):
        exec_dir, recent_dir, date = log_dirs
        buy_sig = "buy-sig"
        sell_sig = "sell-sig"

        exec_path = exec_dir / f"execution_results-{date}.jsonl"
        exec_path.write_text(
            json.dumps(_minimal_buy_record(buy_sig, ts_ms=1_800_000_000_000))
            + "\n"
            + json.dumps(
                _minimal_sell_record(
                    sell_sig, ts_ms=1_800_000_100_000, exit_type="TAKE_PROFIT"
                )
            )
            + "\n",
            encoding="utf-8",
        )

        recent_path = recent_dir / f"recent_trades-{date}.jsonl"
        recent_path.write_text(
            json.dumps(_recent_trade_line(buy_sig, "BUY", ts_ms=1_800_000_000_000))
            + "\n"
            + json.dumps(
                _recent_trade_line(sell_sig, "SELL", ts_ms=1_800_000_100_000)
            )
            + "\n",
            encoding="utf-8",
        )

        handler = ts.TradesHandler.__new__(ts.TradesHandler)
        trades = handler.read_trades_by_run()
        actions = {t["action"]: t for t in trades}

        assert "SELL" in actions
        assert actions["SELL"]["reason"] == "TAKE_PROFIT"
        assert actions["BUY"]["tx_hash"] == buy_sig

    def test_run_id_less_recent_included_in_current_run_window(self, log_dirs):
        exec_dir, recent_dir, date = log_dirs
        exec_path = exec_dir / f"execution_results-{date}.jsonl"
        exec_path.write_text(
            json.dumps(_minimal_buy_record("exec-buy", run_id="run-A")) + "\n",
            encoding="utf-8",
        )

        recent_path = recent_dir / f"recent_trades-{date}.jsonl"
        recent_path.write_text(
            json.dumps(
                _recent_trade_line(
                    "recent-sell",
                    "SELL",
                    ts_ms=1_800_000_000_000,
                    run_id="",
                )
            )
            + "\n",
            encoding="utf-8",
        )

        handler = ts.TradesHandler.__new__(ts.TradesHandler)
        trades = handler.read_trades_by_run()
        hashes = {t["tx_hash"] for t in trades}

        assert "recent-sell" in hashes
        assert "exec-buy" in hashes


class TestCache:
    def test_cache_returns_same_object_within_ttl(self, log_dirs, monkeypatch):
        exec_dir, _, date = log_dirs
        path = exec_dir / f"execution_results-{date}.jsonl"
        path.write_text(
            json.dumps(_minimal_buy_record("cached")) + "\n", encoding="utf-8"
        )

        handler = ts.TradesHandler.__new__(ts.TradesHandler)
        first = handler.load_all_trades([0])
        second = handler.load_all_trades([0])
        assert first is second

        path.write_text(
            json.dumps(_minimal_buy_record("cached-new")) + "\n", encoding="utf-8"
        )
        third = handler.load_all_trades([0])
        assert third is not first
        assert third[0]["tx_hash"] == "cached-new"
