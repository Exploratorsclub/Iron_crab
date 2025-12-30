# Golden Replay Fixtures

This directory contains "golden" test scenarios for deterministic replay testing.

## Structure

Each scenario has:
- `{scenario}_intents.jsonl` - Input TradeIntents
- `{scenario}_expected.jsonl` - Expected DecisionRecords

## Scenarios

1. **normal_trade** - Standard successful trade flow
2. **rejected_trade** - Trade rejected due to risk limits
3. **sim_failed** - Simulation failure scenario

## Usage

These fixtures are used by `tests/golden_replay_test.rs` to ensure
the execution pipeline produces deterministic, reproducible results.

Any change to the decision logic should either:
1. Keep the fixtures passing (behavior unchanged)
2. Update the fixtures with a documented reason (intentional change)

## DoD Reference

This implements DoD N) P1: Golden Replay Tests
