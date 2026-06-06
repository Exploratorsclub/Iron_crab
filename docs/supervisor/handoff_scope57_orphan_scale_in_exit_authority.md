# Handoff: Scope 57 — Orphan Recovery State-Machine + Exit Authority Hint

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

**Prod-Symptome (2026-06-06, nach PR3.2 Confirm-Fix):**

1. **Keine Scale-in-Intents:** `ENTER_SCALE_IN` / `ENTER_SCALE_IN_BUY` = 0 in ~2h; dagegen 42× `ENTER_PROBE`, 11× `ORPHANED BUY RECOVERED`, 8× `Position opened`.
2. **Offene Positionen werden nicht vollstaendig verkauft:** PumpFun SELL-Fails (`6024`/`6023`), EE-Reject `SIM_INSUFFICIENT_BALANCE`, `RISK_MAX_OPEN_POSITIONS`.
3. **Drift:** EE `open_positions=5`, Momentum-Gauge oft `0`; `exits_generated_total` hoch, aber Restpositionen bleiben on-chain / in LockManager.

**Root Cause (validiert):**

- Orphan-Pfad ruft `open_position()` ohne Tracker-State-Update → Scale-in-Gate (`PositionOpenProbe`) wird nie erreicht.
- Exit-Sizing nutzt Momentum-Overlay; JetStream `WalletBalanceSnapshot` war nicht als Authority-Hint angebunden (PA-5 offen).

**Phase-1-Fix (dieser Scope):** Orphan-Pfad an normale State-Machine; Exit-Amount aus Wallet-Snapshot wenn verfuegbar; Metriken.

**Phase-2-Follow-up (separates Epic):** PA-4/PA-5/PA-6 laut `Iron_crab-eval/docs/plans/plan_position_authority_sot_migration.md` — dediziertes `position-manager`-Binary optional ab PA-6.

## Relevante Invarianten (Volltext)

### I-7 Hot Path RPC-Freiheit

Keine blockierenden RPC-Calls in `process_intent`, `check_for_signals`, `process_exit_signals`, `generate_and_publish_exit_intent`. Authority-Snapshot nur aus Geyser/JetStream/KV/in-process Cache.

### I-9 Simulation-Gate

Keine TX ohne erfolgreiche Simulation. Exit-Amount-Aenderung darf Sim nicht umgehen.

### I-12 Decision Record

Orphan-Recovery und Exit-Suppression muessen geloggt/metriciert werden, nicht still.

### I-13 Position-Pool-Matching

Exit-Quotes und Scale-in-Gate nur aus `position.pool` / matching TokenTracker.

### I-14 tokens_per_sol

Scale-in-Gate und Exit-PnL weiter in einheitlicher `tokens_per_sol`-Konvention (Scope 56 Fix beibehalten).

## Bestehendes Pattern

Normaler BUY-Confirm setzt Tracker-State (`momentum_bot.rs` ~7318–7328):

```rust
match pending.entry_kind {
    Some(EntryKind::Probe) => {
        tr.state = TrackerState::PositionOpenProbe { filled_at: Instant::now() };
    }
    Some(EntryKind::ScaleIn) => {
        tr.state = TrackerState::PositionOpenFull { filled_at: Instant::now() };
    }
    None => {}
}
```

Orphan-Pfad muss dasselbe Pattern nach `open_position()` anwenden.

Scope 50 `exit_generated`-Reset in `open_position` bei scale-in add beibehalten.

## Implementierung (Scope 57)

1. **Orphan Probe:** `PositionOpenProbe { filled_at }` nach neuer Position.
2. **Orphan Scale-in (existing position):** `PositionOpenFull { filled_at }`.
3. **Exit sizing:** `resolve_exit_token_amount_raw` — bevorzugt gecachtes `WalletBalanceSnapshot` (JetStream, kein RPC).
4. **Metriken:** `momentum_orphan_probe_recovery_total`, `momentum_orphan_scale_in_recovery_total`, `momentum_exit_amount_overlay_only_total`, `momentum_scale_in_gate_blocked_total{reason=...}`.

## Erlaubte Dateien

- `src/bin/momentum_bot.rs`
- `src/metrics.rs`
- Unit-Tests in `momentum_bot.rs` test module

## Verboten

- Neues position-manager-Binary
- RPC im Hot Path
- Simulation-Gate oder I-12 Verwerfen ohne Record
- Big-Bang Entfernung von `momentum.positions`

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy -- -D warnings
cargo test --bin momentum-bot
```

## PR-Titel

`fix(momentum): orphan recovery sets PositionOpenProbe + exit amount authority hint (Scope 57)`

Branch: `architecture-rebuild`

## Prod-Verifikation nach Deploy

```bash
grep -c ENTER_SCALE_IN /path/to/momentum.log
grep ORPHANED BUY RECOVERED momentum.log | tail -5
grep "partial confirmed SELL" execution-engine.log
curl -s localhost:9898/metrics | egrep 'open_positions|position_authority'
curl -s localhost:9897/metrics | egrep 'open_positions|momentum_orphan|momentum_scale_in_gate'
```

Erwartung: `ENTER_SCALE_IN > 0` bei guten Probes; Momentum/EE `open_positions` naeher; keine Partial-SELL mit `sold_raw << total_pos` ohne Follow-up-Exit.
