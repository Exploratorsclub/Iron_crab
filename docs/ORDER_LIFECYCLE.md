# Order & Position Lifecycle

**Stand:** 2026-08-22

State Machines für Intent, Position und Execution. Schema: `src/ipc/schema.rs`.

`DecisionOutcome` im Code: `Rejected`, `Expired`, `SimFailed`, `Sent`, `Confirmed`, `FailedConfirmed`. Ein Intent endet in genau einem Outcome.

Zusätzlich zum JSONL-Pfad: JetStream `TRADE_INTENTS` / `EXECUTION_RESULTS`. Offene Positionen: KV `POSITION_AUTHORITY` ist die Daten-SSOT, `position-manager` der einzige Writer; Momentum-Overlay ist Strategiezustand. Preis-Updates nur bei `source_pool == position.pool` (I-13).

**Quellen:** Eval `TARGET_ARCHITECTURE.md`, `MOMENTUM_V2_SPEC.md`, `DEFINITION_OF_DONE.md` (historisch), `src/ipc/schema.rs`

---

## 1. TradeIntent Lifecycle (Execution Engine)

Jeder Intent durchläuft die Pipeline **einmal** und endet in **genau einem** `DecisionOutcome`.

```
Intent Received
      │
      ▼
┌─────────────────┐
│ Idempotency     │── duplicate ──► Rejected (LockDuplicateIntent)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ TTL/Deadline    │── expired ────► Expired
└────────┬────────┘
         │
         ▼
┌─────────────────┐     BUY only
│ KillSwitch      │── active ──────► Rejected (KillSwitchActive)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Risk Checks     │── fail ────────► Rejected (RiskMaxPosition, etc.)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Capital/Resource│── conflict ───► Rejected (LockCapitalConflict, etc.)
│ Locks           │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Plan + Simulate │── sim fail ───► SimFailed
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Send (optional)│── send_enabled=false ──► Rejected (send_disabled)
└────────┬────────┘
         │
         ├── send fail ──► Rejected (SendFailed, BundleFailed)
         │
         ▼
    Sent ──► (async) Confirm
                 │
                 ├── success ──► Confirmed
                 └── fail ─────► FailedConfirmed
```

### DecisionOutcome (terminal)

| Outcome | Bedeutung |
|---------|-----------|
| `Rejected` | Abgelehnt vor Send (KillSwitch, Risk, Lock, send_disabled) |
| `Expired` | TTL/Deadline überschritten |
| `SimFailed` | Simulation fehlgeschlagen |
| `Sent` | TX gesendet, Bestätigung ausstehend |
| `Confirmed` | TX on-chain bestätigt, erfolgreich |
| `FailedConfirmed` | TX on-chain bestätigt, aber fehlgeschlagen (z.B. Slippage) |

---

## 2. ExecutionResult Lifecycle

Nach dem Send schreibt die Execution Engine `ExecutionResult` (NATS + JSONL).

```
ExecutionStatus:
  Sent        → TX submitted, awaiting confirmation
  Confirmed   → TX on-chain, success
  Failed      → TX on-chain, reverted/failed
```

- **BUY Confirmed**: `fill_in`, `fill_out` → Position `entry_price`, `sol_invested`
- **SELL Confirmed**: `fill_out` → realized PnL; Position wird geschlossen

---

## 3. Momentum TrackerState (Token/Mint)

Jeder Mint hat einen `TrackerState`. **Nicht** jede Position hat einen Tracker — Tracker können in `Rejected` enden ohne Position.

```
Discovery ──► Validation ──► ProbeBuyPending ──► PositionOpenProbe
                  │                  │                    │
                  │                  │                    ├──► ScaleInPending ──► PositionOpenFull
                  │                  │                    │
                  │                  │                    └──► (window expire) ──► PositionOpenFull (probe only)
                  │                  │
                  └── Rejected       └── Rejected (exec fail/timeout)
```

| State | Bedeutung |
|-------|-----------|
| `Discovery` | Token beobachtet, erste Trades gesammelt |
| `Validation` | Filter-Check, wartet auf Velocity/Quality |
| `ProbeBuyPending` | Probe-BUY Intent gesendet, wartet auf ExecutionResult |
| `PositionOpenProbe` | Nur Probe-Fill, ggf. Scale-In möglich |
| `ScaleInPending` | Scale-In BUY gesendet, wartet auf ExecutionResult |
| `PositionOpenFull` | Volle Position (Probe + Scale oder nur Probe nach Window) |
| `Rejected` | Terminal, Token für TTL geblacklistet |

---

## 4. Position Lifecycle (Open → Close)

Position existiert **erst ab** bestätigtem BUY (ExecutionResult status=Confirmed).

```
(keine Position)
      │
      │  ExecutionResult BUY Confirmed
      │  handle_execution_result() → open_position()
      ▼
┌──────────────────┐
│ Position OPEN    │  entry_price, token_amount, sol_invested, pool, dex
│                  │  current_price ← Trade/PoolCacheUpdate (pool-matched!)
└────────┬─────────┘
         │
         │  Price Updates (nur von position.pool!)
         │  should_exit() prüft: STOP_LOSS, TAKE_PROFIT, Trailing, TIME_EXIT
         │
         ├── Exit-Signal (STOP_LOSS, TAKE_PROFIT, etc.)
         │   → SELL Intent published
         │   → exit_generated = true, exit_generated_at = now
         │
         │  ExecutionResult SELL Confirmed
         │  handle_execution_result() → close_position()
         ▼
┌──────────────────┐
│ Position CLOSED  │  removed from positions HashMap
└──────────────────┘
```

### Wichtige Felder

| Feld | Bedeutung |
|------|-----------|
| `entry_price` | tokens_per_sol bei Entry (aus fill_out/fill_in) |
| `current_price` | Aktueller tokens_per_sol (nur von position.pool!) |
| `exit_generated` | true = SELL-Intent bereits gesendet, kein erneuter Exit |
| `exit_generated_at` | Für Retry-Cooldown (reconcile_timed_exits) |

### Exit-Arten (exit_type)

| exit_type | Trigger |
|-----------|---------|
| `STOP_LOSS` | pnl ≤ -hard_stop_loss_pct |
| `TAKE_PROFIT` | pnl ≥ take_profit_pct, hold_secs ≥ take_profit_min_hold_secs |
| `TRAILING_STOP` | Drawdown von ATH erreicht, trailing aktiviert |
| `TIME_EXIT` | hold_secs ≥ max_hold_time_secs |
| `MOMENTUM_EXIT` | Buy ratio unter Schwellwert |
| `LP_REMOVAL` | LP entfernt nach Entry |
| `DEV_SELL` | Dev verkauft nach Entry |

---

## 5. PendingIntent (Momentum → Execution Korrelation)

```
momentum-bot publish BUY Intent
      │
      │  pending_intents.insert(intent_id, PendingIntent { mint, pool, dex, entry_kind: Probe|ScaleIn, ... })
      │
      ▼
Execution Engine: Sent → Confirmed
      │
      │  ExecutionResult via NATS
      ▼
momentum-bot handle_execution_result()
      │
      │  Finde PendingIntent by intent_id
      │  → open_position() oder add_investment() (ScaleIn)
      │  → pending_intents.remove(intent_id)
      ▼
Position aktualisiert
```

**Orphaned Buy:** ExecutionResult kommt an, aber kein PendingIntent (z.B. cleanup_stale_pending entfernte ihn). → Orphaned Buy Recovery: Position aus ExecutionResult + TokenTracker rekonstruieren.

---

## 6. Reconcile (Retry & Recovery)

- **reconcile_timed_exits()**: Alle 15s. Positionen mit `exit_generated=true` aber kein SELL-Confirm seit X Sekunden → erneuter SELL-Intent. `exit_generated` wird bei Failed/Timeout zurückgesetzt (FIX-19).
- **recover_positions_from_jsonl()**: Bootstrap. Liest execution_results JSONL, rekonstruiert offene Positionen für Restart.
- **reconcile_timed_exits** prüft auch `exit_generated=false` + `hold_secs >= max_hold_time` → TIME_EXIT (z.B. wenn Exit nie generiert wurde).

---

## 7. Invarianten (aus ORDER_LIFECYCLE)

- Ein Intent hat **genau ein** DecisionOutcome.
- Eine Position ist **entweder** open **oder** closed (kein Zwischenzustand).
- `exit_generated=true` ⇒ kein erneuter Exit-Signal bis Retry/Reconcile.
- Preis-Updates für Position nur wenn `source_pool == position.pool`.
- PendingIntent wird nach ExecutionResult-Handling entfernt (oder bei Stale-Cleanup).
