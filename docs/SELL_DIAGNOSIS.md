# Diagnose: Warum werden Positionen nicht verkauft?

## Server-Checks (ohne Code-Änderung)

### 1. Momentum-Bot Logs prüfen

```bash
# Letzte 500 Zeilen mit EXIT/TIME_EXIT/Sell/position
journalctl -u momentum-bot -n 500 --no-pager | grep -E "EXIT|TIME_EXIT|Sell|position|Failed|Multi-pool|No pools"

# Oder live mitverfolgen
journalctl -u momentum-bot -f | grep -E "EXIT|🚨|reconcile|Failed|No pools"
```

**Was du siehst, wenn Exits funktionieren:**
- `🚨 EXIT SIGNAL DETECTED` mit mint, exit_type, reason
- `♻️ Retrying timed exit` (aus reconcile)
- `EXIT SIGNAL DETECTED` + `exit_type=TIME_EXIT`

**Vermutete Fehler:**
- `No pools known for mint` → mint_pools hat keine Einträge für diesen Token
- `No pools with recent trade data and accounts available` → 5-Min-Filter: Pools als „stale“ gewertet
- `Multi-pool routing failed, using original pool` → Fallback greift, aber ggf. fehlen dex_pool_accounts
- `Failed to generate/publish sell intent` → Ursache steht im Fehlertext

---

### 2. Execution-Engine: Rejected Intents

```bash
journalctl -u execution-engine -n 300 --no-pager | grep -E "rejected|Reject|REJECTED|capital_lock|sell_token"
```

Mögliche Reject-Gründe:
- `LOCK_CAPITAL_CONFLICT` – Kapital gesperrt
- `sell_token_balance` – Balance-Check fehlgeschlagen
- `SIM_INSUFFICIENT_BALANCE` – Simulation zeigt 0 Balance

---

### 3. Heartbeat & Positions

```bash
journalctl -u momentum-bot -n 100 --no-pager | grep -E "heartbeat|open_positions|pending_intents|exits_generated"
```

Erwartung:
- `open_positions` > 0
- `exits_generated` sollte steigen, wenn Sells ausgelöst werden
- `pending_intents` > 0, wenn Sells noch in der Queue sind

---

### 4. Entscheidungskontrolle (Decision Records)

```bash
# Heutige Decision Records mit Reject-Gründen
grep -h "rejected\|Reject" ~/Iron_crab/trade_logs/decisions/decision_records-$(date +%Y%m%d).jsonl 2>/dev/null | tail -20
```

---

## Bekannte Code-Ursachen

### A) Probe+Scale getrennt statt Gesamtposition (Aggregations-Bug)

**Symptom:** SELL-Intent verkauft nur Teil der Position (z.B. required=4.5B bei available=9.5B). Custom(11) oder Reject wegen Teileverkauf.

**Ursache:** Position sollte immer Probe+Scale als Gesamtposition haben. Beim Restart wurde bisher JetStream KV bevorzugt; KV kann veraltet sein (nur nach Probe gespeichert, bevor Scale-In verarbeitet war). JSONL (`execution_results`) summiert korrekt alle BUY-Fills, wurde aber nur als Fallback bei leerem KV genutzt.

**Fix (implementiert):** Beide Quellen werden geladen. JSONL ist maßgeblich für `token_amount` (summiert Probe+Scale). Bei Mints in beiden Quellen wird JSONL verwendet. Stale KV-Einträge werden korrigiert und zurückgeschrieben.

---

### B) Custom(11) / Token-2022 CloseAccount bei Teileverkäufen

**Symptom:** Simulation schlägt fehl mit `UiTransactionError(InstructionError(2, Custom(11)))`. Log:
`Program log: Error: Non-native account can only be closed if its balance is zero`.

**Ursache:** Das `close_token_ata`-Flag triggert eine CloseAccount-Anweisung nach dem Sell. CloseAccount schlägt fehl, wenn die ATA noch Token-Restbestand hat (Teileverkauf). Custom(11) = SPL Token `NonNativeHasBalance`.

**Fix (implementiert):** CloseAccount nur wenn `sell_balance_hint` bestätigt `available == required` (Vollverkauf).

---

### C) 5-Minuten-Filter in `find_best_sell_pool`

Pools werden verworfen, wenn `last_updated` älter als 5 Minuten ist:

```rust
// momentum_bot.rs ~Z.2042
let max_age = std::time::Duration::from_secs(300);
// ...
now.duration_since(p.last_updated) < max_age
```

Wenn der Token nach dem Kauf inaktiv ist (keine neuen Trades), ist `last_updated` nach `max_hold_time` älter als 5 Minuten → **kein gültiger Pool** → `find_best_sell_pool` schlägt fehl.

**Fix:** Für TIME_EXIT / reconcile den Freshness-Filter lockern oder deaktivieren, wenn `hold_secs >= max_hold_time`.

---

### D) Leere `mint_pools`

Falls `mint_pools` für den Mint leer ist:
- `find_best_sell_pool` bricht mit `No pools known for mint` ab.
- Es greift der Fallback mit `original_pool`/`original_dex` aus der Position.

Wenn die Position `pool`/`dex` leer hat (z.B. Orphaned Buy Recovery ohne Tracker), wird das übersprungen:

```rust
// collect_timed_exit_reconcile_candidates ~Z.3038
if !pos.pool.is_empty() && !pos.dex.is_empty() { ... } else { continue; }
```

---

### E) Fehlende `dex_pool_accounts`

Wenn Pool-Registrierung ohne `dex_pool_accounts` erfolgte (z.B. nur über Trades):
- `valid.is_empty()` wegen `p.dex_pool_accounts.is_some()`
- oder Fallback mit leeren Accounts, EE muss aus LivePoolCache auflösen

---

## Empfohlene Reihenfolge auf dem Server

1. Nach `No pools`, `Failed to generate`, `Multi-pool routing failed` in momentum-bot-Logs suchen.
2. Rejected-Intents in execution-engine-Logs prüfen.
3. Bei Bestätigung von C) den 5-Minuten-Filter für TIME_EXIT anpassen.
