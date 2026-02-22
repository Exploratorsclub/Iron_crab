# Analyse: Keine Executed-Intents + WSOL-Manager erst nach Trade

## 1. Warum keine Intents als "executed" obwohl TX on-chain (Slippage) gescheitert?

### Root Cause

`INTENTS_EXECUTED_TOTAL` wird **nur** bei `DecisionOutcome::Confirmed` erhöht (execution_engine.rs ~7481):

```rust
if matches!(decision.outcome, DecisionOutcome::Confirmed) {
    INTENTS_EXECUTED_TOTAL.fetch_add(1, Ordering::Relaxed);
    ...
}
```

Bei **Slippage** (TX gesendet, on-chain bestätigt, aber fehlgeschlagen) gilt `DecisionOutcome::FailedConfirmed`. Diese TXs wurden tatsächlich ausgeführt (Sent + Confirmed als fehlgeschlagen), zählen aber nicht als "executed".

### Semantik

| Outcome           | Bedeutung                              | Aktuell als executed gezählt? |
|-------------------|----------------------------------------|--------------------------------|
| Confirmed         | TX on-chain, erfolgreich                | ✅ Ja                           |
| FailedConfirmed   | TX on-chain, fehlgeschlagen (Slippage) | ❌ Nein                        |
| Sent              | TX gesendet, noch keine Bestätigung    | ❌ Nein                        |
| Rejected/SimFailed | Nicht gesendet                         | ❌ Nein                        |

**Fix:** `FailedConfirmed` ebenfalls als "executed" zählen — der Intent wurde on-chain ausgeführt, auch wenn das Ergebnis ein Fehler war.

---

## 2. Warum wickelt WsolManager erst nach dem Trade statt direkt nach Killswitch-Reset?

### Root Cause

WsolManager läuft **event-getrieben** und führt `check_and_act()` nur aus, wenn eine **WalletBalanceUpdate**-NATS-Nachricht empfangen wird:

```
market-data (Geyser) → WalletBalanceUpdate (NATS) → WsolManager.handle_balance_update() → check_and_act()
```

**WalletBalanceUpdate** wird publiziert von:
1. **market-data** – bei Geyser-Token-Account-Updates (Wallet-Trades)
2. **execution-engine** – einmalig beim Startup nach JetStream-Bootstrap
3. **execution-engine** – nicht bei Killswitch-Reset

### Problem bei Killswitch-Reset

- Während Killswitch aktiv: WsolManager überspringt Wrap (`is_kill_switch_active() → return`)
- Nach Reset: Killswitch ist inaktiv, aber **keine neue WalletBalanceUpdate** wird gesendet
- WsolManager wartet weiter auf die nächste NATS-Nachricht
- Die nächste Nachricht kommt erst, wenn ein Trade passiert und Geyser ein Balance-Update liefert
- Bis dahin: kein Wrap → erste Trade-Chance ohne WSOL → Slippage/Fail

### Fix

Beim **ResetKillSwitch** einen synthetischen **WalletBalanceUpdate** mit den aktuellen LockManager-Werten (sol, wsol) an `wallet_balance_topic` publizieren. WsolManager erhält die Nachricht, aktualisiert seine Caches und führt `check_and_act()` aus. Da der Killswitch jetzt inaktiv ist, wird bei Bedarf gewickelt.

---

## 3. Zusammenfassung der Fixes

| Problem                               | Fix                                                                 |
|--------------------------------------|---------------------------------------------------------------------|
| Slippage-TX nicht als "executed"     | `FailedConfirmed` zusätzlich zu `Confirmed` bei INTENTS_EXECUTED_TOTAL zählen |
| WSOL-Wrap erst nach Trade            | Bei ResetKillSwitch WalletBalanceUpdate mit LockManager-Balances publizieren |
