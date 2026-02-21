# Analyse: WsolManager wrappt nicht + Intent-Rejects (2026-02-21)

## Problemstellung

Nach Deploy:
1. **Anzeige korrekt:** SOL+WSOL und WSOL=0 werden richtig angezeigt
2. **WsolManager wrappt nicht:** Obwohl WSOL=0 und native SOL ~3.3 verfügbar
3. **Intents rejected:** Momentum-Bot Intents werden abgelehnt

---

## Teil 1: Warum WsolManager nicht wrappt

### Root Cause

Der **WsolManager** erhält seine Balance-Daten **ausschließlich** über **NATS Core** (fire-and-forget) von `market-data`:
- `market-data` → `WalletBalanceUpdate` → `wallet_balance_topic(wallet)` → WsolManager
- Kein JetStream, keine RPC-Fallback

Der **LockManager** (für die Anzeige) erhält seine Daten aus **zwei** Quellen:
1. **JetStream** (persistent): `bootstrap_token_balances_from_wallet_snapshot` beim Start
2. **NATS Core**: WalletBalanceUpdate (Balance-Updater Listener)

### Das Problem

- Nach Neustart/Deploy: `market-data` sendet den initialen `WalletBalanceUpdate` beim **eigenen** Bootstrap
- Wenn **execution-engine nach market-data** startet: WsolManager subscribed – aber der Bootstrap-Update wurde **bereits gesendet** und ist verpasst
- NATS Core ist ephemeral: keine Nachrichten-Persistenz, kein Replay
- LockManager bekommt die Werte aus **JetStream** (persistent) → Anzeige korrekt
- WsolManager wartet auf eine WalletBalanceUpdate-Nachricht die **nie kommt**, bis eine Wallet-Transaktion einen Geyser-Update auslöst

### Evidenz

- Heartbeat: `available_wsol=0 native_sol=3319433071` (LockManager-Werte) – korrekt
- Keine WsolManager-Logs ("Balance update received", "WSOL below minimum, wrapping", "Not enough SOL")
- WsolManager hat `sol_balance=0, wsol_balance=0` (Initial) → "Not enough SOL to wrap" wird nie erreicht, weil `check_and_act` nur bei eingehenden Nachrichten aufgerufen wird – und es kommen keine

### Fix

Die execution-engine seedet den WsolManager nach dem JetStream-Bootstrap:
- **Nach** dem Spawn des WsolManager-Tasks: kurze Verzögerung (damit WsolManager subscribed hat)
- **Publish** eines synthetischen `WalletBalanceUpdate` an `wallet_balance_topic` mit den gebootstrapten Werten aus `lock_manager`
- WsolManager erhält die Nachricht, aktualisiert Caches, ruft `check_and_act` auf → Wrap wird ausgelöst

---

## Teil 2: Warum Intents rejected wurden

### Reject-Gründe (aus decision_records)

| Run/Intent      | primary_reject_reason     | Details                                              |
|-----------------|---------------------------|------------------------------------------------------|
| int-b9d8e7a8-*  | KILL_SWITCH_ACTIVE        | kill_switch_active: buy blocked                      |
| int-828b2d16-*  | KILL_SWITCH_ACTIVE        | kill_switch_active: buy blocked                      |
| int-b3f6f068-*  | KILL_SWITCH_ACTIVE        | kill_switch_active: buy blocked                      |

### Erklärung

Die **aktuellen** (nach Deploy) Intents wurden **nicht wegen WSOL** abgelehnt, sondern weil der **Kill Switch aktiv** ist:
- `kill_switch_active: buy blocked`
- Vermutlich manuell aktiviert (z.B. nach Liquidation/Stop)
- Die Intents waren `ENTER_PROBE_BUY` – Momentum-Bot wollte kaufen, aber der Kill Switch blockt neue BUYs

### Weitere Reject-Gründe (historisch in den Logs)

- **SIM_FAILED / Custom(11)**: Teileverkauf mit CloseAccount – "Non-native account can only be closed if its balance is zero" (bekannt aus SELL_DIAGNOSIS)
- **LOCK_RESOURCE_CONFLICT**: Parallel-Intents auf gleichen Mint
- **RISK_MAX_OPEN_POSITIONS**: Max. offene Positionen erreicht

---

## Zusammenfassung

| Thema              | Ursache                                                                 | Fix / Aktion                                         |
|--------------------|-------------------------------------------------------------------------|------------------------------------------------------|
| WsolManager        | Kein Bootstrap aus JetStream; verpasst ephemeralen NATS-Update         | Publish WalletBalanceUpdate nach JetStream-Bootstrap|
| Intent-Rejects     | Kill Switch aktiv (nicht WSOL-Mangel)                                  | Kill Switch deaktivieren für neue Buys               |

---

## Fix-Implementierung

1. In `execution_engine.rs`: Nach WsolManager-Spawn einen `WalletBalanceUpdate` mit LockManager-Bootstrap-Werten publishen
2. Optional: systemd `After=market-data.service` für execution-engine (Startreihenfolge)
