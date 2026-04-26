# Handoff: Scope 38 - market-data Startup watchdog-safe machen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in AGENTS.md und .cursor/rules/ironcrab-core.mdc BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Fixe einen konkreten Produktionsfehler: `market-data` laeuft nach Deploy in einen **systemd-Watchdog-Crashloop**.

Aktueller Befund aus dem Serverlauf:
- `market-data.service` ist wiederholt in `Result: watchdog` / `signal=ABRT` gelaufen.
- Der Restart-Rhythmus liegt bei ca. 30-35s und passt exakt zu `WatchdogSec=30`.
- Kurz vor jedem Restart loggt `market-data` erneut den Wallet-Bootstrap fuer dieselbe Wallet und denselben PumpSwap-Mint.
- Der blockierende Pfad ist sehr wahrscheinlich der startup-kritische Wallet-Bootstrap, insbesondere:
  - `publish_wallet_snapshot(...)`
  - darin `run_bounded_wallet_dex_bootstrap_verify(...)`
  - darin serielle `await`-Aufrufe auf Cold-Path-`Ensure...`-Handler wie `handle_ensure_pump_amm_pool_accounts(...)`
- Fuer den betroffenen Mint `E7UaWyQoDgvUTvgQLxbR3oVyYpf3eq2hN95RzrwQpump` sieht man wiederholt:
  - `EnsurePumpAmmPoolAccounts start`
  - `pump_amm: LivePoolCache miss for pool discovery, falling back to RPC`
- Die regulaeren Watchdog-Pings passieren erst spaeter in der Main-Loop / Activity-Interval-Branches. Wenn der Startup-Pfad davor >30s blockiert, schiesst systemd den Prozess ab.

Ziel dieses Scopes:
- `market-data` muss nach Deploy **stabil aktiv bleiben**.
- Der Startup-Pfad muss **watchdog-safe** werden.
- Die Wallet-Bootstrap-/Cold-Path-Verifikation darf **nicht** den Watchdog ausloesen.
- Es soll **kein** Scope fuer Liquidation-/PumpSwap-6013-Fix werden.
- Es soll **kein** Scope werden, der einfach nur `WatchdogSec` hochsetzt.

## Relevante Invarianten (Volltext)

### I-7 Hot Path RPC-Freiheit
Im normalen Trading-Hot-Path sind keine neuen RPC-Calls erlaubt. RPC darf nur im Cold Path stattfinden und dort nur auf der dafuer vorgesehenen Seite (`market-data`), nicht als lokaler Shortcut in `execution-engine`.

### I-4 Geyser-First
Bestehende Geyser-/JetStream-First-Muster duerfen nicht durch direkte lokale RPC- oder cache-bypass Logik im `execution-engine` ersetzt werden. Autoritativer State kommt weiter aus MASTER -> JetStream -> SLAVE.

### I-9 Simulation-Gate
Es duerfen keine Transaktionen gesendet werden, die die Simulation nicht erfolgreich passiert haben. Ein Recovery-/Discovery-Schritt darf nur dazu dienen, anschliessend erneut sauber zu planen/simulieren, nicht die Simulation zu umgehen.

### I-12 Decision Record / Beobachtbarkeit
Ein Fehlerpfad darf nicht still verschwinden. Wenn die bounded Wallet-Bootstrap-Verifikation verschoben, entkoppelt oder watchdog-safe gemacht wird, muss der Ablauf weiterhin beobachtbar bleiben: keine stille Deaktivierung ohne Logs, kein Hang, kein stilles Drop.

### Keyless / Rollen-Trennung
`market-data` bleibt keyless. Dieser Scope darf keinerlei Key-Loading, Signing oder execution-engine-seitige Workarounds einfuehren.

## Bestehendes Pattern

Nutze das bestehende Architektur-Muster:
- Lange oder potenziell blockierende Cold-Path-Arbeit darf nicht den zentralen Liveness-/Service-Loop abwuergen.
- Der systemd-Watchdog wird im regulaeren Runtime-Loop ueber `sd_notify(... Watchdog)` bedient; startup-kritische Arbeiten muessen deshalb entweder:
  - ausreichend frueh watchdog-safe gestaltet werden, oder
  - aus dem startup-kritischen, seriell blockierenden Pfad herausgezogen werden.

Konkret relevante Stellen:
- `src/bin/market_data.rs`
  - `publish_wallet_snapshot(...)`
  - `run_bounded_wallet_dex_bootstrap_verify(...)`
  - Main/Loop mit `sd_notify(Ready)` und spaeteren Watchdog-Pings

Wichtige Bug-Pattern / Kontext:
- `KNOWN_BUG_PATTERNS.md` #32: PumpSwap Discovery kann bei bestimmten Fallbacks teuer/problematisch werden; keine unbounded oder falschen RPC-Fallbacks reaktivieren.
- `KNOWN_BUG_PATTERNS.md` #34: Cold-Path-Recovery darf nicht versehentlich denselben stale Cache-State recyceln.
- `KNOWN_BUG_PATTERNS.md` #23: Wallet-/SOL-/WSOL-Logik ist empfindlich; dieser Scope soll NICHT nebenbei die Dashboard-Metriken refactoren.

## Erwartete Aenderung

Schneide den kleinstmoeglichen Impl-Scope, der Folgendes erreicht:

1. Mache den `market-data`-Startup-Pfad watchdog-safe.
2. Der Wallet-Bootstrap darf weiterhin:
   - WalletBalanceSnapshots publizieren
   - bounded Cold-Path-Verifikation fuer Wallet-Mints anstossen
3. Aber diese bounded DEX-Verifikation darf den Prozess nicht mehr so lange seriell blockieren, dass `WatchdogSec=30` ausloest.
4. Bevorzuge eine **strukturelle Entkopplung** des langen Bootstrap-Verify-Pfads vom startup-kritischen Liveness-Pfad.
5. Wenn du statt Entkopplung einen kleineren watchdog-safe Mechanismus im selben Pfad waehlst, muss die Begruendung klar sein und der Scope klein bleiben.
6. Nicht erlaubt ist ein "Fix" nur durch:
   - systemd-Unit aendern / `WatchdogSec` hochsetzen
   - bounded Verify komplett entfernen
   - PumpSwap-/Liquidation-/6013-Fix in denselben Scope ziehen

## Akzeptanzkriterien

- `market-data` bleibt nach Start / Deploy stabil aktiv und faellt nicht mehr in einen Watchdog-Restart-Loop.
- Der startup-kritische Pfad blockiert den Watchdog nicht mehr.
- Die bounded Wallet-DEX-Verifikation bleibt funktional vorhanden.
- Keine neuen Hot-Path-RPCs.
- Keine Aenderungen an `execution-engine`.
- Keine systemd-Unit-Aenderungen als primaerer Fix.

## Erlaubte Dateien

- `Iron_crab/src/bin/market_data.rs`
- kleine zugehoerige Tests im selben File oder bestehende rust-Tests, **nur wenn wirklich noetig**

## Verboten

- Keine Aenderungen an `execution_engine.rs`
- Keine Aenderungen im Eval-Repo
- Keine systemd-Service-Dateien aendern
- Kein "Workaround" nur durch Erhoehung von `WatchdogSec`
- Keine Aenderungen am Liquidation-/PumpSwap-6013-Pfad in diesem Scope
- Keine neuen globalen `getProgramAccounts`-Scans
- Keine neuen Hot-Path-RPCs

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo test --features test_helpers --quiet
```

## Erwarteter Abschlussbericht

Bitte nenne am Ende kurz:
- welche STOP-CHECKs geprueft wurden
- welche Dateien geaendert wurden
- wie der Startup-Pfad watchdog-safe gemacht wurde
- warum der Fix die bounded Wallet-DEX-Verifikation beibehalt
- welche Tests / Checks gelaufen sind
