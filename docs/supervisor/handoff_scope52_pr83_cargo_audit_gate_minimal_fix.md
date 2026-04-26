# Handoff: Scope 52 - PR #83 cargo-audit Gate minimal schliessen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

PR `#83` ist fachlich fuer den eingegrenzten PumpSwap-Recovery-Scope aktuell ok, aber **nicht mergebar**, weil die GitHub-CI auf dem letzten Commit `fe1dc9144040156ed114dfff5591c6b56746aea2` weiterhin bei `Security audit (cargo-audit)` rot ist.

Dieser Scope ist **kein** weiterer PumpSwap-Feature-/Recovery-Scope. Die funktionale Aenderung aus Scope 51 soll **nicht** erneut ausgedehnt oder umgebaut werden.

Ziel dieses Scopes:

1. Das rote `cargo-audit`-Gate auf PR `#83` mit einer **minimalen, risikoarmen** Aenderung gruenn bekommen.
2. Falls das **nicht** mit einem kleinen, vertretbaren Dependency-/Lockfile-Fix moeglich ist, dann **keine** breitflächige oder spekulative Runtime-Aenderung machen, sondern sauber stoppen und begruenden.
3. Den bestehenden PumpSwap-Scope funktional unveraendert lassen.

## Harte Evidenz

GitHub-Run auf dem aktuellen PR-Head:

- Workflow: `CI`
- Run: `24635858717`
- Head: `fe1dc9144040156ed114dfff5591c6b56746aea2`

Aktueller Status:

- `build-test`: gruen
- `Python backend (pyo3)`: gruen
- `Eval (Level 5)`: gruen
- `Security audit (cargo-audit)`: rot

Aus dem Audit-Log:

- `RUSTSEC-2026-0098` (`rustls-webpki`)
- `RUSTSEC-2026-0099` (`rustls-webpki`)
- `RUSTSEC-2025-0161` (`libsecp256k1` unmaintained)
- `RUSTSEC-2026-0097` (`rand`)

Wichtig:

- Die PR-Diff fuer Scope 51 aendert **nur**
  - `src/execution/live_pool_cache.rs`
  - `src/execution/tx_builder.rs`
- Es wurden in diesem Scope bisher **keine** Dependency-Dateien geaendert.
- Die letzten erfolgreichen `architecture-rebuild`-CI-Runs liegen vor Auftreten dieser neuen Advisories; deshalb ist ein rotes Audit hier derzeit ein **Gate-Blocker**, auch wenn es vermutlich nicht vom PumpSwap-Code selbst verursacht wurde.

## Relevante Invarianten (Volltext)

### I-4 Geyser-First
HOT PATH (Discovery, Buy, Sell, Monitoring): GEYSER-ONLY. Keine neuen blockierenden RPC-Calls.

### I-5 Cold Path
COLD PATH (Liquidation, Manual Actions, Bootstrap): RPC erlaubt. Safety und correctness vor Speed. Autoritativer On-Chain-State darf hier nachgeladen werden.

### I-7 Hot-Path RPC-Freiheit
Nie RPC im normalen Trading-Hot-Path ohne explizite Freigabe. Dieser Scope darf keinerlei neue Runtime-RPC-Pfade einfuehren.

### I-9 Simulation-Gate
Wenn Simulation fehlschlaegt, darf keine Transaktion gesendet werden. Dieser Scope darf keinen Simulations-Bypass einfuehren.

### I-12 Decision Record
Wenn ein Intent verworfen oder abgebrochen wird, darf das nicht still passieren. Keine Aenderung an Failure-/Decision-Semantik ohne zwingenden Grund.

### I-24d Cold-Path Discovery nur per Request/Reply
`execution-engine` darf fehlende oder unbrauchbare PumpSwap-Accounts im Cold Path weder selbst discovern noch lokal als Truth in den SLAVE Cache schreiben. Discovery, MASTER-Write und JetStream-Publikation bleiben bei `market-data`.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Symptomfix ohne harte Evidenz. Keine spekulative Scope-Erweiterung.
- `KNOWN_BUG_PATTERNS.md` #34
  - Den funktionalen Scope 51 nicht wieder aufmachen oder semantisch veraendern.
- `KNOWN_BUG_PATTERNS.md` #36
  - "Vorhanden" oder "wirkt irgendwie ready" reicht nicht. Keine unscharfen Heuristiken oder Label.

## Bestehendes Pattern

Fuer diesen Scope gilt das Muster:

1. **Minimaler Gate-Fix zuerst**
   - Nur genau so viel aendern wie noetig, damit `cargo-audit` auf dem PR-Head gruen wird.
2. **Keine breite Vendor-/Runtime-Lawine**
   - Keine grossflaechige Aufnahme tausender Vendor-Dateien.
   - Keine opportunistischen Dependency-Upgrades ohne klaren Bezug zum Audit-Fail.
3. **Wenn klein nicht moeglich, sauber stoppen**
   - Wenn ein gruener Audit-Stand nur ueber breiten, riskanten Stack-Umbau erreichbar waere, dann abbrechen und exakt benennen, welche transitive Kette blockiert.

## Erwartete Arbeitsreihenfolge

### A. Exakt beweisen, woher die Advisories kommen

Bitte zuerst am aktuellen GitHub-Stand belegen:

1. welche direkte/transitive Dependency-Kette die vier Audit-Findings zieht
2. ob ein **kleiner** Lockfile-/Patch-/Version-Fix moeglich ist
3. ob `rustls-webpki`, `rand` und `libsecp256k1` ueber denselben oder verschiedene Pfade kommen

### B. Nur minimalen Audit-Fix machen

Erlaubt sind nur kleine, klar begrenzte Wege wie z. B.:

- `Cargo.lock`-Update
- kleine `Cargo.toml`-Patch-/Version-Anpassung
- kleine, gezielte Dependency-Ersetzung

Nicht erlaubt:

- breites Vendoring
- grosse Solana-/Tokio-/TLS-Stack-Upgrades ohne Nachweis, dass das der kleinste sichere Fix ist
- Mitziehen unzusammenhaengender Runtime-Refactors
- erneute Aenderung am Scope-51-PumpSwap-Code, ausser falls ein kleiner Begleitfix durch den Dependency-Fix technisch zwingend waere

### C. Wenn kein kleiner Fix moeglich ist: STOP mit Beweis

Falls ein minimaler Fix nicht moeglich ist, dann:

1. keine grossen Dateien oder riskanten Dependency-Blobs committen
2. stattdessen klar auflisten:
   - welche Root-Dependencies die Advisories hereinziehen
   - warum ein kleiner Fix scheitert
   - welche breitere Folgeaenderung noetig waere
   - warum das den Scope von PR `#83` unverhaeltnismaessig vergroessern wuerde

## Akzeptanzkriterien

- `Security audit (cargo-audit)` wird auf PR `#83` gruen
  - **oder**
- es gibt einen belastbaren Stop-Bericht, warum das auf diesem PR nicht minimal loesbar ist

Und zusaetzlich:

- kein funktionaler Umbau des PumpSwap-Recovery-Scopes
- keine neuen Runtime-RPC-Pfade
- kein Simulations-Bypass
- keine grossflaechige Vendor-/Dependency-Lawine ohne zwingenden Beleg

## Erlaubte Dateien

- `Cargo.toml`
- `Cargo.lock`
- direkt zugehoerige kleine Cargo-/Audit-Konfigurationsdateien, falls bereits im Repo etabliert
- nur im Ausnahmefall eine sehr kleine Anzahl weiterer Dateien, wenn technisch zwingend fuer einen minimalen Dependency-Fix

## Verboten

- keine Aenderungen in `src/` fuer neue Funktionalitaet
- kein erneuter PumpSwap-Logikumbau
- kein grossflaechiges Vendoring
- keine breiten Framework-/Runtime-Upgrades ohne expliziten Nachweis, dass das der kleinste sichere Fix ist
- keine Secrets / Keys

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo audit
```

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche STOP-CHECKs geprueft wurden
- welche direkte/transitive Dependency-Ketten die Advisories verursachen
- ob ein minimaler Fix moeglich war
- welche Dateien geaendert wurden
- welche Checks lokal/CI gelaufen sind
- falls kein Fix gemacht wurde: exakte Stop-Begruendung mit naechstkleiner sinnvoller Folgescope-Empfehlung
