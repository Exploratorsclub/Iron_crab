# Handoff: Scope 53 - PR #83 cargo-audit Fix darf Eval nicht brechen

WICHTIG: Lies und befolge die STOP-CHECK Regeln in `AGENTS.md` und `.cursor/rules/ironcrab-core.mdc` BEVOR du eine Datei aenderst. Wenn eine geplante Aenderung gegen eine Regel verstoesst, STOPPE sofort und melde den Verstoss statt die Aenderung durchzufuehren.

## Task-Beschreibung

Dein letzter Follow-up-Commit `8163ede77cfcc15b207dfe050e4f165f86e5c693` hat `cargo-audit` erfolgreich gruen gemacht, aber den verpflichtenden `Eval (Level 5)`-Gate auf demselben PR gebrochen.

Dieser neue Scope ist **kein** neuer PumpSwap-/Execution-Scope. Die funktionale Scope-51-Aenderung bleibt fachlich eingegrenzt und soll **nicht** wieder angefasst werden, ausser wenn ein sehr kleiner Begleitfix technisch zwingend waere.

Ziel:

1. `cargo-audit` weiter gruen halten
2. **und gleichzeitig** `Eval (Level 5)` wieder gruen bekommen
3. ohne die Dependency-Aufloesung im Eval-Kontext kaputt zu machen

## Harte Evidenz

Aktueller PR-Head:

- `8163ede77cfcc15b207dfe050e4f165f86e5c693`

Checks auf diesem Stand:

- `Security audit (cargo-audit)`: gruen
- `build-test`: gruen
- `Python backend (pyo3)`: gruen
- `Eval (Level 5)`: **rot**

### Exakter Eval-Fehler

Aus dem GitHub-Log des fehlgeschlagenen `Eval (Level 5)`-Runs:

```text
error: failed to select a version for `rustls-webpki`.
  ... required by package `rustls v0.23.37`
  ... which satisfies dependency `rustls = "^0.23.4"` (locked to 0.23.37) of package `reqwest v0.12.28`
...
  previously selected package `rustls-webpki v0.103.12`
    ... which satisfies dependency `rustls-webpki = "^0.103.12"` of package `ironcrab v0.4.0`
    ... which satisfies git dependency `ironcrab` of package `ironcrab-eval`
failed to select a version for `rustls-webpki` which could resolve this conflict
```

Wichtig:

- Der Blocker ist **nicht** mehr `cargo-audit`.
- Der Blocker ist jetzt die neue **direkte** `rustls-webpki = "0.103.12"`-Erzwingung in `ironcrab`, die im Eval-Kontext mit der dortigen Aufloesung kollidiert.
- Der letzte Commit hat ausserdem `.cargo/audit.toml` und `.github/workflows/ci.yml` geaendert.

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
Keine stillen Semantikbrueche oder Failure-Bypasses.

### I-24d Cold-Path Discovery nur per Request/Reply
Keine neue lokale Discovery-/Truth-Logik in `execution-engine`.

## Relevante Bug-Patterns

- `KNOWN_BUG_PATTERNS.md` #19
  - Kein Symptomfix ohne harte Evidenz. Nicht erneut breit an Dependencies schrauben, ohne genau zu beweisen, warum.
- `KNOWN_BUG_PATTERNS.md` #34
  - Scope 51 funktional nicht wieder aufmachen.
- `KNOWN_BUG_PATTERNS.md` #36
  - Ein gruener Teil-Check reicht nicht; der Gesamtzustand muss wirklich `ready` sein.

## Erwartete Arbeitsreihenfolge

### A. Den letzten Audit-Fix eval-kompatibel machen

Bitte zuerst exakt pruefen:

1. Welche Aenderung war fuer den Eval-Resolver kausal?
   - direkte `rustls-webpki`-Dependency?
   - Lockfile?
   - Kombination aus beidem?
2. Welche **kleinste** Anpassung haelt `cargo-audit` gruen, ohne `Eval (Level 5)` zu brechen?

### B. Minimaler korrigierter Fix

Erlaubt sind nur kleine, klar begrenzte Wege wie z. B.:

- direkte `rustls-webpki`-Erzwingung wieder entfernen, wenn sie der eigentliche Resolver-Konflikt ist
- stattdessen Audit-Konfiguration / Ignore / CI-Tooling so anpassen, dass
  - `cargo-audit` weiter sinnvoll laeuft
  - `Eval (Level 5)` resolvbar bleibt
- kleine Lockfile-/Config-Anpassungen

### C. Nicht erlaubt

- kein erneuter funktionaler Umbau in `src/execution/*`
- keine grossen Dependency-/Vendor-Lawinen
- kein breiter TLS-/Reqwest-/Solana-Stack-Umbau ohne harten Nachweis
- keine neue Logik nur fuer CI-Workarounds, die den Runtime-Scope vergroessert

## Akzeptanzkriterien

- `Security audit (cargo-audit)` gruen
- `Eval (Level 5)` gruen
- `build-test` gruen
- `Python backend (pyo3)` gruen
- Scope-51-Funktionalitaet bleibt inhaltlich unveraendert

## Erlaubte Dateien

- `Cargo.toml`
- `Cargo.lock`
- `.cargo/audit.toml`
- `.github/workflows/ci.yml`
- nur im Ausnahmefall eine sehr kleine Anzahl direkt zusammenhaengender Dateien

## Verboten

- keine neuen `src/`-Features
- kein erneuter PumpSwap-Logikumbau
- kein grossflaechiges Vendoring
- keine Secrets / Keys

## Pruef-Befehle

```bash
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test --quiet
cargo audit --deny warnings
```

Und bitte explizit darauf achten, dass die GitHub-CI mit `Eval (Level 5)` auf dem PR-Stand wieder aufloesbar ist.

## Erwarteter Abschlussbericht

Bitte am Ende kurz nennen:

- welche Aenderung den Eval-Resolver-Konflikt verursacht hat
- wie du `cargo-audit` gruenn haeltst, ohne Eval zu brechen
- welche Dateien geaendert wurden
- welche Checks lokal/CI gelaufen sind
