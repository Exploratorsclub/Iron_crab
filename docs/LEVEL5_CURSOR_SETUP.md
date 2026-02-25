# Level-5 Cursor Setup

## Zwei Fenster

### Fenster 1 – Implementation (Iron_crab)
**Ordner öffnen:** `c:\Users\Robert Onuk\Desktop\Trading_bot\Iron_crab`

- Agent-Rolle: Implementation Agent
- Sieht: Code, CI Pass/Fail
- Sieht nicht: Spec-Details (liegen im Eval-Repo), Eval-Test-Code

### Fenster 2 – Test Authority (ironcrab-eval)
**Ordner öffnen:** `c:\Users\Robert Onuk\Desktop\Trading_bot\ironcrab-eval`

- Agent-Rolle: Test Authority
- Sieht: Spec (`docs/spec/`), Tests, API via ironcrab
- Aufgabe: Blackbox-Szenarien und Invarianten aus der Spec schreiben

**Voraussetzung:** `Iron_crab` muss als Geschwister vorhanden sein (gleicher Parent wie Iron_crab-eval).

## Prüfen

```bash
# Im ironcrab-eval Ordner:
cd c:\Users\Robert Onuk\Desktop\Trading_bot\ironcrab-eval
cargo test
```

Falls `cargo test` fehlschlägt (z.B. "path ../Iron_crab not found"): Prüfen, ob beide Ordner unter `Trading_bot` stehen:
```
Trading_bot/
├── Iron_crab/
└── ironcrab-eval/
```

## Multi-Root Workspace (optional)

Beide Repos in einem Fenster: `File → Add Folder to Workspace` → `ironcrab-eval` hinzufügen.

`.cursorignore` in Iron_crab blendet `ironcrab-eval/` und `Iron_crab-eval/` aus, falls sie als Unterordner existieren. Bei Sibling-Layout im Multi-Root sind beide Roots sichtbar – dann Fenster 1 nur mit Iron_crab öffnen, wenn der Implementation Agent keine Spec/Tests sehen soll.

---

## Workflow

### Wann welcher Agent?

| Situation | Fenster | Agent |
|-----------|---------|-------|
| Code ändern, Bug fixen, Feature bauen | 1 (Iron_crab) | Implementation |
| Neue Tests aus Spec, Spec pflegen | 2 (ironcrab-eval) | Test Authority |

### Tasks für den Implementation Agent

Spec-Kontext explizit mitschicken, wenn relevant:

```
Implementiere [FEATURE]. Kontext aus Spec (ironcrab-eval/docs/spec/): [Ausschnitt einfügen oder Verweis].
Erlaubte Dateien: [z.B. src/execution/]. Vermeide Architektur-Verstöße (INVARIANTS.md).
```

Bei reinen Code-Fixes reicht oft: „Fix X in [Modul], siehe INVARIANTS.md“.

### Iteration

1. Agent ändert Code → Push
2. CI läuft → Eval-Tests Pass/Fail
3. Bei Fail: Agent erneut mit Fehlermeldung (z.B. „Eval-Test Y schlägt fehl: …“)
4. Wiederholen bis Pass
