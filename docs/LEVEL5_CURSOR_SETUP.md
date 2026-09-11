# Level-5 Cursor Setup

Zwei getrennte Fenster, damit Implementation und Test Authority sich nicht in die Quere kommen.

Sibling-Layout (Ordnernamen genau so, Groß/Kleinschreibung beachten):

```
Trading_bot/
├── Iron_crab/
└── Iron_crab-eval/
```

Arbeits-Branches: Iron_crab → `architecture-rebuild` (gemeinsam), Iron_crab-eval → `main`. `architecture-rebuild-next` ist Maintainer-Entwicklung.

## Zwei Fenster

### Fenster 1 – Implementation (Iron_crab)

Ordner öffnen: `.../Trading_bot/Iron_crab`

- Rolle: Implementation Agent
- Sieht: Code, CI Pass/Fail, `docs/INVARIANTS.md`
- Sieht nicht: Eval-Test-Code (`Iron_crab-eval/tests/`)

### Fenster 2 – Test Authority (Iron_crab-eval)

Ordner öffnen: `.../Trading_bot/Iron_crab-eval`

- Rolle: Test Authority
- Sieht: Spec (`docs/spec/`), Tests, öffentliche API via `ironcrab`
- Aufgabe: Blackbox-Szenarien und Invarianten aus der Spec schreiben

## Prüfen

```bash
cd .../Trading_bot/Iron_crab-eval
cargo test
```

Falls der Build die crate `ironcrab` nicht findet: Sibling-Layout prüfen und Path-Patch aus [CONTRIBUTING.md](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/CONTRIBUTING.md) setzen — Patch nicht committen.

## Multi-Root Workspace (optional)

Beide Repos in einem Fenster: `File → Add Folder to Workspace` → `Iron_crab-eval` hinzufügen.

`.cursorignore` in Iron_crab blendet `ironcrab-eval/` und `Iron_crab-eval/` aus, falls sie als Unterordner existieren. Bei Sibling-Layout im Multi-Root sind beide Roots sichtbar — Fenster 1 nur mit Iron_crab öffnen, wenn der Implementation Agent keine Spec/Tests sehen soll.

---

## Workflow

### Wann welcher Agent?

| Situation | Fenster | Agent |
|-----------|---------|-------|
| Code ändern, Bug fixen, Feature bauen | 1 (Iron_crab) | Implementation |
| Neue Tests aus Spec, Spec pflegen | 2 (Iron_crab-eval) | Test Authority |

### Tasks für den Implementation Agent

Spec-Kontext explizit mitschicken, wenn relevant. STOP-CHECK in `AGENTS.md` zuerst.

```
Implementiere [FEATURE]. Kontext aus Spec (Iron_crab-eval/docs/spec/): [Ausschnitt].
Erlaubte Dateien: [z.B. src/execution/]. Keine Eval-Testdateien lesen.
```

### Iteration

1. Impl-Änderung → PR auf `architecture-rebuild`
2. CI: Unit-Tests + Job **Eval (Level 5)**
3. Bei Fail: Fix im Impl-Repo (nicht die Tests an die Impl anpassen, außer die öffentliche API hat sich bewusst geändert — dann Eval-PR auf `main`)
4. Eval-PRs separat; schlankes Gate „Rust“, volle Suite über Impl-CI oder manuellen Workflow
