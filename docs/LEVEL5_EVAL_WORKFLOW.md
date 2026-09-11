# Level-5 Evaluator Workflow

Stand: 2026-08-22.

Der Level-5-Setup trennt Implementation (`Iron_crab`) und Evaluation (`Iron_crab-eval`) in separate Repos.

## Architektur

| Rolle | Workspace | Sichtbarkeit |
|-------|-----------|--------------|
| **Implementation Agent** | nur Iron_crab | Code, CI-Logs (Pass/Fail), Kern-Regeln (`docs/INVARIANTS.md`). Keine Eval-Testdateien. Spec aus Eval-`docs/` nur wenn im Handoff referenziert. |
| **Test Authority** | nur Iron_crab-eval | Spec, öffentliche API (via `ironcrab`), schreibt Szenarien und Invarianten. Kein `Iron_crab/src/`. |
| **Evaluation Runner** | CI | Führt Tests aus, gibt Pass/Fail zurück. |

## Zwei Gates

### A) Impl-CI — Job `Eval (Level 5)`

In `.github/workflows/ci.yml` (Branches `architecture-rebuild`, `architecture-rebuild-next`, `main`, `release/**`):

1. Checkout Iron_crab (PR-Stand)
2. Clone Iron_crab-eval (`main`)
3. Patch: git-Dependency `ironcrab` → Path auf den PR-Checkout
4. `cargo test` im Eval-Repo — **volle** Invarianten-/Blackbox-Suite

Das ist der kanonische Nachweis, dass Impl und Eval-Tests zur gleichen öffentlichen API passen.

### B) Eval-CI — Workflow `Rust` (schlank)

In Iron_crab-eval auf `main`/PRs: `fmt`, `check`, `build`, `clippy -p ironcrab-eval` **ohne** `--all-targets` und **ohne** `cargo test`.

Zusätzlich: manueller Workflow **Eval invariant tests** (`workflow_dispatch`).

## Lokale Entwicklung

```
Trading_bot/
├── Iron_crab/       # impl, Branch architecture-rebuild (gemeinsam)
└── Iron_crab-eval/  # eval, Branch main
```

Path-Patch für lokale volle Suite: siehe [Iron_crab-eval/CONTRIBUTING.md](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/CONTRIBUTING.md). Den Patch nicht committen.

## Tests

Die Suite liegt in `Iron_crab-eval/tests/` (Dutzende Invarianten- und Blackbox-Dateien, u. a. Hot-Path-RPC, LockManager, Quotes, DEX-Parser, Market-Data Admission, Arb Track-Requests). Die Dateiliste in älteren Docs mit nur vier Tests ist veraltet.

Katalog: Iron_crab-eval `docs/spec/INVARIANTS.md` Abschnitt A.

Siehe auch `docs/SPEC_LOCATION.md` und [CONTRIBUTING.md](../CONTRIBUTING.md).
