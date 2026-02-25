# Level-5 Evaluator Workflow

## Übersicht

Der Level-5-Setup trennt Implementation (ironcrab) und Evaluation (Iron_crab-eval) in separate Repos. Die CI führt Eval-Tests als eigenen Job aus und meldet nur Pass/Fail.

## Architektur

| Rolle | Workspace | Sichtbarkeit |
|-------|-----------|--------------|
| **Implementation Agent** | nur ironcrab | Code, CI-Logs (Pass/Fail), keine Spec, keine Eval-Tests |
| **Test Authority** | nur Iron_crab-eval | Spec, API (via ironcrab), schreibt Szenarien und Invarianten |
| **Evaluation Runner** | CI | Führt Eval-Tests aus, gibt nur Ergebnis zurück |

## CI-Flow

1. **Checkout** ironcrab
2. **Clone** Iron_crab-eval → `ironcrab-eval/`
3. **Build + cargo test** in `ironcrab-eval/` (Abhängigkeit: `ironcrab = { path = ".." }`)
4. **Report** Pass/Fail

Job-Name: `Eval (Level 5)` in `.github/workflows/ci.yml`.

## Lokale Entwicklung

Klonen von Iron_crab-eval als Sibling von Iron_crab:

```
Trading_bot/
├── Iron_crab/       # impl
└── Iron_crab-eval/  # eval
```

In Iron_crab-eval: `Cargo.toml` mit `path = "../Iron_crab"` für lokale Dev (oder `path = ".."` wenn ironcrab-eval als Unterordner von Iron_crab geklont wird, wie in CI).

## Tests

- **Blackbox**: `pump_amm_geyser_first`, `ipc_schema_serde` (LivePoolCache, Quote-Calculator, IPC-Schema Roundtrip)
- **Invarianten**: `invariants_lock_manager`, `invariants_quote_monotonic` (LockManager, Quote-Monotonie)

Siehe auch `docs/SPEC_LOCATION.md` für den Spezifikations-Standort.
