# Mitentwickeln an Iron_crab

Stand: 2026-08-22 (GitHub `architecture-rebuild`).

Dieses Repo ist die **Implementierung**. Spec und Eval-Tests liegen in [Iron_crab-eval](https://github.com/Exploratorsclub/Iron_crab-eval). Produktions-Deploy bleibt beim Maintainer — hier geht es um Code, Spec und Tests.

## Zwei Repos, zwei Branches

| Repo | Rolle | Branch für Mitentwickler | Hinweis |
|------|--------|--------------------------|---------|
| `Exploratorsclub/Iron_crab` | Implementation | **`architecture-rebuild`** | Das ist der gemeinsame Stand. PRs hierhin. |
| `Exploratorsclub/Iron_crab-eval` | Spec + Blackbox-/Invarianten-Tests | **`main`** | — |

`architecture-rebuild-next` ist die **aktive Maintainer-Entwicklung** und nicht der Clone für die gemeinsame Arbeit. GitHub-Default von Iron_crab ist `architecture-rebuild` — den auschecken.

```text
Trading_bot/
├── Iron_crab/        # dieses Repo
└── Iron_crab-eval/   # Sibling, gleicher Parent-Ordner
```

## Was zuerst lesen

1. Dieses File
2. `docs/INVARIANTS.md` — kompakte P0-Regeln für den Hot Path
3. [Eval-Spec `INVARIANTS.md`](https://github.com/Exploratorsclub/Iron_crab-eval/blob/main/docs/spec/INVARIANTS.md) — lebender Katalog inkl. eval-getesteter Regeln
4. `AGENTS.md` — STOP-CHECK vor jeder Code-Änderung
5. `.cursor/rules/ironcrab-core.mdc` — verbindliche Agent-/Review-Regeln
6. Spec-Überblick: `docs/SPEC_LOCATION.md`

Bei Konflikt zwischen älterer Zielarchitektur und Invarianten gelten **Invarianten + dieses CONTRIBUTING**. Die originale Zielarchitektur in Eval `docs/spec/TARGET_ARCHITECTURE.md` ist Referenz, kein Freibrief.

## Architektur in einem Absatz

Mehrere Prozesse, NATS (`ironcrab.v1.*`), JetStream für Bot-Zustand, Geyser-First für den Hot Path.

- `market-data` (9801) — Geyser-Ingest, Pool-Discovery, MarketEvents, Wallet-Updates
- `momentum-bot` (9802) — EARLY/ESTABLISHED, nur `TradeIntent`s
- `arb-strategy` (9803) — marktgetriebene Arbitrage, nur `TradeIntent`s (eigenes Binary, nicht optional)
- `execution-engine` (9804) — einziger Signer; Plan → Simulate → Send → Confirm
- `position-manager` (9805) — einziger Writer des KV `POSITION_AUTHORITY` (Positions-Daten-SSOT)
- `control-plane` (8080, Python) — REST, Kill-Switch, Config (keyless)
- `trades-server` (9899, Python) — Grafana-Datasource

DEXes (Geyser-First): Raydium AMM V4, Raydium CPMM, Orca Whirlpool, Meteora DLMM, Meteora CPMM, PumpFun, PumpSwap (PumpFun AMM).

## P0-Regeln (nicht verhandeln)

- **Single-Signer:** nur `execution-engine` lädt Keys und sendet.
- **Intent-only:** Strategien erzeugen `TradeIntent`s, keine TXs.
- **Simulate-gated:** Simulation fehlgeschlagen ⇒ nicht senden.
- **Decision Record:** kein stilles Verwerfen von Intents.
- **Hot Path RPC-frei:** alles, was im normalen Momentum-/Arb-Flow von `process_intent` erreichbar ist, darf kein RPC machen. RPC nur Cold Path (Liquidation, Bootstrap, manuelle Aktionen), typisch hinter `allow_rpc_on_miss` / `allow_rpc_fallback`.
- **Geyser-First:** bestehenden Cache-/Geyser-Pfad nicht durch RPC ersetzen.
- **Level-5:** Eval-Tests (`Iron_crab-eval/tests/`) nicht lesen und nicht anpassen. Spec unter `Iron_crab-eval/docs/` darf gelesen werden.

## Lokales Setup (ohne Deploy)

Voraussetzungen: Rust **1.89.0** (`rust-toolchain.toml`), `protobuf-compiler`, NATS mit JetStream. Unter Windows Rust-Builds über **WSL2** (nicht `/mnt/c/`, sondern Linux-Filesystem). Details: `docs/LOCAL_SETUP.md`.

```bash
git clone https://github.com/Exploratorsclub/Iron_crab.git
cd Iron_crab
git checkout architecture-rebuild
git clone https://github.com/Exploratorsclub/Iron_crab-eval.git ../Iron_crab-eval

cp config.example.toml config.toml   # RPC / Geyser / Keypair nur lokal, nie committen
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo test --features test_helpers
```

Eval-Suite lokal (Sibling-Checkout, `ironcrab` per Path-Patch wie in CI):

```bash
cd ../Iron_crab-eval
# siehe Iron_crab-eval/CONTRIBUTING.md — volle Suite, nicht das schlanke Eval-PR-Gate
cargo test
```

## PRs

- Branch von `architecture-rebuild` ableiten; PR-Ziel ist `architecture-rebuild`.
- Klein und isoliert. Architektur-Änderungen nur mit expliziter Freigabe.
- CI in diesem Repo (Workflow `CI`): `fmt`, `clippy -D warnings`, Unit-Tests, `test_helpers`, Release-Build, optional Python-Feature, `cargo-audit`, plus Job **Eval (Level 5)** (`cargo test` im Eval-Repo gegen den PR-Checkout).
- Grüne CI ist die Definition of Done für den Change. Fachliche Invarianten prüft **Eval (Level 5)**, nicht das schlanke Eval-PR-Gate.

## Was hier nicht dokumentiert ist

Produktions-Deploy, systemd, Server-SSH und Live-Keys. Fragen dazu an den Maintainer, nicht in PRs mischen.

## Weitere Docs

| Thema | Ort |
|--------|-----|
| Spec (kanonisch) | Iron_crab-eval `docs/spec/` |
| Eval-Workflow / CI | `docs/LEVEL5_EVAL_WORKFLOW.md` |
| Cursor-Fenster (Impl vs Eval) | `docs/LEVEL5_CURSOR_SETUP.md` |
| Config | `docs/CONFIG_SCHEMA.md` |
| Bekannte Bugmuster | `docs/KNOWN_BUG_PATTERNS.md` |
| Runbook (Betrieb, Maintainer) | `docs/RUNBOOK_PROD.md` |
| Validator / Geyser | `docs/VALIDATOR_SETUP.md` |
