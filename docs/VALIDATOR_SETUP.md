# Validator & Geyser — aktueller Stand

**Stand:** 2026-08-22  
**Gilt für:** gemeinsamer Impl-Branch `architecture-rebuild`  
**Betrieb:** Änderungen am laufenden Validator nur durch den Maintainer. Dieses File ist die Karte, kein Deploy-Auftrag.

Das Dokument vom 2026-01-09 war ein **einmaliger Optimierungs-Rollout** (Ledger/Cache/DEX-Index). Die Zahlen (RAM 82 GB, 30–60 s Downtime, Plugin-Owner-Filter auf Port 10001) sind **nicht** der heutige Betriebsstand.

---

## Rolle

IronCrab läuft **auf demselben Host** wie ein **nicht-votender** Agave-RPC-Knoten (Solana/Agave 3.x). Ziel: lokale RPC- und Geyser-Latenz, kein fremder RPC im Hot Path.

```text
Agave (non-voting)
  RPC   127.0.0.1:8899
  WS    127.0.0.1:8900
  Geyser Yellowstone gRPC 127.0.0.1:10000
           │
           ▼
      market-data  (explizite Account-/TX-Subscriptions, client-seitig)
           │  NATS
           ├─► momentum-bot / arb-strategy  → TradeIntents
           └─► execution-engine             → Plan → Simulate → Send
```

Hot Path = Geyser / LivePoolCache. RPC ist Cold Path (Liquidation, Bootstrap, manuelle Aktionen). Lokaler Validator hat **keine vollständige TX-Historie** — Mint-Alter u. ä. brauchen einen Indexer (Helius), nicht den lokalen RPC.

---

## Dateien im Repo (Vorlage)

| Datei | Zweck |
|--------|--------|
| `docs/agave-validator-optimized.service` | systemd-Vorlage für `agave-validator` |
| `docs/geyser-grpc-plugin-config.json` | Yellowstone-Plugin (lib + Bind-Adresse) |
| `docs/validator-lag-exporter.py` + `.service` | echter Slot-Lag vs. Mainnet (`:9180`) |
| `docs/systemd/market-data.service` | Bot-Seite: `--geyser-url http://127.0.0.1:10000` |
| `docs/GEYSER_FILTER_UPDATE.md` | Warum das Plugin **keine** Owner-Filter hat |

Auf dem Server liegen die Live-Kopien typischerweise unter:

- `/etc/systemd/system/agave-validator.service`
- `/home/sol/geyser-config.json` (von `--geyser-plugin-config` referenziert)
- Binary: `/usr/local/bin/agave-validator`
- Ledger/Accounts: `/var/solana/ledger`, `/var/solana/accounts`
- User: `sol` (Validator), `ironcrab` (Bot)

Live-Unit und Repo-Vorlage können nach Recoveries (Snapshot, `--expected-genesis-hash`, `--gossip-host`) **abweichen**. Vor Änderungen die Live-Unit lesen, nicht nur dieses File.

---

## Validator-Unit (Repo-Vorlage)

Wesentliche Flags in `docs/agave-validator-optimized.service`:

| Flag | Wert / Bedeutung |
|------|------------------|
| `--no-voting` | RPC/Geyser-Knoten, kein Vote-Credit |
| `--rpc-port` / `--full-rpc-api` | `8899`, volle RPC-API |
| `--enable-rpc-transaction-history` | lokale History, **trotzdem unvollständig** vs. Indexer |
| `--limit-ledger-size` | `100000000` Slots |
| `--accounts-db-cache-limit-mb` | `327680` (320 GiB) |
| `--accounts-index-scan-results-limit-mb` | `16384` |
| `--rpc-threads` | `32` |
| `--geyser-plugin-config` | `/home/sol/geyser-config.json` |
| `--entrypoint` | `entrypoint{,2,3}.mainnet-beta.solana.com:8001` |
| `--dynamic-port-range` | `8000-8025` |

Account-Index (`--account-index program-id spl-token-owner spl-token-mint`) plus Include-Keys beschleunigen **RPC** `getProgramAccounts` / Token-Queries. Das ist **nicht** dasselbe wie Geyser-Subscriptions.

Include-Keys in der Vorlage:

| Pubkey | In `market_data.rs` | Rolle |
|--------|---------------------|--------|
| `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | ja | Raydium AMM V4 |
| `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | ja | Raydium CPMM |
| `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | ja | Orca Whirlpool |
| `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | ja | PumpFun |
| `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | ja | PumpSwap |
| `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | ja | Meteora DLMM |
| `cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D` | ja | Meteora CPMM (IronCrab-Konstante) |
| `A5RH5EVEkUnEfpWvz7b94NqzsforWk63mLcujoXVKiHs` | **nein** | nur Index-Key in der Unit |
| `Ase7z1mRLps2cTNQnRHpLyQL4Q5FHwonjmZnYCTuUDZM` | **nein** | Wallet-Index für RPC-Balances |

Meteora **DAMM v2** (`cpamdpZCGKUy5JxQXB4dcpGPiikHawvSWAd6mEn1sGG`) ist ein anderes Programm und in IronCrab nicht die `METEORA_CPMM`-Konstante.

Schnelle Gossip-Peers messen: `docs/tools/entrypoint_latency_test_v2.sh` (inkl. `*.anza.xyz`). Die Unit nutzt aktuell die `solana.com`-Entrypoints.

---

## Geyser-Plugin

Repo-Stand `docs/geyser-grpc-plugin-config.json`:

```json
{
  "libpath": "/usr/local/lib/solana/libyellowstone_grpc_geyser.so",
  "grpc": {
    "address": "127.0.0.1:10000"
  }
}
```

- Port **10000** (nicht 10001).
- **Keine** Owner-/Account-Listen in der Plugin-JSON. Yellowstone filtert das hier nicht server-seitig.
- Filtering und Membership liegen in **market-data** (explizites Account-Set, Track-Requests, Wallet-/Momentum-/Arb-Pins). Neue DEXes: Code + `market-data`-Deploy, **kein** Validator-Restart nur wegen eines Program-IDs.

Bot: market-data `--geyser-url` / `GEYSER_URL` (Default `http://127.0.0.1:10000`). execution-engine: `[solana] geyser_grpc_url`. Es gibt **kein** Config-Key `[geyser] url`. Caps: `[market_data_geyser] max_tracked_accounts`, `geyser_full_reconnect_threshold` — `docs/CONFIG_SCHEMA.md`.

`market-data.service` setzt `CPUAffinity=48-55` und `Nice=10`, damit der Validator die restlichen Kerne behält.

---

## Gesundheit und Slot-Lag

`getHealth` sagt nicht, ob der Knoten am Tip ist.

**Quelle für Lag:** Sidecar `validator-lag-exporter` (Port `9180`):

```text
ironcrab_validator_slots_behind = reference_getSlot − local_getSlot
```

Grafana-Panels „Slots Behind“ und Alerts nutzen diese Metrik (`job="validator-lag-exporter"`), nicht `solana_node_num_slots_behind` / `getHealth`.

Grobe Schwellen (Dashboard): 0–2 grün, ab 3 gelb, ab 50 orange, ab 200 rot.

Prüfen:

```bash
curl -s http://127.0.0.1:9180/metrics | grep ironcrab_validator_
ss -lntp | grep -E '8899|10000|9180'
curl -s http://127.0.0.1:9801/metrics | grep -E '^geyser_connected|^market_data_geyser_head_slot'
```

Validator kann **in Sync** sein (`slots_behind ≈ 0`), während der Bot `live_pool_cache_miss` / keine Intents hat. Dann liegt es am Pin-/Cache-Lifecycle in market-data, nicht am Validator.

---

## Restart, Catchup, Wipe

- Prozess-Restart ≠ sofort synced. Catchup nach Restart oder Ledger-/Accounts-Wipe dauert **Minuten bis Stunden**, nicht „30–60 Sekunden fertig“.
- Nach Wipe: Geyser und RPC können leben, explizite Pool-Accounts fehlen trotzdem im LivePoolCache, bis Track-Requests/Bootstrap greifen.
- Snapshot-/Genesis-Flags (`--expected-genesis-hash`, `--wal-recovery-mode`, `--only-known-rpc`) gehören in die **Live-Unit**, falls jemals gesetzt — nicht stillschweigend aus dieser Vorlage löschen.

---

## Troubleshooting

| Symptom | Zuerst prüfen |
|---------|----------------|
| Plugin lädt nicht | `journalctl -u agave-validator`, JSON `jq .`, Pfad `libyellowstone_grpc_geyser.so`, `chown sol:sol` |
| Geyser-Port tot | `ss -lntp \| grep 10000`; market-data `--geyser-url` |
| `geyser_connected=1`, aber keine Pools | explizites Set / Track-Requests / Admission, nicht Plugin-Owner-Filter |
| Hoher RAM | `--accounts-db-cache-limit-mb` in der **Live-Unit**; 320 GiB Cache ist Absicht |
| Disk voll | `--limit-ledger-size`; Ledger unter `/var/solana/ledger` |
| „Hinterher“ trotz `getHealth ok` | `ironcrab_validator_slots_behind` |
| RPC langsam, Trading tot | Hot Path darf nicht auf RPC umgestellt werden (I-4 / I-7) |

---

## Was dieses File nicht ist

- Kein Produktions-Deploy-Skript (`deploy.sh` / systemd der Bots → `docs/RUNBOOK_PROD.md`).
- Kein Live-Inventar von RAM/Disk — Januar-2026-Messwerte hier nicht fortschreiben.
- Keine Anleitung, den Validator ohne Maintainer-Freigabe neu zu starten.

Onboarding Code/Spec/Tests: [CONTRIBUTING.md](../CONTRIBUTING.md).
