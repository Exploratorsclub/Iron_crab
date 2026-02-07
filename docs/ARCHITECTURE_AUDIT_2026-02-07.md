# IronCrab Architektur-Audit – 2026-02-07

## Kontext

Systematisches Audit aller RPC-Calls im Codebase mit Fokus auf:
- Hot-Path Latenz (Momentum-Buy, Arb, Sell)
- Geyser-First Architektur-Verstöße
- Killswitch/Liquidation Zuverlässigkeit
- Logik-Bugs und Inkonsistenzen

## Legende Schweregrade

| Symbol | Bedeutung |
|--------|-----------|
| **KRITISCH** | RPC im Hot-Path (Buy/Sell/Arb Pipeline) – verursacht direkte Latenz |
| **VERSTOSS** | RPC wo Geyser-Daten vorhanden sind/sein sollten |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **BOOTSTRAP** | Einmalige Initialisierung beim Start |
| **CLEANUP** | Post-Trade Housekeeping (niedrigere Priorität) |

---

## 1. EXECUTION ENGINE (`src/bin/execution_engine.rs`)

### AKZEPTABEL – Simulation & TX-Sending

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 6971, 7001 | `get_latest_blockhash()` in `simulate_transaction()` | AKZEPTABEL – Simulation braucht Blockhash |
| 7040 | `simulate_transaction_with_config()` | AKZEPTABEL – Simulation ist Pflicht (simulate-gated) |
| 6122, 7102-7104 | `get_latest_blockhash_retry()` in `send_transaction_rpc/with_fallback` | AKZEPTABEL – TX-Send braucht Blockhash |
| 7148 | `send_transaction_with_config()` | AKZEPTABEL – Finale TX-Übermittlung |

### BOOTSTRAP

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 3693 | `get_latest_blockhash()` beim Start | BOOTSTRAP – Einmaliger Healthcheck |

### KRITISCH / VERSTOSS

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 2071-2088 | `get_token_accounts_by_owner()` in `cleanup_wallet_after_liquidation()` | **VERSTOSS** – RPC-Scan aller Token-Accounts nach Liquidation | Wallet-Snapshots aus market-data/JetStream nutzen (wie es `run_liquidation_job()` schon korrekt macht) |
| 2092 | `get_account(&wsol_ata)` in `cleanup_wallet_after_liquidation()` | **VERSTOSS** – WSOL-Check per RPC | WSOL-Status aus Geyser/WalletSnapshot |
| 2347 | `get_account(&token_account_pk)` in Manual-Burn-Job | **VERSTOSS** – Token-Account per RPC bei manueller Burn-Anfrage | Account-Daten aus LivePoolCache oder WalletSnapshot |
| 2400 | `get_token_decimals_or_default()` in Manual-Burn-Job | **VERSTOSS** – Mint-Decimals per RPC | Aus WalletSnapshot/LivePoolCache (Decimals werden mit Geyser-Mint-Info gesendet) |
| 2420 | `get_account(&bc)` in Manual-Burn-Job | **VERSTOSS** – Bonding-Curve-Check per RPC für Route-Validation | LivePoolCache hat Bonding-Curve-State |

---

## 2. DEX-MODULE – KRITISCHSTE VERSTÖSSE

### `src/solana/dex/pumpfun.rs` – Pump.fun Bonding Curve

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 280 | `get_account_retry(bonding_curve)` in `fetch_bonding_curve()` | **KRITISCH** – Bonding-Curve-Fetch bei jedem Quote im Hot-Path | LivePoolCache `CachedPoolState::PumpFun` hat die BC-Daten bereits via Geyser |
| 293 | `get_account(bonding_curve)` in `fetch_bonding_curve_fast()` | **KRITISCH** – Derselbe Call mit Timeout (Sniping-Pfad) | Geyser-Event beim Pool-Create liefert die initialen Daten sofort |
| 700 | `get_account_retry(&token_mint)` | **VERSTOSS** – Mint-Existenzprüfung per RPC | Geyser liefert Mint-Info (schon implementiert in market-data) |
| 981 | `get_account_retry(&bonding_curve)` in `build_swap_ix()` | **KRITISCH** – BC-Fetch für Creator-Auflösung direkt im TX-Build-Pfad! | Code sagt selbst "FALLBACK to RPC" – LivePoolCache sollte den Creator haben |

### `src/solana/dex/pumpfun_amm.rs` – PumpSwap AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 378-465 | **Eigener `rpc_call()` Wrapper** – umgeht komplett die zentrale `SolanaRpc`-Abstraktion | **ARCHITEKTUR-VERSTOSS** – Eigene HTTP-RPC-Implementierung mit Helius-Throttling parallel zum offiziellen RPC-Client |
| 472, 506 | `rpc_call("getAccountInfo")` | **VERSTOSS** – Account-Daten per RPC | LivePoolCache / Geyser |
| 594, 624 | `rpc_call("getTokenAccountsByOwner")` | **KRITISCH** – Token-Account-Discovery per RPC im Quote/Build-Pfad | Wallet-ATAs aus Geyser-Subscription |
| 685-703 | `derive_existing_pda()` mit `rpc_get_account_owner_and_executable()` | **VERSTOSS** – PDA-Existenzprüfung per RPC | PDA-Adressen sind deterministisch; Existenz kann aus Geyser-Account-Updates abgeleitet werden |
| 705-748 | `try_parse_pool_static_from_market_account()` | **VERSTOSS** – Pool-Account per RPC parsen | Sollte aus LivePoolCache/Geyser-Discovery kommen |
| 1388, 1824, 1875, 2044 | `rpc_call_tx_history("getSignaturesForAddress")`, `rpc_call_tx_history("getTransaction")` | **VERSTOSS** – Transaction-History-Abfragen per RPC | Geyser-Transactions liefern die gleichen Daten |

### `src/solana/dex/orca.rs` – Orca Whirlpool

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 359 | `get_account_retry(pool_id)` in `fetch_current_tick()` (dead_code) | Nicht aktiv, aber bereit zum Einsatz | LivePoolCache hat Tick-Daten via Geyser |
| 430-474 | `get_multiple_accounts([vault_a, vault_b])` in `load_reserves_if_needed()` | **KRITISCH** – Vault-Balances per RPC bei Cache-Miss (5min TTL) | Geyser-Vault-Updates in market-data → LivePoolCache (schon teilweise implementiert als `PoolStateUpdate`) |
| 548 | `get_multiple_accounts(&vault_pubkeys)` in `batch_refresh_vault_balances()` | **VERSTOSS** – Batch-Refresh aller Vault-Balances per RPC | Geyser trackt Vault-Accounts bereits in market-data |
| 1503 | `get_account_retry(pool_address)` in `refresh_pools()` | **BOOTSTRAP** – Pool-Fetch beim Startup | Akzeptabel als Bootstrap, sollte aber danach aus Geyser aktualisiert werden |

### `src/solana/dex/raydium.rs` – Raydium AMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 185 | `get_account_retry(pool_address)` in `load_pool_from_geyser()` | **VERSTOSS** – Ironie: Funktion heißt "from_geyser" aber macht RPC-Call! 20 Retries × 500ms = bis zu 10s Latenz | Geyser liefert den Pool-Account direkt im Account-Update-Event |
| 770 | `get_account(&p.market_id)` in `refresh_pools()` | **BOOTSTRAP** – Serum-Market-Account beim Refresh | Akzeptabel als Bootstrap |
| 1296-1297 | `get_token_account_balance()` in `fetch_and_update_reserves()` | **KRITISCH** – Vault-Balances per RPC on-demand | Geyser-Vault-Updates → LivePoolCache |

### `src/solana/dex/meteora_dlmm.rs` – Meteora DLMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 216 | `get_account(pool_addr)` in `update_reserve_balances()` | **VERSTOSS** – Pool-Account-Fetch für Vault-Adressen | LivePoolCache hat Meteora-State |
| 240-241 | `get_account_retry(&reserve_x/y)` | **KRITISCH** – Vault-Balances per RPC bei jedem Quote | Geyser trackt diese Accounts; market-data publiziert `BinArrayUpdate` |

### `src/solana/dex/raydium_cpmm.rs` – Raydium CPMM

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 206-207 | `get_account_retry(&vault_0/1)` | **KRITISCH** – Vault-Balances per RPC | Geyser → LivePoolCache |

---

## 3. TX-INFRASTRUKTUR

### `src/execution/tx_builder.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 218 | `get_account(pool_id)` in `fetch_orca_from_rpc()` | **KRITISCH** – Orca-Whirlpool-Fetch als Fallback im TX-Build-Pfad | LivePoolCache (`CachedPoolState::Orca`) |

### `src/solana/tx_sender.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 459 | `send_transaction_with_config()` | AKZEPTABEL – RPC-Fallback im Fallback-Chain (TPU → Jito → RPC) |

### `src/solana/tpu_client.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 151, 211 | `get_slot()` | AKZEPTABEL – Slot-Query für Leader-Schedule (TPU-Routing) |

### `src/solana/arbitrage.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 315 | `get_latest_blockhash()` | AKZEPTABEL – Simulation |
| 328 | `simulate_transaction()` | AKZEPTABEL – Simulate-gated |
| 129 | `get_balance_retry()` | **VERSTOSS** – Balance-Check per RPC vor Arb-Execution; sollte aus Geyser-Balance-Tracking kommen |

---

## 4. WALLET & TOKEN UTILS

### `src/solana/token_utils.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 13 | `get_token_supply(mint)` | **VERSTOSS** – Mint-Decimals per RPC | Geyser liefert `TokenMintInfo` mit Decimals |
| 18 | `get_account(mint)` (Fallback) | **VERSTOSS** – Dasselbe als Fallback | Aus LivePoolCache oder Geyser-Mint-Subscription |
| 33, 37 | `get_token_supply()` + `get_account()` in `try_token_decimals()` | **VERSTOSS** – Gleiche Logik | Gleiche Lösung |

### `src/wallet.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 211 | `get_balance()` | **VERSTOSS** im Hot-Path / AKZEPTABEL für Utility | Geyser-Balance-Tracking |
| 220 | `get_account(mint)` für Token-Programm-Erkennung | **VERSTOSS** | Geyser-Mint-Info |
| 268 | `get_account(&ata)` für ATA-Existenz-Check | **VERSTOSS** wenn im Hot-Path | Geyser-Account-Subscription |
| 325, 364, 449, 567 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – TX-Sending |
| 385 | `get_account(&to_ata)` | **VERSTOSS** | Geyser-Account-Subscription |

### `src/execution/wsol_manager.rs`

| Zeile | Call | Problem | Geyser-Alternative |
|-------|------|---------|---------------------|
| 501 | `get_balance()` | **VERSTOSS** – SOL-Balance per RPC | Geyser-Wallet-Tracking (market-data publiziert WalletUpdates) |
| 530 | `get_token_account_balance()` | **VERSTOSS** – WSOL-Balance per RPC | Geyser-Wallet-Tracking |
| 846, 895 | `get_latest_blockhash()` + `send_and_confirm_transaction()` | AKZEPTABEL – Wrap/Unwrap TX-Sending |

### `src/execution/account_janitor.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 618, 834, 1075 | `get_latest_blockhash_retry()` + `send_and_confirm_transaction()` | AKZEPTABEL – Housekeeping-TXs (Close-Accounts, Merge) |

---

## 5. SONSTIGE BINARIES

### `src/bin/sell_all.rs` / `src/bin/sell_all_keyless.rs`

Emergency-Tools – hier sind RPC-Calls akzeptabel, da dies keine Hot-Path-Binaries sind.

### `src/bin/market_data.rs`

| Zeile | Call | Bewertung |
|-------|------|-----------|
| 676 | `get_multiple_accounts(&keys)` | BOOTSTRAP – Initiale Account-Daten beim Start vor Geyser-Subscription |

---

## 6. WEITERE ARCHITEKTUR-PROBLEME & LOGIK-BUGS

### BUG A: Killswitch-Liquidation – Token werden übersprungen

**Problem**: Bei `run_liquidation_job()` (Zeile 1316-2061) gibt es mehrere Pfade wo Token übersprungen werden:

1. **Zeile 1948-1997**: `min_out_sol.is_none()` → Token wird übersprungen wenn **kein einziger DEX einen Quote liefert**. Die `quote_attempts` werden ins Decision-Record geschrieben, aber die Ursachen können sein:
   - `pumpfun=err` – Bonding-Curve-Fetch schlägt fehl (RPC-Timeout)
   - `pump_amm=timeout (10s)` – PumpSwap-Quote dauert zu lang
   - `meteora=skip active_id=0 (no Geyser data)` – Keine Geyser-Daten für Meteora
   - `raydium=none` – Kein Raydium-Pool gefunden
   - `orca=skip no_pool_accounts` – Orca hat keine Pool-Accounts im Cache

2. **Zeile 1533-1556**: Pump.fun-Quote ist erfolgreich, aber **Creator fehlt im Cache** → Token wird zu multi-pool-routing degradiert. Wenn auch dort kein Quote kommt → übersprungen

3. **Zeile 1598-1626**: PumpSwap-Quote erfolgreich, aber `pool_accounts_v1_for_base_mint()` gibt `None` zurück → Token wird übersprungen

**Root Cause für 2 fehlende Token**: Wahrscheinlich eine Kombination aus:
- RPC-Timeouts bei den Quote-Requests (die DEX-Module machen RPC-Calls im Quote-Pfad!)
- Fehlende Geyser-Daten (Meteora `active_id=0`)
- Creator nicht im LivePoolCache

### BUG B: `load_pool_from_geyser()` in `raydium.rs` macht 20 RPC-Retries

**Zeile 177-213**: Die Funktion heißt `load_pool_from_geyser()` aber macht bis zu **20 RPC-Calls mit 500ms Delay** (= bis zu 10 Sekunden Latenz). Dies blockiert die Geyser-Event-Verarbeitung.

### BUG C: PumpFunAmmDex hat eigene RPC-Infrastruktur

**Zeilen 378-465**: `pumpfun_amm.rs` hat einen komplett eigenen `rpc_call()` HTTP-Client mit:
- Eigenem Retry-Logic
- Eigenem Rate-Limiting (Helius-Throttle)
- Eigenem Endpoint-Fallback
- **Umgeht komplett die zentrale `SolanaRpc`** mit ihrem Adaptive Limiter

Dies bedeutet:
- RPC-Rate-Limits werden nicht global koordiniert
- Metrics erfassen diese Calls nicht korrekt
- Keine Geyser-Integration möglich ohne komplettes Refactoring

### BUG D: Token-Decimals werden immer per RPC geholt

**`token_utils.rs`** wird an vielen Stellen aufgerufen (z.B. Zeile 2400 in execution_engine.rs). **Jeder Call macht 1-2 RPC-Requests**. Die Decimals sind statisch (ändern sich nie nach Mint-Erstellung) und sollten einmal aus Geyser-Mint-Info gecacht werden.

### BUG E: `cleanup_wallet_after_liquidation()` macht RPC statt Geyser

**Zeile 2071-2088**: Direkt nach der Liquidation wird `get_token_accounts_by_owner()` per RPC aufgerufen – obwohl `run_liquidation_job()` korrekt JetStream-Snapshots verwendet. **Inkonsistenz**: Liquidation = Geyser-basiert, Cleanup = RPC-basiert.

### BUG F: Orca Reserve-Fetching hat 5min TTL mit RPC-Fallback

**Zeile 400-475**: `load_reserves_if_needed()` hat einen 5-Minuten-Cache und fällt dann auf RPC zurück. Bei 50+ Pools = 50+ RPC-Calls alle 5 Minuten. Geyser liefert Vault-Updates in Echtzeit.

---

## 7. HANDLUNGSEMPFEHLUNGEN

### Priorität 1 – Hot-Path RPC eliminieren (Latenz-kritisch)

| # | Datei | Problem | Fix |
|---|-------|---------|-----|
| 1 | `pumpfun.rs:280,293` | BC-Fetch per RPC bei jedem Quote | Quote aus `CachedPoolState::PumpFun` im LivePoolCache berechnen |
| 2 | `pumpfun.rs:981` | Creator per RPC im TX-Build | Creator MUSS aus Cache kommen; wenn nicht → Intent ablehnen statt RPC-Call |
| 3 | `meteora_dlmm.rs:240-241` | Vault-Balances per RPC | Geyser-Vault-Subscription in market-data → NATS → LivePoolCache |
| 4 | `raydium.rs:1296-1297` | Vault-Balances per RPC | Gleiche Lösung |
| 5 | `raydium_cpmm.rs:206-207` | Vault-Balances per RPC | Gleiche Lösung |
| 6 | `orca.rs:430-474` | Vault-Balances per RPC (5min TTL) | Gleiche Lösung |
| 7 | `tx_builder.rs:218` | Orca-Whirlpool-Fetch per RPC | LivePoolCache `CachedPoolState::Orca` nutzen |
| 8 | `token_utils.rs:13-44` | Mint-Decimals per RPC | Globalen Decimals-Cache aus Geyser-Mint-Info aufbauen |

### Priorität 2 – Architektur-Verstöße beheben

| # | Datei | Problem | Fix |
|---|-------|---------|-----|
| 9 | `pumpfun_amm.rs` (komplett) | Eigene RPC-Infrastruktur | LivePoolCache für PumpSwap-Pools nutzen; RPC nur als letzten Fallback |
| 10 | `raydium.rs:185` | `load_pool_from_geyser()` macht RPC | Geyser-Account-Update direkt parsen statt RPC-Fetch |
| 11 | `execution_engine.rs:2071-2092` | Cleanup per RPC | WalletSnapshot aus JetStream nutzen (wie Liquidation selbst) |
| 12 | `wsol_manager.rs:501,530` | Balance per RPC | Geyser-Balance-Updates aus market-data |
| 13 | `execution.rs:129` | Balance per RPC vor Arb | Geyser-Balance |

### Priorität 3 – Killswitch-Zuverlässigkeit

| # | Problem | Fix |
|---|---------|-----|
| 14 | Token werden übersprungen wenn kein Quote kommt | Logging erweitern; Retry mit Timeout für übersprungene Token; Zweiter Durchlauf mit erhöhter Slippage |
| 15 | Pump.fun Creator fehlt im Cache | Sicherstellen dass market-data den Creator für ALLE bekannten Bonding Curves cached |
| 16 | PumpSwap `pool_accounts_v1_for_base_mint()` gibt `None` | Pool-Accounts aus LivePoolCache/Geyser-Discovery statt RPC-Discovery |

---

## 8. GESCHÄTZTE LATENZ-AUSWIRKUNG

Ein typischer **Momentum-Buy** durchläuft aktuell:
1. Quote → PumpFun `fetch_bonding_curve_fast()` = **+200-2000ms RPC** (sollte: 0ms aus Cache)
2. TX-Build → `build_swap_ix()` → ggf. Creator-RPC-Fallback = **+500-2000ms** (sollte: 0ms)
3. Simulation → `simulate_transaction()` = **+100-500ms** (unvermeidlich)
4. TX-Send → `send_transaction` = **+50-400ms** (unvermeidlich)

**Aktuelle Gesamtlatenz**: ~1000-5000ms
**Optimierte Latenz (nur Sim+Send)**: ~200-900ms

Das ist ein **3-5x Geschwindigkeitsgewinn** der für Early-Momentum und Arbitrage entscheidend ist.
