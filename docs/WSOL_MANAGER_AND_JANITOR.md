# WSOL Manager & Account Janitor

**Stand:** 2026-08-22

Wrap/Unwrap liegt **nicht** in der Arb-TX. `WsolManager` in der execution-engine folgt JetStream-`WalletBalanceSnapshot` (market-data Geyser → `WALLET_SNAPSHOT`). Nach erfolgreichem Wrap gilt ein Pending-Floor, bis der Snapshot nachzieht; bei Timeout **ein** RPC-Resync (Cold Path), kein 30-Sekunden-Polling als Normalbetrieb.

```
market-data (Geyser)
        │  JetStream WALLET_SNAPSHOT
        ▼
execution-engine WsolManager → wrap/unwrap TX (Signer)
```

## Komponenten

### 1. WsolManager ✅

**Verantwortlichkeiten:**
- WSOL-Puffer für Arbitrage halten
- Event-driven: Geyser → market-data → JetStream `WALLET_SNAPSHOT`
- Wrap/Unwrap außerhalb der Arb-TX
- Nach Wrap: Pending-Floor bis Snapshot; Timeout → ein RPC-Resync (Cold Path)

**Trigger:**
- JetStream subject `ironcrab.wallet_snapshot.{wallet}.{mint}` (`WalletBalanceSnapshot`)
- Nicht: deprecated Core-NATS `ironcrab.v1.wallet_balance.*`

**Konfiguration:**
```toml
[execution_engine.wsol_manager]
enabled = true
min_wsol_sol = 0.5        # Wrap-Trigger: wenn WSOL < min
target_wsol_sol = 1.0     # Ziel-Balance nach Wrap
max_wsol_sol = 2.0        # Unwrap-Trigger: wenn WSOL > max
min_native_sol = 0.1      # SOL reserve für rent
cooldown_secs = 30        # Cooldown zwischen Aktionen
dry_run = false           # Log only, kein TX send
```

**Logic:**
```
on_wsol_balance_update(new_balance):
    if new_balance < min_wsol:
        wrap_amount = target_wsol - new_balance
        if sol_balance >= wrap_amount:
            execute_wrap(wrap_amount)
    elif new_balance > max_wsol:
        unwrap_amount = new_balance - target_wsol
        execute_unwrap(unwrap_amount)  # Closes entire WSOL ATA
```

### 2. AccountJanitor

**Verantwortlichkeiten:**
- Aufräumen von Dust und ungenutzten Accounts
- Background-Task, low-priority
- Mehrere Aktionen mit unterschiedlichen Intervallen

**Aktionen:**

| Aktion | Intervall | Bedingung | Beschreibung |
|--------|-----------|-----------|--------------|
| Merge Dust | 5 min | balance > 0 && ATAs für gleichen Token | Mehrere ATAs → eine ATA |
| Swap Dust | 24h | value > 0.01 SOL | Token → SOL via Jupiter |
| Close Empty ATA | 7 Tage | balance == 0 && age > 24h | SOL (rent) zurückbekommen |

**Konfiguration:**
```toml
[account_janitor]
enabled = true

# Merge gleicher Token nach Arb
merge_dust_interval_secs = 300      # 5 min
merge_dust_min_value_sol = 0.001    # Nur wenn > 0.001 SOL wert

# Swap dust → SOL (via interne DEX-Integration)
swap_dust_interval_secs = 86400     # 24h
swap_dust_min_value_sol = 0.01      # Nur wenn > 0.01 SOL wert
swap_dust_max_slippage_bps = 500    # 5% max slippage

# Close empty ATAs
close_ata_interval_secs = 604800    # 7 Tage
close_ata_min_age_secs = 86400      # Nur wenn älter als 24h
close_ata_max_per_run = 20          # Max ATAs pro Run
```

---

## Implementation Checklist

### Phase 1: WsolManager (Priority: HIGH) ✅ COMPLETE

#### 1.1 Wallet snapshots ✅
- [x] JetStream `WALLET_SNAPSHOT` / `ironcrab.wallet_snapshot.{wallet}.{mint}`
- [x] Deprecated: Core-NATS `ironcrab.v1.wallet_balance.*` (nicht SSOT)

#### 1.2 WsolManager Core ✅
- [x] `src/execution/wsol_manager.rs`
- [x] Config unter `[execution_engine.wsol_manager]`
- [x] Snapshot-driven wrap/unwrap; RPC-Resync nur Cold Path nach Wrap-Timeout
  - [x] `handle_balance_update()` handler
  - [x] `execute_wrap()` - SOL → WSOL
  - [x] `execute_unwrap()` - WSOL → SOL (closes ATA)

#### 1.3 Wrap/Unwrap Instructions ✅
- [x] `build_wrap_sol_ix()` - via `Treasury.build_wrap_sol_ixs()` (existiert bereits)
- [x] `build_and_send_unwrap_tx()` - WSOL → Native SOL (close ATA)
- [x] `build_and_send_wrap_tx()` - Native SOL → WSOL

#### 1.4 Integration execution-engine ✅
- [x] Config parsing für `[execution_engine.wsol_manager]`
- [x] WsolManager Task spawnen in `main()`
- [x] Graceful shutdown handling (watch::channel)
- [x] Separate NATS connection für WsolManager

### Phase 1.5: Geyser Wallet Tracking + Arb-TX Optimization ✅ COMPLETE

#### 1.5.1 market-data Wallet Balance Updates ✅
- [x] `TrackedWallet` struct in `src/bin/market_data.rs`
- [x] Geyser subscription für Wallet + WSOL ATA
- [x] `WalletBalanceUpdate` publishing zu NATS
- [x] `IRONCRAB_WALLET_PUBKEY` env var support

#### 1.5.2 Arb-TX Optimierung ✅
- [x] WSOL ATA creation aus `build_swap_plan()` entfernt (~20k CU gespart)
- [x] Wrap SOL (Transfer + SyncNative) aus `build_swap_plan()` entfernt (~1.3k CU gespart)
- [x] Verify: Arb-TX hat jetzt nur: Token ATA (idempotent) + Buy Swap + Sell Swap
- [x] Compute Units reduziert: 500k → 450k

#### 1.6 Testing ✅
- [x] Unit tests für WsolManager logic (12 tests)
- [ ] Integration test: Balance update → Wrap trigger (requires mock RPC)
- [ ] Dry-run test auf Server

---

### Phase 2: AccountJanitor (Priority: MEDIUM) - Close ATAs ✅ COMPLETE

#### 2.1 AccountJanitor Core ✅
- [x] `src/execution/account_janitor.rs` erstellen
- [x] `AccountJanitorConfig` struct (in `src/config.rs`)
- [x] `AccountJanitor` struct mit:
  - [x] `new(config, treasury, rpc)` constructor
  - [x] `run()` async main loop (timer-based)
  - [x] Graceful shutdown via watch::channel

#### 2.2 Merge Dust (gleiche Token) ✅ COMPLETE
- [x] `find_duplicate_atas()` - ATAs für gleichen Mint finden
- [x] `merge_atas_for_mint()` - Transfer von ATA_2 → canonical ATA
- [x] `run_merge_duplicate_atas()` - TX bauen und senden
- [x] Config: `merge_dust_enabled`, `merge_dust_interval_secs`, `merge_dust_max_per_run`
- [x] Prometheus metrics: `JANITOR_MERGE_DUST_TOTAL`, `JANITOR_TOKENS_MERGED_TOTAL`

#### 2.3 Swap Dust → SOL ✅ COMPLETE (via interne DEX, nicht Jupiter)
- [x] `find_dust_tokens()` - Tokens mit kleinem Wert finden
- [x] Wert schätzen via Router `best_quote_exact_in()` → WSOL
- [x] Route über Raydium/Orca/Meteora (existierende DEX-Integration)
- [x] `swap_dust_token()` - Swap TX bauen und senden
- [x] Config: `swap_dust_enabled`, `swap_dust_interval_secs`, `swap_dust_min_value_sol`, `swap_dust_max_slippage_bps`, `swap_dust_max_per_run`
- [x] Prometheus metrics: `JANITOR_SWAP_DUST_TOTAL`, `JANITOR_SWAP_DUST_SOL_RECOVERED`, `JANITOR_SWAP_DUST_FAILED`
- [x] **Kein Jupiter** - keine externe API Dependency

#### 2.4 Close Empty ATAs ✅
- [x] `find_empty_atas()` - ATAs mit balance == 0
- [x] `estimate_ata_age()` - Alter via getSignaturesForAddress
- [x] `close_atas()` - Batch close mit closeAccount instruction
- [x] Configurable: interval, min_age, max_per_run, dry_run

#### 2.5 Integration execution-engine ✅
- [x] Config parsing für `[execution_engine.account_janitor]`
- [x] AccountJanitor Task spawnen in `main()`
- [x] Graceful shutdown handling

#### 2.6 Testing ✅
- [x] Unit tests für config defaults (12 tests)
- [ ] Dry-run test auf Server (log only, no send)

---

### Phase 3: Monitoring & Observability ✅ COMPLETE

- [x] Prometheus Metrics:
  - [x] `wsol_balance_lamports` - Aktuelle WSOL Balance (gauge)
  - [x] `wsol_wrap_total` - Wrap Counter
  - [x] `wsol_unwrap_total` - Unwrap Counter
  - [x] `wsol_wrap_lamports_total` - Total lamports wrapped
  - [x] `wsol_unwrap_lamports_total` - Total lamports unwrapped
  - [x] `janitor_close_ata_total` - Close ATA Counter
  - [x] `janitor_sol_recovered_lamports` - SOL recovered from rent
  - [x] `janitor_sweep_runs_total` - Janitor sweep runs
  - [x] `janitor_accounts_scanned_total` - Total accounts scanned
- [x] Metrics in `src/metrics.rs` definiert und exportiert
- [x] Integration in `wsol_manager.rs` und `account_janitor.rs`
- [x] Logging: Jede Aktion mit Details loggen (tracing)
- [x] DecisionRecords für Wrap/Unwrap/Janitor Aktionen via JSONL ✅

---

## File Structure (Final)

```
src/execution/
├── mod.rs                  # pub mod wsol_manager; pub mod account_janitor;
├── wsol_manager.rs         # WsolManager ✅
├── account_janitor.rs      # AccountJanitor (Phase 2)
├── live_pool_cache.rs
├── quote_calculator.rs
└── ...

src/nats/
├── topics.rs               # + TOPIC_WALLET_BALANCE_PREFIX, wallet_balance_topic() ✅
└── ...

src/ipc/
├── schema.rs               # WalletBalanceSnapshot (startup)
└── ...

src/solana/
├── cross_dex_handler.rs    # build_swap_plan() - optimized (no WSOL wrap) ✅
│                           # + get_all_dexes() - für Router (Janitor swap_dust)
└── ...

src/bin/
├── execution_engine.rs     # + WsolManager spawn ✅
│                           # + AccountJanitor spawn mit Router ✅
└── market_data.rs          # + TrackedWallet, Geyser wallet tracking, WalletBalanceUpdate ✅

trade_logs/
├── decisions/              # Decision records (execution pipeline)
├── executions/             # Execution results
├── burns/                  # Burn operations
├── wsol/                   # WSOL Manager actions (wrap/unwrap) ✅ NEW
│   └── wsol_actions-YYYYMMDD.jsonl
└── janitor/                # Account Janitor actions ✅ NEW
    └── janitor_actions-YYYYMMDD.jsonl
```

---

## Open Questions

1. ~~**Jupiter Integration**: Eigener HTTP Client oder existierende Lib?~~
   - **Entscheidung**: Kein Jupiter! Externe API Dependency vermeiden.
   - Dust Swap über interne DEX-Integration (Raydium/Orca/Meteora)
   - Wir haben bereits Quote-Funktionen für diese DEXes

2. **ATA Age Detection**: Wie bestimmen wir das Alter einer ATA? ✅ RESOLVED
   - Lösung: `getSignaturesForAddress` → älteste Signature timestamp

3. **Merge Dust Timing**: Direkt nach Arb oder periodic?
   - Tendenz: Periodic (5 min) - Arb-TX nicht verzögern

4. **Error Handling**: Was passiert wenn Wrap/Unwrap failed?
   - Retry mit exponential backoff
   - Alert wenn mehrfach failed

---

## Dependencies

Keine neuen crates nötig, alles bereits vorhanden:
- `solana-sdk` - TX Building
- `spl-token` - Token Instructions
- `tokio` - Async runtime

---

## Rollout Plan

1. **Dev**: Implementierung + Unit Tests
2. **Staging**: Dry-run Mode (log only, no send)
3. **Prod Phase 1**: WsolManager enabled, Janitor disabled
4. **Prod Phase 2**: Full enablement nach 1 Woche monitoring
