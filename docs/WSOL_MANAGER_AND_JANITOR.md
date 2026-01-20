# WSOL Manager & Account Janitor Implementation

**Status**: Phase 1.5 Complete (WsolManager + Arb-TX Optimization)  
**Created**: 2026-01-20  
**Last Updated**: 2026-01-20

## Motivation

Professionelle Arbitrage-Bots vermeiden wrap/unwrap in der eigentlichen Arb-TX:
- Spart ~21k+ CU pro TX (WSOL ATA + Transfer + SyncNative entfernt)
- Weniger Instructions = schnellere Serialisierung
- Arb-TX ist jetzt minimal: Token ATA (idempotent) + Swaps + Jito Tip

## Architektur-Übersicht

```
execution-engine
├── main_loop (Intent processing)
├── WsolManager (tokio task) ✅ IMPLEMENTED
│   ├── Event-driven via NATS (wallet balance updates)
│   ├── Polling-only fallback (wenn NATS unavailable)
│   ├── Wrap wenn WSOL < min_wsol
│   └── Unwrap wenn WSOL > max_wsol (close ATA)
└── AccountJanitor (tokio task) 🔜 Phase 2
    ├── Merge dust gleicher Token (alle 5 min)
    ├── Swap dust → SOL (alle 24h)
    └── Close empty ATAs (alle 7 Tage)
```

## Komponenten

### 1. WsolManager ✅

**Verantwortlichkeiten:**
- Sicherstellen dass genug WSOL für Arbitrage bereitsteht
- Event-driven via Geyser → market-data → NATS
- Wrap/Unwrap Operationen (KEINE in Arb-TX)
- Fallback: Polling alle 30s wenn NATS nicht verfügbar

**Trigger:**
- NATS Event: `ironcrab.v1.wallet_balance.<wallet>` (von market-data)
- Fallback: Periodic polling (30s oder 60s interval)

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

# Swap dust → SOL
swap_dust_interval_secs = 86400     # 24h
swap_dust_min_value_sol = 0.01      # Nur wenn > 0.01 SOL wert
swap_dust_max_slippage_bps = 500    # 5% max slippage
swap_dust_use_jupiter = true        # Jupiter API für beste Route

# Close empty ATAs
close_ata_interval_secs = 604800    # 7 Tage
close_ata_min_age_secs = 86400      # Nur wenn älter als 24h
close_ata_max_per_run = 20          # Max ATAs pro Run
```

---

## Implementation Checklist

### Phase 1: WsolManager (Priority: HIGH) ✅ COMPLETE

#### 1.1 NATS Infrastructure ✅
- [x] NATS Topic: `ironcrab.v1.wallet_balance.<wallet>` (in `src/nats/topics.rs`)
- [x] `wallet_balance_topic()` helper function
- [x] `WalletBalanceUpdate` struct in `src/execution/wsol_manager.rs`

#### 1.2 WsolManager Core ✅
- [x] `src/execution/wsol_manager.rs` erstellen
- [x] `WsolManagerConfig` struct (in `src/config.rs` unter `ExecutionEngineCfg`)
- [x] `WsolManager` struct mit:
  - [x] `new(config, treasury, rpc)` constructor
  - [x] `run()` async main loop (NATS subscription + periodic fallback)
  - [x] `run_polling_only()` fallback wenn NATS unavailable
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

#### 1.6 Testing
- [ ] Unit tests für WsolManager logic
- [ ] Integration test: Balance update → Wrap trigger
- [ ] Dry-run test auf Server

---

### Phase 2: AccountJanitor (Priority: MEDIUM)

#### 2.1 AccountJanitor Core
- [ ] `src/execution/account_janitor.rs` erstellen
- [ ] `AccountJanitorConfig` struct
- [ ] `AccountJanitor` struct mit:
  - [ ] `new(config, treasury, rpc)` constructor
  - [ ] `run()` async main loop (timer-based)
  - [ ] Separate timer für jede Aktion

#### 2.2 Merge Dust (gleiche Token)
- [ ] `find_duplicate_atas()` - ATAs für gleichen Mint finden
- [ ] `build_merge_ix()` - Transfer von ATA_2 → ATA_1
- [ ] `execute_merge()` - TX bauen und senden

#### 2.3 Swap Dust → SOL
- [ ] `find_dust_tokens()` - Tokens mit kleinem Wert finden
- [ ] `get_token_value_sol()` - Wert schätzen (via price cache oder Jupiter)
- [ ] Jupiter Quote API Integration
- [ ] `execute_dust_swap()` - Swap TX bauen und senden

#### 2.4 Close Empty ATAs
- [ ] `find_empty_atas()` - ATAs mit balance == 0
- [ ] `get_ata_age()` - Wann wurde ATA erstellt (first tx timestamp)
- [ ] `build_close_ata_ix()` - closeAccount instruction
- [ ] `execute_close_atas()` - Batch close (max N pro TX)

#### 2.5 Integration execution-engine
- [ ] Config parsing für `[account_janitor]`
- [ ] AccountJanitor Task spawnen in `main()`
- [ ] Graceful shutdown handling

#### 2.6 Testing
- [ ] Unit tests für Janitor logic
- [ ] Integration test: Timer → Action trigger
- [ ] Dry-run test auf Server (log only, no send)

---

### Phase 3: Monitoring & Observability

- [ ] Prometheus Metrics:
  - [ ] `wsol_balance_gauge` - Aktuelle WSOL Balance
  - [ ] `wsol_wrap_total` - Wrap Counter
  - [ ] `wsol_unwrap_total` - Unwrap Counter
  - [ ] `janitor_merge_total` - Merge Counter
  - [ ] `janitor_swap_total` - Swap Counter
  - [ ] `janitor_close_ata_total` - Close ATA Counter
  - [ ] `janitor_sol_recovered_total` - SOL recovered from rent
- [ ] Logging: Jede Aktion mit Details loggen
- [ ] DecisionRecords für Wrap/Unwrap/Janitor Aktionen

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
└── ...

src/bin/
├── execution_engine.rs     # + WsolManager spawn ✅
└── market_data.rs          # + TrackedWallet, Geyser wallet tracking, WalletBalanceUpdate ✅
```

---

## Open Questions

1. **Jupiter Integration**: Eigener HTTP Client oder existierende Lib?
   - Tendenz: Eigener minimaler Client, nur Quote + Swap Endpoints

2. **ATA Age Detection**: Wie bestimmen wir das Alter einer ATA?
   - Option A: getSignaturesForAddress → älteste Signature
   - Option B: Lokale Tracking-Datenbank
   - Tendenz: Option A (einfacher, keine State)

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
- `reqwest` - Jupiter API (falls benötigt)
- `tokio` - Async runtime

---

## Rollout Plan

1. **Dev**: Implementierung + Unit Tests
2. **Staging**: Dry-run Mode (log only, no send)
3. **Prod Phase 1**: WsolManager enabled, Janitor disabled
4. **Prod Phase 2**: Full enablement nach 1 Woche monitoring
