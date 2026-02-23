# IronCrab Architektur-Audit – Konsolidierte Fassung

**Stand:** 2026-02-23 | **Quellen:** main branch (ARCHITECTURE_AUDIT_2026-02-07) + arch-audit-tsw Worktree (ARCHITECTURE_AUDIT_2026-02-23)

> Dieses Dokument ist die einzige aktuelle Architektur-Audit-Quelle. Enthält Revert-Analyse (main), RPC-Matrix (beide), **CrossDexHandler PumpFun-Befund** (tsw) und SSOT-Analyse.

---

## Inhaltsverzeichnis

1. [Kontext und Revert-Historie](#1-kontext-und-revert-historie)
2. [Neue Erkenntnisse aus arch-audit-tsw (2026-02-23)](#2-neue-erkenntnisse-aus-arch-audit-tsw-2026-02-23)
3. [RPC im Hot Path](#3-rpc-im-hot-path)
4. [Single Source of Truth](#4-single-source-of-truth)
5. [Logik-Bugs und Architektur-Probleme](#5-logik-bugs-und-architektur-probleme)
6. [Bereits umgesetzte Audit-Implementierungen](#6-bereits-umgesetzte-audit-implementierungen)
7. [Priorisierte Empfehlungen](#7-priorisierte-empfehlungen)

---

## 1. Kontext und Revert-Historie

### 1.1 Revert vom 2026-02-09

Am 2026-02-09 wurde der Branch auf `e341c04b` zurückgesetzt (Hard-Reset), weil Änderungen aus 18 Commits ungewollt die Liquidation zerstört und Architekturprinzipien verletzt hatten. Danach wurden 6 gezielte Fixes für Liquidation und Grafana wieder hinzugefügt.

| Metrik | Wert |
|--------|------|
| Revertete Commits | 18 |
| Verlorene Zeilen (netto) | ~+1633 / -176 |
| Wieder hergestellte Commits | 6 |
| **Noch fehlende Änderungen** | ~1450 Zeilen in 15 Dateien |

### 1.2 Legende

| Symbol | Bedeutung |
|--------|-----------|
| **KRITISCH** | RPC im Hot-Path – direkte Latenz |
| **VERSTOSS** | RPC wo Geyser-Daten vorhanden sein sollten |
| **SSOT** | Verletzung Single Source of Truth |
| **AKZEPTABEL** | Unvermeidlich (Simulation, TX-Send, Blockhash) |
| **COLD PATH** | RPC per Architektur erlaubt |

---

## 2. Neue Erkenntnisse aus arch-audit-tsw (2026-02-23)

### 2.1 CrossDexHandler: PumpFun ohne LivePoolCache ⚠️ KRITISCH/SSOT

**Datei:** `cross_dex_handler.rs` Zeile 206

```rust
let mut pumpfun = PumpFunDex::new(Arc::clone(&self.rpc), None)?;  // ← hardcodiert None!
```

**Problem:** CrossDexHandler hat `pool_cache: Option<Arc<LivePoolCache>>` und nutzt ihn für Raydium, PumpFunAmm, Meteora, Orca – **aber nicht für PumpFun Bonding Curve**. Beim Arb-Swap-Build für `pumpfun` wird daher immer RPC-Fallback für Creator ausgelöst (`get_account_retry` in `build_swap_ix_async`).

**SSOT-Verletzung:** Zwei Quellen für PumpFun Creator/Bonding-Curve – Cache (Momentum-Pfad) vs. RPC (Arb-Pfad).

**Fix:** `PumpFunDex::new(Arc::clone(&self.rpc), self.pool_cache.clone())`

**Priorität:** P1 – einfacher 1-Zeilen-Fix.

### 2.2 Geänderte Erkenntnisse vs. 2026-02-07

| Thema | 2026-02-07 | 2026-02-23 (tsw) |
|-------|------------|------------------|
| CrossDexHandler PumpFun Cache | Nicht erwähnt | **NEU:** PumpFun erhält keinen pool_cache |
| Meteora/Raydium CPMM quote_mint | BUG H offen | Implementiert (`extract_quote_mint`) |
| PumpFun SELL migriert | A.5 fehlt | ✅ Guard implementiert (real_reserves) |
| Raydium RPC-Retries | 20 × 500ms | 3 × 300ms (korrigiert) |

---

## 3. RPC im Hot Path

### 3.1 KRITISCH – DEX-Module

| Modul | Zeile | Call | Geyser-Alternative |
|-------|-------|------|---------------------|
| **pumpfun** | 309, 322 | fetch_bonding_curve, fetch_bonding_curve_fast | LivePoolCache |
| **pumpfun** | 1124 | get_account_retry (Creator) in build_swap_ix_async | LivePoolCache – **CrossDexHandler muss Cache übergeben** |
| **pumpfun_amm** | 308-310, 483, 542, 638, 659 | get_token_accounts, get_multiple_accounts | LivePoolCache (new_with_cache) |
| **orca** | 440 | get_multiple_accounts(vaults) | LivePoolCache |
| **orca** | 1372 | get_multiple_accounts(tick_arrays) | Geyser/Pre-Cache |
| **raydium** | 194 | load_pool_from_geyser (macht RPC!) | Geyser-Parse |
| **raydium** | 1276, 1336-1337 | get_account(market_id), get_token_account_balance | LivePoolCache |
| **raydium_cpmm** | 237-238 | get_account_retry(vault) | LivePoolCache |
| **meteora_dlmm** | 240, 269-270, 480 | get_account pool/reserves | LivePoolCache |

### 3.2 TX-Builder

| Zeile | Call | Status |
|-------|------|--------|
| 218 | fetch_orca_from_rpc | KRITISCH |
| 523, 1378 | load_pool_from_geyser (Raydium) | KRITISCH |
| 1518 | load_pool_by_address (Meteora) | KRITISCH |

### 3.3 Arbitrage – execution.rs

| Zeile | Call | Fix |
|-------|------|-----|
| 129 | get_balance_retry(wallet) | LockManager/Geyser-Wallet-Snapshot |

### 3.4 AKZEPTABEL

- execution_engine: simulate, send_transaction, get_latest_blockhash
- Liquidation, cleanup_wallet, Manual Burn, sell_all, market_data Bootstrap: COLD PATH
- account_janitor, wsol_manager: TX-Sending

### 3.5 Latenz-Auswirkung

- **Aktuell:** ~1,5–8 s | **Optimiert:** ~0,2–0,9 s | **Potenzial:** 3–8× schneller

---

## 4. Single Source of Truth

### 4.1 MASTER/SLAVE Pool-Cache ✅ Korrekt

```
market-data (MASTER) → Geyser → LivePoolCache → JetStream
                              ↓
        execution-engine, momentum-bot (SLAVE)
```

### 4.2 SSOT-Verletzungen

| Problem | Status |
|---------|--------|
| **CrossDexHandler: PumpFun ohne pool_cache** | ❌ SSOT verletzt – Arb-Pfad nutzt RPC statt Cache |
| Pool-Matching (FIX-38) | ✅ Eingehalten |
| PumpSwap quote_mint hardcodet | ⚠️ Potenziell bei non-SOL-Pools |
| Meteora/Raydium CPMM quote_mint | ✅ Behoben |

---

## 5. Logik-Bugs und Architektur-Probleme

| Bug | Status |
|-----|--------|
| **A** Killswitch-Liquidation Token übersprungen | Teilweise behoben |
| **B** load_pool_from_geyser macht RPC | ❌ Irreführender Name |
| **C** PumpFunAmm eigene RPC-Infrastruktur | ❌ new_with_cache existiert, RPC bei Miss |
| **D** Token-Decimals RPC | ✅ Cold Path only |
| **E** cleanup_wallet RPC | ✅ Akzeptiert (Cold Path) |
| **F** Orca 5-min TTL | ✅ Behoben (AUDIT-F) |
| **G** Ghost Open Positions | ✅ Behoben |
| **H** quote_mint hardcodet | ✅ Meteora/CPMM behoben, PumpSwap offen |
| **I** PumpFun SELL migriert | ✅ Behoben |

---

## 6. Bereits umgesetzte Audit-Implementierungen

| Item | Status |
|------|--------|
| AUDIT-F Orca Reserves | ✅ LivePoolCache einzige Quelle |
| AUDIT-E cleanup RPC | ✅ Akzeptiert by design |
| BUG D, F, G, I | ✅ Behoben |
| BUG H (Meteora/CPMM) | ✅ Behoben |
| PumpFunAmm new_with_cache | ✅ CrossDexHandler nutzt es für pump_amm |
| Liquidation Multi-Pool | ✅ Multi-Pool first, PumpFun last |
| PumpFun SELL Guard | ✅ real_reserves == 0 → Ok(None) |

---

## 7. Priorisierte Empfehlungen

### Priorität 1 – Sofort (1-Zeilen-Fix!)

| # | Problem | Fix |
|---|---------|-----|
| **1** | **CrossDexHandler: PumpFun ohne LivePoolCache** | `PumpFunDex::new(rpc, self.pool_cache.clone())` in cross_dex_handler.rs:206 |
| 2 | Arbitrage get_balance_retry | LockManager/available_sol vor RPC |

### Priorität 2 – Kurzfristig

| # | Aktion |
|---|--------|
| 3 | load_pool_from_geyser umbenennen |
| 4 | PumpFun Creator: LivePoolCache immer liefern (market-data) |
| 5 | Cache-capped min_out für PumpFun BUY (A.6) |

### Priorität 3 – Langfristig

| # | Aktion |
|---|--------|
| 6 | Raydium: Geyser-Account-Update direkt parsen |
| 7 | Orca Tick-Array: Geyser-Subscription |
| 8 | A.2, A.3: Creator-Handling, WSOL-Seeding zurückholen |

---

## Anhang: REVERT – Verlorene Änderungen (Kurzreferenz)

| Kategorie | Status |
|-----------|--------|
| A.1 PumpSwap Geyser-First | ⚠️ new_with_cache vorhanden, Revert hatte „fehlt" – Code hat sich evolutionär angepasst |
| A.2 Bonding-Curve Exit | ❌ Fehlt |
| A.3 Market-Data Wallet-Tracking | ❌ Fehlt |
| A.4 Liquidation 6005-Retry | ❌ Fehlt |
| A.5 PumpFun SELL migriert | ✅ Behoben |
| A.6 Cache-capped min_out | ❌ Fehlt |
| A.7 available_trading_capital_lamports | ❌ Fehlt |

---

## Referenzen

- `INVARIANTS.md`, `KNOWN_BUG_PATTERNS.md`, `ORDER_LIFECYCLE.md`
- `AUDIT_F_ORCA_RESERVES_IMPLEMENTATION_PLAN.md`, `AUDIT_E_IMPLEMENTATION_PLAN.md`
- `.cursor/rules/ironcrab-core.mdc`

---

*Konsolidiert: 2026-02-23 aus main (2026-02-07) + arch-audit-tsw (2026-02-23)*
