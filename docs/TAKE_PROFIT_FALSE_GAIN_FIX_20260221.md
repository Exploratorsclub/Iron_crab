# TAKE_PROFIT: Falsche Gains → realer Verlust — Root Cause & Fix

**Datum**: 2026-02-21  
**Symptom**: Dashboard zeigt TAKE_PROFIT mit „+200% gain“ in Detail, aber PnL (SOL) und PnL % sind negativ. Trades erfolgen oft innerhalb einer Sekunde nach Probe-Buy.

---

## 1. Root Cause

### Wrong-Pool Price Pollution

`current_price` (für `pnl_pct()`) wurde aus **jeder** Trade- und PoolCacheUpdate-Nachricht aktualisiert, **ohne zu prüfen, ob die Quelle denselben Pool nutzt wie die offene Position**.

Bei Tokens mit mehreren Pools (z.B. PumpFun Bonding Curve + PumpSwap AMM):

1. Probe-Buy auf **Bonding Curve** → Position mit `pool = bonding_curve_address`
2. **PoolCacheUpdate** oder **Trade** von **AMM** → anderes Reserve-/Trade-Ratio
3. `update_position_price(mint, tokens_per_sol)` überschreibt `current_price` mit AMM-Daten
4. AMM kann ein völlig anderes tokens_per_sol haben → falsches `current_price`
5. `pnl_pct = (entry/current - 1)*100` → fälschlich hoher Gain
6. TAKE_PROFIT feuert → Verkauf auf Bonding Curve → realer Verlust

### Timing

- TAKE_PROFIT kann sofort (≈1 s) nach Probe-Buy auslösen
- Kein Schutz gegen Preis-Updates von falschen Pools

---

## 2. Fix

### 2.1 Pool-Matching bei Preis-Updates

`update_position_price()` bekommt optional `source_pool: Option<&str>`:

- **Trade**: Es wird nur aktualisiert, wenn `position.pool == trade.pool_address`
- **PoolCacheUpdate**: Es wird nur aktualisiert, wenn `position.pool == update.pool_address`
- Bei leerem `position.pool` (Legacy) wird weiterhin aktualisiert (Rückwärtskompatibilität)

```rust
// momentum_bot.rs
fn update_position_price(..., source_pool: Option<&str>) {
    if let Some(pool) = source_pool {
        if !pos.pool.is_empty() && pos.pool != pool {
            trace!(... "Skipping price update: source pool != position pool");
            return;
        }
    }
    pos.update_price(new_price);
}
```

### 2.2 Mindest-Haltedauer für TAKE_PROFIT

Neuer Parameter: `take_profit_min_hold_secs` (Default: 5 s)

- TAKE_PROFIT wird erst ausgelöst, wenn `hold_secs >= take_profit_min_hold_secs`
- Verhindert frühe Fehl-Trigger durch kurzzeitige Preis-Spikes

---

## 3. Betroffene Dateien

- `src/bin/momentum_bot.rs`: Pool-Matching, `take_profit_min_hold_secs`
- `src/config.rs`: `take_profit_min_hold_secs` (Default 5)

---

## 4. Konfiguration

```toml
[momentum]
take_profit_min_hold_secs = 5   # Sekunden vor erstem TAKE_PROFIT (Default)
```

---

## 5. Referenzen

- `docs/TAKE_PROFIT_AND_TIMED_EXIT_ANALYSIS.md` — ältere Analyse
- `docs/BUGS_FIXES.md` — FIX-PNL (Formel-Inversion)
