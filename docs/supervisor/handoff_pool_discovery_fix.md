# Handoff: Pool Discovery & Bootstrap Fix (Impl Agent)

**Plan-Referenz:** `Iron_crab-eval/docs/plans/plan_fix_pool_discovery_and_bootstrap.md`

## Aufgabe

Implementiere die Fixes A, B, C, D aus dem Plan. Reihenfolge einhalten.

## Fix A: getTokenAccountsByOwner entfernen (pumpfun_amm.rs)

**Datei:** `src/solana/dex/pumpfun_amm.rs`
**Stelle:** Zeile 550-590, Closure `find_authority_with_existing_token_account`

### Aenderung

1. Den "Slow path" Block (Zeile 576-587) entfernen — `find_any_token_account_for_owner_and_mint` darf NICHT mehr aufgerufen werden
2. Stattdessen: Wenn ATA nicht on-chain existiert, ATA-Adresse trotzdem ableiten und zurueckgeben
3. Fuer WSOL (quote_mint = `So11111111111111111111111111111111111111112`): immer SPL Token (`TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA`) verwenden
4. Log-Warnung wenn ATA nicht existiert aber abgeleitet wird

### Pseudocode
```rust
// Ersetze den gesamten Closure-Body:
let find_authority_with_token_account = |candidates: Vec<Pubkey>, mint: Pubkey| async move {
    for cand in candidates {
        // 1) ATA on-chain pruefen (getAccountInfo, braucht keinen Index)
        for tp in [token_program, token_2022_program] {
            let ata = Self::derive_ata_with_program(cand, mint, tp);
            if let Some((ata_owner, ata_exec, ata_data)) =
                self.rpc_get_account_owner_executable_and_data(ata).await?
            {
                if !ata_exec && (ata_owner == token_program || ata_owner == token_2022_program) {
                    if let Some((ata_mint, ata_token_owner)) =
                        Self::parse_spl_token_account_mint_and_owner(&ata_data)
                    {
                        if ata_mint == mint && ata_token_owner == cand {
                            return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((cand, ata)));
                        }
                    }
                }
            }
        }
        // 2) ATA existiert nicht on-chain → ableiten und trotzdem verwenden
        //    PumpSwap erstellt sie via CreateIdempotent waehrend des Swaps.
        //    Fuer WSOL ist das Token-Program immer SPL Token.
        if mint == expected_quote_mint {
            let ata = Self::derive_ata_with_program(cand, mint, token_program);
            warn!(
                candidate = %cand,
                mint = %mint,
                derived_ata = %ata,
                "pump_amm: ATA not on-chain, using derived address (PumpSwap will create via CreateIdempotent)"
            );
            return Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(Some((cand, ata)));
        }
    }
    Ok::<Option<(Pubkey, Pubkey)>, anyhow::Error>(None)
};
```

**WICHTIG:** `expected_quote_mint` ist bereits als lokale Variable definiert (Zeile 399). Sicherstellen dass der Closure Zugriff darauf hat (ggf. als Parameter oder `move`).

### Pruefung
- `cargo check` muss fehlerfrei sein
- Kein Aufruf von `find_any_token_account_for_owner_and_mint` mehr in diesem Kontext
- `find_token_account_by_owner_and_mint` und `find_any_token_account_for_owner_and_mint` Funktionen NICHT loeschen (werden woanders genutzt, z.B. `execution_engine.rs`)

---

## Fix B: JetStream pool_accounts Persistenz (market_data.rs)

**Datei:** `src/bin/market_data.rs`
**Stelle:** Nach Zeile 3417 (`ctx.live_pool_cache.set_pump_amm_pool_accounts(...)`)

### Aenderung

Nach dem Setzen der pool_accounts im MASTER Cache, ein zusaetzliches `PoolCacheUpdate` auf JetStream publizieren:

```rust
// Nach Zeile 3418:
// FIX-33: Publish updated PoolCacheUpdate to JetStream with pool_accounts metadata
// so that bootstrap after restart has pool_accounts for inactive pools.
if let Some(ref nats) = ctx.nats {
    let mut meta = std::collections::HashMap::new();
    let accounts_str: Vec<String> = pool_accounts.iter().map(|p| p.to_string()).collect();
    meta.insert("pool_accounts".to_string(), accounts_str.join(","));
    // base_mint is at index 2, quote_mint at index 3 in pool_accounts v1 order
    let base_mint_str = pool_accounts.get(2).map(|p| p.to_string()).unwrap_or_default();
    let pool_cache_update = PoolCacheUpdate {
        pool_address: pool_address.to_string(),
        dex: "pump_amm".to_string(),
        update_type: PoolCacheUpdateType::PoolDiscovered,
        base_reserve: None,
        quote_reserve: None,
        base_mint: Some(base_mint_str),
        quote_mint: Some(pool_accounts.get(3).map(|p| p.to_string()).unwrap_or_default()),
        geyser_slot: Some(tx_update.slot),
        metadata: Some(meta),
    };
    if let Err(e) = nats.publish_pool_cache_update(&pool_cache_update).await {
        warn!(error = %e, pool = %pool_address, "FIX-33: Failed to publish PoolCacheUpdate with pool_accounts to JetStream");
    }
}
```

Gleiche Logik auch nach Zeile 3318 (create_pool Pfad) einfuegen.

**WICHTIG:** Pruefe ob `PoolCacheUpdate` und `publish_pool_cache_update` korrekt importiert/verfuegbar sind. Schaue in der bestehenden Codebasis wie PoolCacheUpdates publiziert werden und verwende das gleiche Pattern.

---

## Fix C: Liquidation-Timeout (execution_engine.rs)

**Datei:** `src/bin/execution_engine.rs`
**Stellen:** Zeile 1645 und 2020

### Aenderung
```rust
// Zeile 1645: Duration::from_secs(10) → Duration::from_secs(45)
// Zeile 2020: Duration::from_secs(10) → Duration::from_secs(45)
```

---

## Fix D: Startup pool_accounts Seeding (execution_engine.rs)

**Datei:** `src/bin/execution_engine.rs`
**Stelle:** Nach dem Bootstrap-Aufruf `bootstrap_pool_cache_from_jetstream()`

### Aenderung

Nach dem JetStream-Bootstrap, iteriere alle PumpSwap Pools im SLAVE Cache die keine pool_accounts haben. Fuer jeden, versuche `discover_pool_static(base_mint)` aufzurufen (das nutzt jetzt Fix A und den FIX-31 Fast Path).

```rust
// Nach bootstrap_pool_cache_from_jetstream():
// FIX-33: Proactively seed pool_accounts for PumpSwap pools without them
if let Some(ref pump_amm) = pump_amm_dex {
    let pools_without_accounts = live_pool_cache.get_pump_amm_pools_without_accounts();
    if !pools_without_accounts.is_empty() {
        info!(
            count = pools_without_accounts.len(),
            "Startup: seeding pool_accounts for PumpSwap pools via getAccountInfo"
        );
        for (pool_addr, base_mint) in &pools_without_accounts {
            match pump_amm.discover_pool_static_public(*base_mint).await {
                Ok(Some(pool_static)) => {
                    // pool_accounts are now in pump_amm.pools_by_base cache
                    // Also update LivePoolCache
                    let accounts = pool_static.to_pool_accounts_vec();
                    live_pool_cache.set_pump_amm_pool_accounts(pool_addr, accounts);
                }
                Ok(None) => {
                    warn!(pool = %pool_addr, base_mint = %base_mint, "Startup seeding: pool not found on-chain");
                }
                Err(e) => {
                    warn!(pool = %pool_addr, base_mint = %base_mint, error = %e, "Startup seeding: discovery failed");
                }
            }
        }
    }
}
```

**WICHTIG:**
- `get_pump_amm_pools_without_accounts()` muss auf LivePoolCache implementiert werden — gibt Vec<(Pubkey, Pubkey)> zurueck (pool_addr, base_mint) fuer alle PumpAmm Eintraege mit leeren pool_accounts
- `discover_pool_static` ist private — ggf. eine public Wrapper-Methode `discover_pool_static_public` oder direkt `try_parse_pool_static_from_market_account` nutzen
- `PumpAmmPoolStatic::to_pool_accounts_vec()` muss die 14 Accounts in der richtigen Reihenfolge zurueckgeben

---

## Hinweise

- Lies `docs/INVARIANTS.md` und `KNOWN_BUG_PATTERNS.md` VOR der Implementierung
- `cargo check` nach jeder Aenderung
- Keine Aenderungen an Tests (Iron_crab-eval)
