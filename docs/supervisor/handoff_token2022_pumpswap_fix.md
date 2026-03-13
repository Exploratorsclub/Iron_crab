# Handoff: Token-2022 PumpSwap Liquidation Fix (Bug #28, #29, #30)

## Kontext

Liquidation fuer 2 PumpSwap AMM Tokens scheitert:
1. `err_discovery` — pool_accounts werden durch PoolDiscovered Events überschrieben
2. `Custom(6023)` — build_swap_ix übergibt falsches Token Program für Token-2022
3. Liquidation Retry Scan ignoriert Token-2022 Accounts

Beide betroffenen Tokens sind Token-2022: `TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb`

## Aenderung 1: PoolDiscovered Merge-Logik (pool_cache_sync.rs)

**Datei:** `src/execution/pool_cache_sync.rs`
**Stelle:** `apply_pool_cache_update()`, `PoolCacheUpdateType::PoolDiscovered` Branch (ca. Zeile 274-280)

**Aktueller Code:**
```rust
PoolCacheUpdateType::PoolDiscovered => {
    if let Some((pool_addr, minimal_state)) = build_minimal_pool_state(update) {
        cache.upsert(pool_addr, minimal_state, update.geyser_slot);
        apply_decimals_from_metadata(cache, update);
        return true;
    }
}
```

**Neuer Code:**
```rust
PoolCacheUpdateType::PoolDiscovered => {
    if let Some((pool_addr, mut minimal_state)) = build_minimal_pool_state(update) {
        // P3 #28: Preserve pool_accounts and creator for pump_amm when PoolDiscovered
        // has empty pool_accounts. Same merge logic as BalanceUpdated (lines 316-324).
        // Without this, a Geyser re-scan of the pool account wipes previously populated
        // pool_accounts, causing "err_discovery" in Liquidation.
        if update.dex == "pump_amm" {
            if let Some(existing) = cache.get(&pool_addr) {
                if let (
                    CachedPoolState::PumpAmm(ref existing_pump),
                    CachedPoolState::PumpAmm(ref mut new_pump),
                ) = (&existing, &mut minimal_state)
                {
                    if new_pump.pool_accounts.is_empty()
                        && !existing_pump.pool_accounts.is_empty()
                    {
                        new_pump.pool_accounts = existing_pump.pool_accounts.clone();
                    }
                    if new_pump.creator.is_none() && existing_pump.creator.is_some() {
                        new_pump.creator = existing_pump.creator;
                    }
                }
            }
        }
        cache.upsert(pool_addr, minimal_state, update.geyser_slot);
        apply_decimals_from_metadata(cache, update);
        return true;
    }
}
```

**Wichtig:** Die Signatur von `build_minimal_pool_state(update)` ändert sich nicht. Nur `minimal_state` wird von immutable zu `mut` geändert.

## Aenderung 2: build_swap_ix Token-2022 Fix (pumpfun_amm.rs)

**Datei:** `src/solana/dex/pumpfun_amm.rs`
**Stelle:** `build_swap_ix()` Methode, Account-Liste (ca. Zeile 2382-2405)

**Aktueller Code (Zeile 2394-2395):**
```rust
AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 11
AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 12
```

**Neuer Code:**
```rust
AccountMeta::new_readonly(base_token_program, false), // 11 - base token program (Token-2022 aware)
AccountMeta::new_readonly(Pubkey::new_from_array(spl_token::id().to_bytes()), false), // 12 - quote token program (WSOL = always SPL)
```

**Hinweis:** Die Variable `base_token_program` existiert bereits (Zeile 2346-2350), wird aber aktuell nur für die ATA-Derivation genutzt. Account 12 bleibt `spl_token::id()` da WSOL immer SPL Token ist.

## Aenderung 3: Liquidation Retry Scan Token-2022 (execution_engine.rs)

**Datei:** `src/bin/execution_engine.rs`
**Stelle:** `run_liquidation_job()`, Retry-Diagnostic-Scan (ca. Zeile 2659-2665)

**Aktueller Code:**
```rust
let token_program_id = Pubkey::new_from_array(spl_token::id().to_bytes());
let retry_rpc_accounts = ctx
    .rpc
    .rpc
    .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
    .await
    .unwrap_or_default();
```

**Neuer Code:**
```rust
let token_program_id = Pubkey::new_from_array(spl_token::id().to_bytes());
let token_2022_program_id = Pubkey::new_from_array(spl_token_2022::id().to_bytes());
let mut retry_rpc_accounts = ctx
    .rpc
    .rpc
    .get_token_accounts_by_owner(&owner, TokenAccountsFilter::ProgramId(token_program_id))
    .await
    .unwrap_or_default();

if let Ok(mut accounts_2022) = ctx
    .rpc
    .rpc
    .get_token_accounts_by_owner(
        &owner,
        TokenAccountsFilter::ProgramId(token_2022_program_id),
    )
    .await
{
    retry_rpc_accounts.append(&mut accounts_2022);
}
```

## Invarianten

- A.33: PoolDiscovered darf pool_accounts nicht ueberschreiben
- A.34: build_swap_ix muss base_token_program für Token-2022 korrekt setzen
- A.35: Liquidation Retry Scan muss Token-2022 abdecken

## Erlaubte Dateien

- `src/execution/pool_cache_sync.rs`
- `src/solana/dex/pumpfun_amm.rs`
- `src/bin/execution_engine.rs`

## Verbotene Dateien

- Alles in `Iron_crab-eval/` (Tests sind Eval-Aufgabe)
- Keine neuen Dateien erstellen
