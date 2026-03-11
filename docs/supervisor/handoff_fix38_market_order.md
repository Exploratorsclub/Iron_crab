# HANDOFF: FIX-38 Removal + Market Order + Sim Commitment

## Kontext
Drei zusammenhaengende Fixes:
1. Simulation nutzt Default Commitment "finalized", aber Geyser streamt auf "processed". Das erzeugt State-Lag bei neuen Tokens.
2. FIX-38 umgeht die Simulation bei bestimmten Fehlern. Ist zu aggressiv — sendet fehlerhafte TX on-chain.
3. Momentum BUY nutzt global:buy mit 3% Slippage. Momentum-Tokens bewegen sich schneller → Custom(6002) on-chain.

Siehe Plan: ../Iron_crab-eval/docs/plans/plan_fix38_removal_market_order.md

## Aufgabe 1: Simulation Commitment auf "processed" setzen

Datei: src/bin/execution_engine.rs, Funktion simulate_transaction() (ca. Zeile 8541)

Aktuell:
```rust
let cfg = RpcSimulateTransactionConfig {
    sig_verify: false,
    replace_recent_blockhash: true,
    ..RpcSimulateTransactionConfig::default()
};
```

Aendern zu:
```rust
let cfg = RpcSimulateTransactionConfig {
    sig_verify: false,
    replace_recent_blockhash: true,
    commitment: Some(solana_commitment_config::CommitmentConfig::processed()),
    ..RpcSimulateTransactionConfig::default()
};
```

Stelle sicher dass CommitmentConfig importiert ist.

## Aufgabe 2: FIX-38 Bypass KOMPLETT entfernen

Datei: src/bin/execution_engine.rs, in process_intent() (ca. Zeilen 7228-7288)

Den gesamten FIX-38 Block ersetzen. Nach `if !sim_result.success {` direkt:

```rust
if !sim_result.success {
    let reason = RejectReason::SimFailed;
    checks.push(CheckResult {
        check_name: "simulation".to_string(),
        passed: false,
        reason_code: Some(reason.to_string()),
        details: sim_result.error_code.clone(),
    });
    ctx.lock_manager.release_locks(&intent.intent_id);
    return emit_sim_failed_decision(
        ctx,
        decision_id,
        &intent,
        checks,
        plan_hash_str,
        sim_result,
    )
    .await;
}
```

Alle FIX-38 Variablen (sim_error, is_ata_create_failure_on_buy, is_pumpfun_sell_balance_lag) und den warn!-Aufruf KOMPLETT entfernen.

## Aufgabe 3: build_buy_exact_sol_ix() — Neue Market Order Funktion

Datei: src/solana/dex/pumpfun.rs

Neue pub Funktion in impl PumpFunDex, direkt nach build_buy_ix():

```rust
pub fn build_buy_exact_sol_ix(
    &self,
    token_mint: &Pubkey,
    bonding_curve: &Pubkey,
    associated_bonding_curve: &Pubkey,
    user_token_account: &Pubkey,
    creator: &Pubkey,
    token_program: &Pubkey,
    sol_amount: u64,
    min_tokens_out: u64,
) -> Result<Instruction>
```

Implementierung:
- Discriminator: `[56, 252, 116, 8, 158, 223, 205, 95]` (buy_exact_sol_in)
- Data Layout: discriminator(8) + sol_amount(8) + min_tokens_out(8)
- Account Layout: IDENTISCH zu build_buy_ix (alle 17 Accounts inkl. bonding_curve_v2)
- Kopiere die Account-Liste aus build_buy_ix 1:1

WICHTIG: Die Accounts sind EXAKT die gleichen wie bei build_buy_ix. Nur der Discriminator und die Data-Interpretation sind anders.

## Aufgabe 4: build_swap_ix_async_with_slippage — market_order Parameter

Datei: src/solana/dex/pumpfun.rs, Funktion build_swap_ix_async_with_slippage()

Neuen Parameter hinzufuegen: `market_order: bool` (als letzter Parameter)

Im buy_token Branch:

```rust
let ix = if buy_token {
    if market_order {
        info!(
            token_mint = %token_mint_str,
            sol_amount = amount_in,
            min_tokens_out = 1u64,
            "pump.fun MARKET ORDER BUY: exact SOL in, min tokens out = 1"
        );
        self.build_buy_exact_sol_ix(
            &token_mint,
            &bonding_curve,
            &associated_bonding_curve,
            &user_token_account,
            &creator,
            &token_program_sdk,
            amount_in,
            1,
        )?
    } else {
        // ... bestehender global:buy Code bleibt unveraendert ...
    }
} else {
    // ... bestehender SELL Code bleibt unveraendert ...
};
```

## Aufgabe 5: tx_builder — market_order aus Intent durchreichen

Datei: src/execution/tx_builder.rs

Vor dem Aufruf von build_swap_ix_async_with_slippage:
```rust
let market_order = intent.metadata.get("market_order").map(|v| v == "true").unwrap_or(false);
```

Und den Aufruf erweitern um `market_order` als letzten Parameter.

## Aufgabe 6: Momentum Bot — Market Order fuer BUY

Datei: src/bin/momentum_bot.rs

Bei BUY-Intent Erstellung (ca. Zeile 6505, wo metadata.insert gemacht wird):
```rust
intent.metadata.insert("market_order".to_string(), "true".to_string());
```

## Aufgabe 7: Execution Engine — Slippage-Check fuer Market Orders skippen

Datei: src/bin/execution_engine.rs, Check 3b (ca. Zeile 6400)

```rust
let is_market_order = intent.metadata.get("market_order").map(|v| v == "true").unwrap_or(false);
if intent.side == TradeSide::Sell || is_market_order {
    checks.push(CheckResult {
        check_name: "max_slippage".to_string(),
        passed: true,
        reason_code: None,
        details: Some(if is_market_order { "skipped_for_market_order".to_string() } else { "skipped_for_sell".to_string() }),
    });
} else {
    // bestehender Slippage-Check fuer Limit Orders (unveraendert)
}
```

## Aufgabe 8: Bestehende Tests aktualisieren

tests/execution_pumpfun_builder.rs:
- Falls build_swap_ix_async_with_slippage direkt aufgerufen wird: neuen market_order: false Parameter hinzufuegen
- Neuer Test: test_pumpfun_market_order_buy — Pruefe dass Discriminator [56, 252, 116, 8, 158, 223, 205, 95] ist

## Aufgabe 9: KNOWN_BUG_PATTERNS.md

Pattern #22: FIX-38 Simulation Bypass
Fix: FIX-38 entfernt. Simulation nutzt jetzt "processed" Commitment.

## Reihenfolge
1-9 wie oben, dann: cargo fmt, cargo clippy -- -D warnings, cargo test
