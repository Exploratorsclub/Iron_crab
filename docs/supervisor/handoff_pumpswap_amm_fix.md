# Handoff: PumpSwap AMM Liquidation Fix (Bug #27 — Degenerate Cache Reserves)

## Kontext

Nach Restart/Deploy werden PumpSwap AMM Pools per Geyser mit `base_reserve=None, quote_reserve=None`
geparst. Vault-Balances kommen asynchron per Geyser-Subscription. Der PoolDiscovered-Event geht mit
`(0, 0)` an JetStream. Wenn nur ein Vault-Balance-Update vor der Liquidation ankommt, hat der
SLAVE-Cache z.B. `(691T tokens, 0 SOL)`.

In `pumpfun_amm.rs` `quote_exact_in()` liefert der Cache `Some((691T, 0))` — ein Cache-HIT.
`amount_out` berechnet sich zu 0 → Code gibt `Ok(None)` zurueck ohne den RPC-Fallback zu erreichen.

Siehe `docs/KNOWN_BUG_PATTERNS.md` Eintrag #27 fuer den vollstaendigen Kontext.

## Aenderung 1: `src/solana/dex/pumpfun_amm.rs` (KRITISCH)

In `quote_exact_in()` (~Zeile 2226): Wenn `amount_out == 0` und Cold Path (`self.allow_rpc_on_miss == true`),
zum RPC-Fallback durchfallen statt `Ok(None)` zurueckzugeben.

### Aktueller Code (Zeile ~2224-2249):
```rust
                let (amount_out, price_impact_bps) =
                    self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
                if amount_out == 0 {
                    return Ok(None);
                }

                debug!(
                    base_mint = %base_mint_str,
                    pool = %pool_market,
                    base_reserve = base_r,
                    quote_reserve = quote_r,
                    amount_out,
                    "pump_amm: quote from LivePoolCache (ZERO RPC)"
                );

                return Ok(Some(Quote {
                    amount_out,
                    price_impact_bps,
                    route: vec![pool_market.to_string()],
                    fee_bps: DEFAULT_TOTAL_FEE_BPS,
                    in_reserve,
                    out_reserve,
                    input_mint: input_mint.to_string(),
                    output_mint: output_mint.to_string(),
                    tick_spacing: None,
                }));
```

### Gewuenschter Code:
```rust
                let (amount_out, price_impact_bps) =
                    self.quote_cp(amount_in, in_reserve, out_reserve, DEFAULT_TOTAL_FEE_BPS);
                if amount_out == 0 {
                    if self.allow_rpc_on_miss {
                        warn!(
                            base_mint = %base_mint_str,
                            base_reserve = base_r,
                            quote_reserve = quote_r,
                            "pump_amm: cache reserves degenerate (one side=0), Cold Path falling through to RPC"
                        );
                        // Fall through to RPC fallback below
                    } else {
                        return Ok(None);
                    }
                } else {
                    debug!(
                        base_mint = %base_mint_str,
                        pool = %pool_market,
                        base_reserve = base_r,
                        quote_reserve = quote_r,
                        amount_out,
                        "pump_amm: quote from LivePoolCache (ZERO RPC)"
                    );

                    return Ok(Some(Quote {
                        amount_out,
                        price_impact_bps,
                        route: vec![pool_market.to_string()],
                        fee_bps: DEFAULT_TOTAL_FEE_BPS,
                        in_reserve,
                        out_reserve,
                        input_mint: input_mint.to_string(),
                        output_mint: output_mint.to_string(),
                        tick_spacing: None,
                    }));
                }
```

Die Aenderung bewegt die bestehende `return Ok(Some(Quote { ... }))` in einen `else`-Branch,
damit bei degenerate Reserves (eine Seite=0) der Code zum RPC-Fallback (Zeile ~2258) durchfaellt.

**Invariante I-7:** `allow_rpc_on_miss` ist nur im Cold Path `true`. Kein neuer RPC im Hot Path.

## Aenderung 2: `src/bin/market_data.rs` (WICHTIG)

Beim ersten Geyser-Parse eines PumpSwap AMM Pool-Accounts, wenn `base_reserve` und `quote_reserve`
noch `None` sind, die Vault-Balances sofort per RPC lesen.

### Aktueller Code (Zeile ~2586):
```rust
                            (s.base_mint, s.quote_mint, s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
```

### Gewuenschter Code:
Ersetze die Zeile mit einem Block, der bei fehlenden Reserves die Vault-Balances per RPC vorlaedt.
Die Variable `rpc` ist im Scope verfuegbar (Arc<SolanaRpc>).

```rust
                            {
                                let (base_r, quote_r) = if s.base_reserve.is_none() || s.quote_reserve.is_none() {
                                    let rpc_clone = Arc::clone(&rpc);
                                    let base_vault = s.pool_base_token_account;
                                    let quote_vault = s.pool_quote_token_account;
                                    let base_bal = rpc_clone.get_token_account_balance_u64(&base_vault).await.unwrap_or(0);
                                    let quote_bal = rpc_clone.get_token_account_balance_u64(&quote_vault).await.unwrap_or(0);
                                    if base_bal > 0 || quote_bal > 0 {
                                        info!(
                                            pool = %account_update.pubkey,
                                            base_vault = %base_vault,
                                            quote_vault = %quote_vault,
                                            base_bal,
                                            quote_bal,
                                            "pump_amm: pre-loaded vault balances via RPC (Cold Start Bootstrap)"
                                        );
                                    }
                                    (base_bal, quote_bal)
                                } else {
                                    (s.base_reserve.unwrap_or(0), s.quote_reserve.unwrap_or(0))
                                };
                                (s.base_mint, s.quote_mint, base_r, quote_r)
                            }
```

**HINWEIS:** Pruefe ob `SolanaRpc` eine Methode `get_token_account_balance_u64` hat. Falls nicht,
nutze eine Alternative: `get_account_opt_retry(&vault).await` und parse mit `try_parse_token_account_balance()`.
Die Funktion `try_parse_token_account_balance` ist in market_data.rs definiert (Zeile ~512) und
liefert `Option<u64>` aus Token-Account-Daten.

**WICHTIG:** Dieser Block ist innerhalb einer `match` arm expression fuer den Geyser account update handler.
Da der Kontext `async` ist (die aeussere Funktion `run_geyser_loop` ist async), sind `.await`-Calls erlaubt.
Pruefe aber, ob der umgebende `match`-Arm eine `.await`-kompatible Position hat. Falls nicht, nutze
`tokio::spawn` mit einem oneshot-Channel oder lies die Vault-Balances synchron aus der Geyser-Daten
(falls im selben Geyser-Batch verfuegbar).

## Nach allen Aenderungen

```bash
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test --quiet
```

## Referenzen

- `docs/KNOWN_BUG_PATTERNS.md` Eintrag #27
- Invariante I-7: Kein RPC im Hot Path
- `src/solana/dex/pumpfun_amm.rs` Zeile ~2208-2262 (quote_exact_in Cache+RPC path)
- `src/bin/market_data.rs` Zeile ~2504-2594 (PumpAmm Geyser handler)
