# LivePoolCache — aktueller Stand

**Stand:** 2026-08-22

Das Januar-2026-Dokument war ein **Umbau-Plan (Option C)**. Der Plan ist umgesetzt; die Architektur ist **Geyser-Master in market-data**, nicht „execution-engine abonniert alle DEX-Programme selbst“.

## Datenfluss

```text
Yellowstone Geyser
        │
        ▼
market-data          MASTER: parse, admission, explicit Geyser-Set
        │  JetStream stream POOL_CACHE
        │  subject ironcrab.pool_cache.{pool}
        ▼
execution-engine     SLAVE LivePoolCache  → Plan / Quote / TX-Bau
arb-strategy         SLAVE LivePoolCache  → Quotes / Opportunities
```

- **Hot Path:** Quotes und TX-Bau aus dem Slave-Cache. RPC nur Cold Path (`allow_rpc_on_miss` / Liquidation / Bootstrap).
- **`src/execution/cache_geyser.rs`** existiert weiter (Reconnect, Mint-/Vault-Ergänzung). Der kanonische Pool-State für Strategien und EE kommt über **JetStream-Slave**, nicht über eine zweite volle DEX-Owner-Subscription in der Engine.
- Parser liegen in den DEX-Modulen und `live_pool_cache.rs` (`CachedPoolState` für Raydium AMM/CPMM, Orca, Meteora DLMM/CPMM, PumpFun, PumpSwap).

## API (Idee)

`LivePoolCache`: `get` / `upsert` / Readiness-Snapshots (Orca ticks, DLMM bins, …). `tx_builder` und `CrossDexHandler` injizieren Cache-State vor einem RPC-Fallback.

`QuoteCalculator` (`src/execution/quote_calculator.rs`) berechnet `min_out` aus dem Cache plus Slippage. Arb-Intents setzen oft nur `max_slippage_bps`; die Engine füllt `min_out` aus dem Cache.

## Invarianten

- I-4 / I-7: kein RPC im normalen Intent-Pfad.
- Cache-Miss im Hot Path ⇒ kein Send auf Rate-Limit/stale State, nicht „heimlich RPC“.
- Slot-/Fingerprint-Frische: Heartbeats ohne Material-Change dürfen `as_of_slot` nicht fälschen (Arb Quote Contract).

## Metriken (Beispiele)

Cache-Größe, Age, Miss, Geyser→Publish, Slave-Merge. Konkrete Namen in `/metrics` der jeweiligen Binaries, nicht die Januar-Wunschliste 1:1.

## Historischer Plan

Die abgehakten Phasen 1–4 unten im alten Sinne (Modul anlegen, TX-Builder umstellen, Quote, Intent-TTL) sind **erledigt**. Phase-5-Checkboxen „Staging dry_run“ sind Betrieb, kein offener Code-Umbau. Dieses File nicht als TODO-Liste lesen.
