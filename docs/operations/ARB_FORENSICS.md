# Arb runtime forensics

The arb hot path remains Geyser-only. This instrumentation does not fetch accounts and does not
alter the slot, spread, profit, simulation, or execution gates.

## Bounded Prometheus metrics

`arb_round_trip_by_dex_pair_total{buy_dex,sell_dex,outcome}` splits every formable v2 route by a
fixed DEX vocabulary and terminal outcome. Unknown DEX names fold into `other`; addresses never
become labels.

`arb_pool_identity_check_total{dex,outcome}` proves whether Trade events pass the authoritative
LivePoolCache identity admission (`match`, `absent`, `mismatch`, or `invalid_pubkey`).

`arb_quote_invariant_violation_total{dex,invariant}` records checked-arithmetic contract failures.
Unknown invariants fold into `other`.

The existing v2 metrics remain authoritative for missing vaults, missing DLMM bins, stale state,
implausible reserves, quote freshness, and quote-pair slot deltas. Together with the new DEX-pair
split they separate account completeness from terminal economic gates without duplicating counters.

## Structured evidence

Terminal slot, spread, and arithmetic anomalies emit `kind="arb_round_trip_forensics"` at most once
per category per minute. Each event contains a deterministic `screen_id`, mint, decimals, probe,
buy/sell DEX and pool, both quote slots and outputs, slot delta, spread, and profit. This provides
exact pool identities and integer evidence without unbounded Prometheus cardinality.

Suggested query:

```bash
journalctl -u arb-strategy --since "30 minutes ago" -o cat \
  | grep 'kind="arb_round_trip_forensics"'
```

## Runtime proof procedure

1. Confirm identity mismatches are zero or inspect their corresponding authoritative-cache logs.
2. Rank terminal outcomes by DEX pair in Grafana.
3. For the dominant failing pair, collect its structured samples and compare raw quote amounts,
   slots, decimals, and pool identities.
4. Correlate missing-state outcomes with existing vault/bin/state-age metrics.
5. Fix only the demonstrated DEX parser, account delivery path, or quote implementation; do not
   relax the slot gate to mask stale state.

The funnel invariant is:

```text
formable = slot_delta + leg_too_old + spread_below + spread_above + profit_below + passed
           + arithmetic_invalid
```

## Deterministic pinned-pool quality cohort

`arb_pin_quality_cohort_member` selects pools with stable FNV-1a hashing at a fixed 1/16 rate.
Market-data and arb-strategy therefore trace the same bounded cohort without another topic, RPC,
or address label. Reconcile snapshots preserve the first pin timestamp in each process.

The stage sequence is:

```text
pin_published -> pin_received -> subscription -> master_update
              -> slave_update -> tracker_seeded -> quote_ready
```

- `arb_pin_quality_cohort_stage_pools{stage,outcome}` counts unique cohort pools per process.
- `arb_pin_quality_stage_latency_ms{stage}` measures first-pin to first-stage latency.
- `arb_pin_quality_slot_updates_total{outcome}` classifies every cohort update as `forward`,
  `duplicate`, or `regression`.
- Subscription outcomes distinguish missing cache layout, vault registration, and DLMM bins.
- Master/slave outcomes distinguish complete from zero-sided reserve state.
- Arb identity mismatches mark the affected cohort pool before admission is rejected.

Pool addresses are emitted only in existing deferred-registration logs and bounded
`kind="arb_pin_quality"` slot-regression warnings. A clean verification run requires no slot
regressions, no unexplained `missing_*` cohort pools after warmup, and convergence from
`pin_received` through `quote_ready`. The arb slot gate remains unchanged until that proof holds.
