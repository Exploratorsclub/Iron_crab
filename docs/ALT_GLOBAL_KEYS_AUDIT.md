# ALT Global Keys Audit — meteora_dlmm → pump_amm (2026-08-09)

Prod symptom: `tx_too_large:1245_bytes_max_1232`, `alt_hit_count=9`, `static_key_count=30`, `alt_configured=true`.

On-chain ALT (prod): `2J74oVKaviWzVr9gaLDvmBf3VAVVpzy88i3YCnyDwvuX`

EE loads **only** the on-chain ALT at startup (`load_alt`) — **no** runtime merge with `COMMON_ACCOUNTS`.

## Root cause (Phase 2)

| Finding | Explanation |
|---------|-------------|
| Low `alt_hit_count` | On-chain ALT missing several **global** keys that appear in every PumpSwap / Meteora bundle. Compiler can only use lookups for pubkeys present in the loaded table. |
| Not a compile bug | Keys **in** the loaded ALT but still static are expected: fee payer (signer), Jito tip (`exclude_tip_from_alt`), program IDs the v0 compiler keeps static. See `alt_in_table_but_static_count` in size-rejection logs. |
| Prod 1245 B vs 1111 B after fix | With corrected `COMMON_ACCOUNTS` and a fully extended on-chain ALT, the prod-pattern test (`cross_dex_meteora_pump_bundle_realistic_common_alt_size_audit`) serializes to **1111 B** (`alt_hit_count=12`, `static_key_count=26`). |

## COMMON_ACCOUNTS changes

| Pubkey | Action | Role |
|--------|--------|------|
| `ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw` | **Added** | PumpSwap `global_config` (swap ix account #2) — `PUMPFUN_AMM_GLOBAL_CONFIG` |
| `GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR` | **Added** | PumpSwap `__event_authority` PDA (swap ix #15) |
| `Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1` | **Added** | Pump.fun bonding-curve `event_authority` (other routes) |
| `39azUYFWPz3VHgKCf3VChUwbpURdCHRxjWVowf5jUJjg` | **Removed** | Was labeled “PumpSwap Global Config” — **not** `global_config` |
| `5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx` | Unchanged | PumpSwap fee config (already present) |
| `C2aFPdENg4A2HQsmrd5rTw5TaYBX5Ku887cWjbFKtZpw` | Unchanged | `global_volume_accumulator` PDA (already present) |

## On-chain ALT extend required (prod)

After merging this PR, extend ALT `2J74oVKaviWzVr9gaLDvmBf3VAVVpzy88i3YCnyDwvuX` with **at minimum** the new/changed globals:

```
ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw
GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR
Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1
```

Verify with audit (no TX):

```bash
cargo run --bin setup-alt -- \
  --rpc-url <RPC> \
  --alt-address 2J74oVKaviWzVr9gaLDvmBf3VAVVpzy88i3YCnyDwvuX \
  --audit-only
```

Then extend (cold path, user-operated):

```bash
cargo run --bin setup-alt -- \
  --rpc-url <RPC> \
  --keypair <AUTHORITY> \
  --alt-address 2J74oVKaviWzVr9gaLDvmBf3VAVVpzy88i3YCnyDwvuX
```

`setup-alt` adds all `COMMON_ACCOUNTS` not already on-chain. **Restart execution-engine** after extend so `load_alt` picks up new indices.

## Keys that must stay static (not ALT candidates)

- Wallet / fee payer (signer)
- User ATAs (per-wallet)
- Pool PDAs, vaults, bin arrays, oracle PDAs
- Jito tip account (filtered from ALT for writable static key — #373)

## Observability

Size rejection logs now include:

- `alt_in_table_but_static_count` / `alt_in_table_but_static` (top 10)
- `static_not_in_alt` (top 10) — use with `setup-alt --extra-addresses` only for **global** gaps, not per-pool keys

Metric: `arb_bundle_tx_too_large_total` (unchanged).

## Phase 3 (out of scope)

ATA as separate TX at opportunity detection — only if Phase 1+2 + on-chain extend still insufficient. Current audit shows **1111 B** with realistic ALT after global fix.
