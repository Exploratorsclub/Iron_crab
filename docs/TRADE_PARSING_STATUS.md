# Trade Event Parsing Status

**Date**: 2026-01-13  
**Branch**: architecture-rebuild  
**Last Update**: PumpFun AMM account indices fixed

## Summary

Trade event parsing for all 6 DEXes has been implemented. ~~**critical bug discovered**: WSOL (wrapped SOL) amounts are not extracted correctly because WSOL is tracked as native lamports in `balances` array, NOT in `token_balances`.~~

**✅ BUG FIXED (2026-01-13)**: All DEX parsers now use `calculate_native_balance_change()` for SOL amounts on SELL operations.

## Status by DEX

| DEX | Implementation | Token Amount | SOL Amount | Notes |
|-----|---------------|--------------|------------|-------|
| Raydium AMM V4 | ✅ | ✅ | ✅ | Fixed: uses native balance for SELL |
| Orca Whirlpool | ✅ | ✅ | ✅ | Fixed: uses native balance for SELL |
| PumpFun Bonding Curve | ✅ | ✅ | ✅ | Already correct (uses native balance) |
| PumpFun AMM | ✅ | ✅ | ✅ | Already correct (uses native balance) |
| Meteora DLMM | ✅ | ✅ | ✅ | Fixed: uses native balance for SELL |
| Raydium CPMM | ✅ | ✅ | ✅ | Fixed: uses native balance for SELL |

## Bugs Fixed This Session

### Bug 1: Unused Variables with `?` Operator (CRITICAL)
**Commit**: 4458eea

**Problem**: 
- Lines 294-295 in `parse_raydium_swap()` had:
  ```rust
  let user_source = update.instruction_accounts.get(14).copied()?;
  let user_destination = update.instruction_accounts.get(15).copied()?;
  ```
- The `?` operator returned `None` if accounts didn't exist
- Variables were NEVER USED but presence of `?` caused ALL parsing to fail
- Result: 0 Trade Events generated after deployment

**Fix**: Removed unused variables completely

**Impact**: Fixed complete regression where no Trade events were emitted

### Bug 2: PumpFun AMM Quote Mint Confusion
**Commit**: 61a7092

**Problem**:
- `instruction_accounts[4]` was user's WSOL ATA (account), not the WSOL mint pubkey
- `calculate_token_balance_change()` searched for wrong pubkey in token_balances
- Result: `sol_amount=0` on all PumpFun AMM swaps

**Fix**: Hardcoded WSOL mint `So11111111111111111111111111111111111111112`

**Impact**: Partial fix, but still doesn't work (see Bug 3)

### Bug 3: WSOL Not in token_balances ✅ FIXED
**Status**: 🟢 FIXED (2026-01-13)

**Root Cause**:
- Geyser `token_balances` only contains SPL tokens
- WSOL (wrapped SOL) is tracked as native lamports in `balances` array (u64 array)
- `calculate_token_balance_change()` looks in `token_balances` → finds nothing → returns `None` → `.unwrap_or(0)` → `sol_amount=0`

**Fix Applied**:
All SELL paths now use `calculate_native_balance_change()` instead of `calculate_token_balance_change()`:

```rust
// SELL: SOL received is in native balances, not token_balances!
let sol_received = calculate_native_balance_change(
    &update.account_keys,
    &update.pre_balances,
    &update.post_balances,
    &trader,
)
.unwrap_or(0);
```

**DEXes Fixed**:
- ✅ Raydium AMM V4 (line ~335)
- ✅ Orca Whirlpool (line ~500)
- ✅ Meteora DLMM (line ~1080)
- ✅ Raydium CPMM (line ~1195)

**Already Correct** (no change needed):
- ✅ PumpFun BC (already used native balance)
- ✅ PumpFun AMM (already used native balance)

### Bug 4: PumpFun AMM Account Index Mismatch ✅ FIXED
**Status**: 🟢 FIXED (2026-01-13)

**Root Cause**:
- `dex_parser.rs` extracted `global_volume_accumulator` from instruction_accounts[19]
- But in real on-chain TXs (v2 format), index 19 is `fee_config`, not `global_volume_accumulator`
- The TX has only 21 accounts (0-20), so index 21/22 caused out-of-bounds or wrong values
- v2 TX format does NOT include `global_volume_accumulator` as an account

**Symptom**:
```
Custom(3007): AccountOwnedByWrongProgram
Account: global_volume_accumulator
Left: pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ (fee_program)
Right: pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA (expected pump AMM)
```

**Fix Applied**:
1. Updated account indices in `dex_parser.rs`:
   - `fee_config` = instruction_accounts[19] (was [21])
   - `fee_program` = instruction_accounts[20] (was [22])
2. Changed `pool_accounts` array to v2 format (12 accounts without `global_volume_accumulator`)
3. `build_swap_ix_from_pool_accounts` already handles v2 format correctly (reads fee_config from [11])

**New v2 pool_accounts format** (12 accounts):
```
[0] pool_market
[1] global_config
[2] base_mint
[3] quote_mint
[4] pool_base_vault
[5] pool_quote_vault
[6] protocol_fee_recipient
[7] protocol_fee_recipient_ta
[8] event_authority
[9] coin_creator_vault_ata
[10] coin_creator_vault_authority
[11] fee_config  ← fee_program is always derived from constant
```

## Current Implementation Details

### Helper Functions

#### `calculate_token_balance_change()`
```rust
fn calculate_token_balance_change(
    pre_balances: &[TokenBalance],
    post_balances: &[TokenBalance],
    mint: &Pubkey,
) -> Option<u64>
```

**Purpose**: Extract SPL token amount changes  
**Works for**: Base tokens (non-SOL)  
**NOT used for**: SOL amounts (use `calculate_native_balance_change` instead)

#### `calculate_native_balance_change()`
```rust
fn calculate_native_balance_change(
    account_keys: &[Pubkey],
    pre_balances: &[u64],
    post_balances: &[u64],
    account: &Pubkey,
) -> Option<u64>
```

**Purpose**: Extract native SOL balance changes  
**Works for**: SOL amounts on SELL operations  
**Finds**: Account index in `account_keys`, then computes `abs_diff` of balances

### Data Structures Available

From `GeyserTransactionUpdate`:
```rust
pub struct GeyserTransactionUpdate {
    pub signature: String,
    pub slot: u64,
    pub account_keys: Vec<Pubkey>,
    pub instruction_accounts: Vec<Pubkey>,
    pub instruction_data: Vec<u8>,
    pub pre_token_balances: Vec<TokenBalance>,  // ✅ SPL tokens
    pub post_token_balances: Vec<TokenBalance>, // ✅ SPL tokens
    pub pre_balances: Vec<u64>,                 // ✅ Used for SOL!
    pub post_balances: Vec<u64>,                // ✅ Used for SOL!
}
```

## Test Plan (After Deployment)

1. **Deploy** market-data with the fix
2. **Monitor** Trade events for 2 minutes
3. **Verify** recent Trade events have BOTH amounts > 0:
   ```bash
   ssh ironcrab-prod "tail -100 ~/Iron_crab/trade_logs/market_events/market_events-*.jsonl | grep Trade" | \
   jq 'select(.sol_amount > 0 and .token_amount > 0)'
   ```
4. **Check** arb-strategy finds opportunities:
   ```bash
   ssh ironcrab-prod "sudo journalctl -u arb-strategy --since '2 minutes ago' | grep opportunity"
   ```
5. **Sample** each DEX type has valid Trade events

## Deployment History

- **03:15:25** - Deployed with unused variable bug → 0 Trade Events
- **03:47:10** - Fixed unused variables → Trade Events appear but amounts=0
- **2026-01-13** - Fixed WSOL extraction bug → All DEXes should now have correct amounts

## Related Files

- `src/solana/dex_parser.rs` - All DEX parsers
- `src/solana/geyser_listener.rs` - Geyser transaction updates
- `trade_logs/market_events/*.jsonl` - Output validation

## Next Steps

1. ✅ Document current status (this file)
2. ✅ Implement `calculate_native_balance_change()` - already existed
3. ✅ Update all DEX parsers to use it
4. ⏳ Deploy and test
5. ⏳ Verify arbitrage detection works
5. ⏳ Verify arbitrage detection works
