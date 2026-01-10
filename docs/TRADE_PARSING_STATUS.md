# Trade Event Parsing Status

**Date**: 2026-01-10  
**Branch**: architecture-rebuild  
**Commit**: 61a7092

## Summary

Trade event parsing for all 6 DEXes has been implemented, but **critical bug discovered**: WSOL (wrapped SOL) amounts are not extracted correctly because WSOL is tracked as native lamports in `balances` array, NOT in `token_balances`.

## Status by DEX

| DEX | Implementation | Token Amount | SOL Amount | Notes |
|-----|---------------|--------------|------------|-------|
| Raydium AMM V4 | ✅ | ❓ | ❓ | Code complete, untested |
| Orca Whirlpool | ✅ | ❓ | ❓ | Code complete, untested |
| PumpFun Bonding Curve | ✅ | ❓ | ❓ | Code complete, untested |
| PumpFun AMM | ✅ | ✅ | ❌ | Token amounts work, SOL=0 |
| Meteora DLMM | ✅ | ❓ | ❓ | Code complete, untested |
| Raydium CPMM | ✅ | ❓ | ❓ | Code complete, untested |

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

### Bug 3: WSOL Not in token_balances (ACTIVE)
**Status**: 🔴 NOT FIXED

**Root Cause**:
- Geyser `token_balances` only contains SPL tokens
- WSOL (wrapped SOL) is tracked as native lamports in `balances` array (u64 array)
- `calculate_token_balance_change()` looks in `token_balances` → finds nothing → returns `None` → `.unwrap_or(0)` → `sol_amount=0`

**Evidence**:
```json
{"kind":"Trade","sol_amount":0,"token_amount":13557664492537,"is_buy":false}
{"kind":"Trade","sol_amount":0,"token_amount":0,"is_buy":true}
{"kind":"Trade","sol_amount":636371517,"token_amount":0,"is_buy":true}
```

**Impact**: 
- PumpFun AMM: SOL amounts = 0 (token amounts work)
- Raydium/Orca/Meteora/CPMM: Likely same issue (untested)
- Arbitrage detection impossible without accurate amounts

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
**Fails for**: WSOL/SOL amounts  
**Reason**: WSOL not in `token_balances`

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
    pub pre_balances: Vec<u64>,                 // ❌ Need this for SOL!
    pub post_balances: Vec<u64>,                // ❌ Need this for SOL!
}
```

## Required Fix

### New Helper Function Needed

```rust
/// Calculate native SOL balance change for a specific account
/// Returns absolute change in lamports
fn calculate_native_balance_change(
    pre_balances: &[u64],
    post_balances: &[u64],
    account_index: usize,
) -> Option<u64> {
    let pre = pre_balances.get(account_index)?;
    let post = post_balances.get(account_index)?;
    
    // For SELL: post > pre (user receives SOL)
    // For BUY: pre > post (user spends SOL)
    Some(post.abs_diff(*pre))
}
```

### Changes Required

All DEX parsers that handle SOL/WSOL need to use native balance extraction:

1. **PumpFun AMM** (SELL): Use `calculate_native_balance_change()` for sol_received
2. **PumpFun Bonding Curve** (SELL): Same fix needed
3. **Raydium AMM** (both): May need same fix if quote is WSOL
4. **Orca** (both): May need same fix if quote is WSOL
5. **Meteora DLMM** (both): May need same fix
6. **Raydium CPMM** (both): May need same fix

### Account Index Determination

Need to find user's account index in `account_keys`:
- PumpFun AMM: user = `instruction_accounts[1]`
- Find index in `update.account_keys` where key == user
- Use that index for `pre_balances[index]` / `post_balances[index]`

## Test Plan (After Fix)

1. **Deploy** with native balance extraction
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
- **Current** - Identified WSOL extraction issue, implementing fix

## Related Files

- `src/solana/dex_parser.rs` - All DEX parsers
- `src/solana/geyser_listener.rs` - Geyser transaction updates
- `trade_logs/market_events/*.jsonl` - Output validation

## Next Steps

1. ✅ Document current status (this file)
2. 🔄 Implement `calculate_native_balance_change()`
3. 🔄 Update all DEX parsers to use it
4. ⏳ Deploy and test
5. ⏳ Verify arbitrage detection works
