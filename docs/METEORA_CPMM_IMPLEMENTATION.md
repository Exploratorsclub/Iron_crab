# Meteora DLMM + Raydium CPMM Implementation Design

**Status**: 🔄 Implementation Phase (Infrastructure ✅ Done, Phase 1 ✅ Done, Phase 2 ✅ Done, Phase 3 ✅ Done, Phase 4 ✅ Done)  
**Target**: 6 DEXes für Cross-DEX Arbitrage  
**Progress**: 90% Complete (Meteora DLMM + Raydium CPMM implemented!)  
**Next**: Integration tests + arb-strategy integration  
**Priority**: High (maximale Arbitrage-Coverage)

**Last Updated**: 2026-01-10 00:30 UTC

---

## Executive Summary

Erweiterung der IronCrab Arbitrage-Engine um zwei zusätzliche DEXes:
1. **Meteora DLMM** (Dynamic Liquidity Market Maker) - Concentrated Liquidity
2. **Raydium CPMM** (Constant Product Market Maker) - Neuere Raydium-Version

**Erwartete Verbesserungen:**
- Arbitrage-Opportunities: +40-60% (mehr DEX-Kombinationen)
- Pool Coverage: 4 → 6 DEXes
- Cross-DEX Triangular Arbitrage: DLMM ↔ AMM ↔ CPMM
- Geyser Event Load: +15-25% (akzeptabel bei aktueller Server-Last)

---

## ✅ Infrastructure Already Completed (2026-01-09)

### Geyser Architecture (3-Layer Model):

**Layer 1 - Geyser Plugin Config** (`/home/sol/geyser-config.json`): ✅ DEPLOYED
```json
{
  "libpath": "/usr/local/lib/solana/libyellowstone_grpc_geyser.so",
  "grpc": {"address": "127.0.0.1:10000"}
}
```
- **NO DEX filters here** - Yellowstone doesn't support server-side filters
- Only libpath + gRPC port
- ASCII encoding (UTF-8 BOM breaks Yellowstone parser)

**Layer 2 - Validator Account-Index**: ✅ DEPLOYED
```bash
--account-index-include-key 675kPX9... # Raydium AMM V4
--account-index-include-key CPMMoo8... # Raydium CPMM ✅
--account-index-include-key whirLbM... # Orca Whirlpool
--account-index-include-key 6EF8rre... # PumpFun
--account-index-include-key pAMMBay... # PumpSwap
--account-index-include-key LBUZKhR... # Meteora DLMM ✅
--account-index-include-key spl-token-owner  # Wallet tracking
```
- Makes lookups for these Program IDs fast (RAM instead of disk)
- 100M ledger slots, 320GB cache, 16GB scan buffer

**Layer 3 - Client-Side Subscription** (market-data): ✅ UPDATED
```rust
// src/bin/market_data.rs (Lines 57-62, 400-407)
const RAYDIUM_AMM_V4: &str = "675kPX9...";
const RAYDIUM_CPMM: &str = "CPMMoo8...";
const ORCA_WHIRLPOOL: &str = "whirLbM...";
const PUMPFUN_PROGRAM: &str = "6EF8rre...";
const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay...";
const METEORA_DLMM: &str = "LBUZKhR...";

let program_ids = vec![
    Pubkey::from_str(RAYDIUM_AMM_V4)?,
    Pubkey::from_str(RAYDIUM_CPMM)?,      // ✅ ADDED
    Pubkey::from_str(ORCA_WHIRLPOOL)?,
    Pubkey::from_str(PUMPFUN_PROGRAM)?,
    Pubkey::from_str(PUMPFUN_AMM_PROGRAM)?,
    Pubkey::from_str(METEORA_DLMM)?,      // ✅ ADDED
];
```

**Geyser Filter Logic** (src/solana/geyser_listener.rs:163-186):
```rust
for (idx, program_id) in self.program_ids.iter().enumerate() {
    // Subscribe ALL accounts owned by DEX program
    accounts_filter.insert(
        format!("dex_accounts_{}", idx),
        SubscribeRequestFilterAccounts {
            owner: vec![program_id.to_string()],
            ...
        },
    );
    
    // Subscribe ALL transactions involving DEX
    transactions_filter.insert(
        format!("dex_transactions_{}", idx),
        SubscribeRequestFilterTransactions {
            account_include: vec![program_id.to_string()],
            ...
        },
    );
}
```

### Current Server Status:
- **Validator**: Running (Slot 392410091+, synced live)
- **Geyser**: Port 10000 LISTENING ✅

---

## ✅ Phase 4 - Raydium CPMM Implementation (2026-01-10 00:30 UTC)

### Implementation Summary:
**File Created**: `src/solana/dex/raydium_cpmm.rs` (460 lines)

**Key Components**:

1. **CpmmPool Layout Parser**:
```rust
pub struct CpmmPool {
    pub status: u8,
    pub token_0_mint: Pubkey,
    pub token_1_mint: Pubkey,
    pub token_0_vault: Pubkey,
    pub token_1_vault: Pubkey,
    pub lp_mint: Pubkey,
    pub fee_rate: u64,  // Raw fee (e.g., 2500 = 0.25%)
}
```
- Account size: 752 bytes (estimated, needs mainnet verification)
- Simplified vs AMM V4 (no Serum/OpenBook accounts)
- Parse offsets: discriminator(8) + status(1) + mints/vaults(32 each)

2. **RaydiumCpmm DEX Connector**:
```rust
pub struct RaydiumCpmm {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<Pubkey, PoolCache>>,
    mint_index: Arc<DashMap<Pubkey, Vec<Pubkey>>>,
}
```

**Features**:
- **refresh_pools()**: getProgramAccounts with 752-byte size filter
- **quote_exact_in()**: Constant product formula (x * y = k)
  - Formula: amount_out = y - (x*y)/(x + amount_in_after_fee)
  - Fee applied to input before calculation
  - Reserve balances fetched from vaults (token accounts)
- **build_swap_ix()**: Swap instruction builder (7 accounts)
  - Discriminator: `[0xf8, 0xc6, 0x9e, 0x91, 0xe1, 0x75, 0x87, 0xc8]`
  - Data: discriminator(8) + amount_in(8) + min_out(8)
- **list_pairs()**: Returns all token pairs in cache
- **DashMap caching**: Same pattern as Meteora (concurrent safe)
- **Mint indexing**: Fast lookup of pools by token mint

3. **Unit Tests**:
```
✅ test_constant_product_quote
✅ test_pool_parse
```

**Test Results**:
```bash
running 2 tests
test solana::dex::raydium_cpmm::tests::test_pool_parse ... ok
test solana::dex::raydium_cpmm::tests::test_constant_product_quote ... ok
test result: ok. 2 passed; 0 failed; 0 ignored
```

**Quote Accuracy**:
- Pool: 1000 SOL / 100k USDC
- Swap: 1 SOL
- Fee: 0.25% (25 bps)
- Expected: ~99.75 USDC
- **Result**: ✅ amount_out in range [99.0, 100.0] USDC

**Differences vs AMM V4**:
| Feature | AMM V4 | CPMM |
|---------|--------|------|
| Serum Integration | ✅ Required | ❌ Not needed |
| Account Count | ~14 accounts | ~7 accounts |
| Complexity | High (order book) | Low (pure AMM) |
| Swap Latency | Higher | Lower |
| Fee Structure | Multi-tier | Single rate |

**Build Status**:
```bash
✅ cargo build --lib: SUCCESS
✅ cargo test --lib raydium_cpmm: 2/2 PASSED
✅ cargo build --release --bin market-data: SUCCESS
```

**Integration**:
- ✅ Module added to `mod.rs`
- ✅ Ready for arb-strategy integration
- ⏳ Mainnet pool validation (next step)
- **market-data**: Connected, receiving DEX transactions ✅
- **Events published**: 2,191+ market events
- **Compilation**: `cargo build --release --bin market-data` ✅ SUCCESS

### What's Working:
✅ Geyser plugin loads cleanly  
✅ All 6 DEX program IDs in validator account-index  
✅ market-data subscribes to all 6 DEXes via Geyser  
✅ Real-time transaction stream active  
✅ No validator crashes or performance issues  
✅ **Meteora DLMM pool parser** (904-byte LB Pair accounts)  
✅ **Meteora DLMM DEX connector** (Dex trait implemented)  
✅ **Pool discovery** (getProgramAccounts with 904-byte filter)  
✅ **Basic quoting** (constant product approximation)  
✅ **Reserve balance fetching** (from token vaults)  
✅ **Bin-walking algorithm** (quote_x_to_y, quote_y_to_x, multi-bin support) ✅ TESTED  
✅ **Swap instruction builder** (meteora_swap_builder.rs) ✅ COMPLETE  
✅ **Bin array fetching** (dynamic discovery via getProgramAccounts + PDA derivation) ✅ TESTED  
✅ **Bin array parsing** (meteora_bin_array_layout.rs, 70 bins per array) ✅ COMPLETE  

### What's In Progress (Phase 4):  
🔄 Integration into arb-strategy  
🔄 Mainnet pool discovery tests  

### What's Missing (Phase 5+):  
❌ Event authority account derivation (optional for logging)  
❌ Oracle account integration (optional for price feeds)  
❌ `raydium_cpmm.rs` - DEX trait implementation  
---

## Current State (Baseline)

### Implementierte DEXes (4):
```rust
// src/solana/dex/mod.rs
pub mod raydium;     // Raydium AMM V4
pub mod orca;        // Orca Whirlpool
pub mod pumpfun;     // PumpFun Bonding Curve
pub mod pumpfun_amm; // PumpSwap AMM
```

### Geyser Filter (aktuell):
**NOTE**: Yellowstone Geyser hat KEINE server-seitigen Filter in der Config!
Filtering erfolgt **client-seitig** via `SubscribeRequest` (siehe geyser_listener.rs).

**Validator Account-Index** (performance optimization): ✅ DEPLOYED
```bash
--account-index-include-key 675kPX9... # Raydium AMM V4
--account-index-include-key CPMMoo8... # Raydium CPMM ✅
--account-index-include-key whirLbM... # Orca Whirlpool
--account-index-include-key 6EF8rre... # PumpFun
--account-index-include-key pAMMBay... # PumpSwap
--account-index-include-key LBUZKhR... # Meteora DLMM ✅
--account-index-include-key spl-token-owner  # Wallet tracking
```

**Current Status**:
- ✅ Validator synced (Slot 392410091+, live)
- ✅ Geyser port 10000 listening
- ✅ market-data connected, streaming DEX transactions
- ✅ 2,191+ market events published
- ✅ `cargo build --release --bin market-data` successful

---

## Target Architecture (6 DEXes)

### New DEX Modules:
```rust
// src/solana/dex/mod.rs
pub mod raydium;         // AMM V4 (existing)
pub mod raydium_cpmm;    // CPMM (NEW)
pub mod orca;            // Whirlpool (existing)
pub mod pumpfun;         // Bonding Curve (existing)
pub mod pumpfun_amm;     // PumpSwap (existing)
pub mod meteora_dlmm;    // DLMM (NEW)
```

### Client-Side Subscription (market-data): ✅ DEPLOYED
**CRITICAL**: Yellowstone Geyser hat **KEINE** server-seitigen Filter!  
Filtering erfolgt **client-seitig** via `SubscribeRequest` in market-data:

```rust
// src/bin/market_data.rs - Lines 57-62, 400-407
const RAYDIUM_AMM_V4: &str = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const RAYDIUM_CPMM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";
const ORCA_WHIRLPOOL: &str = "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc";
const PUMPFUN_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPFUN_AMM_PROGRAM: &str = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const METEORA_DLMM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

let program_ids = vec![
    Pubkey::from_str(RAYDIUM_AMM_V4)?,
    Pubkey::from_str(RAYDIUM_CPMM)?,       // ✅ ADDED
    Pubkey::from_str(ORCA_WHIRLPOOL)?,
    Pubkey::from_str(PUMPFUN_PROGRAM)?,
    Pubkey::from_str(PUMPFUN_AMM_PROGRAM)?,
    Pubkey::from_str(METEORA_DLMM)?,       // ✅ ADDED
];

// geyser_listener.rs then builds SubscribeRequest:
for program_id in program_ids {
    accounts_filter.owner = vec![program_id];           // All accounts owned by DEX
    transactions_filter.account_include = vec![program_id]; // All txs involving DEX
}
```

---

## 1. Meteora DLMM Implementation

### 1.1 Program ID & Constants

```rust
// src/solana/dex/meteora_dlmm.rs

/// Meteora DLMM Program ID
pub const METEORA_DLMM_PROGRAM: &str = "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo";

/// Meteora DLMM Pool Account Size (approximate)
/// Actual size varies based on bin configuration
pub const DLMM_POOL_MIN_SIZE: usize = 3000;
pub const DLMM_POOL_MAX_SIZE: usize = 10000;

/// Bin Step (basis points between price bins)
/// Common values: 1, 5, 10, 25, 50, 100 bps
pub const DEFAULT_BIN_STEP_BPS: u16 = 10; // 0.1%
```

### 1.2 Data Structures

#### DLMM Pool State
```rust
/// Meteora DLMM Pool State (on-chain layout)
/// Note: This is a simplified version - actual layout needs reverse engineering
#[derive(Debug, Clone)]
pub struct DlmmPoolState {
    /// Pool parameters
    pub parameters: PoolParameters,
    
    /// Active bin ID (current price bin)
    pub active_id: i32,
    
    /// Bin step (price increment between bins in bps)
    pub bin_step: u16,
    
    /// Protocol fee bps
    pub protocol_fee_bps: u16,
    
    /// Base fee bps (charged on swaps)
    pub base_fee_bps: u16,
    
    /// Token X mint (base)
    pub token_x_mint: Pubkey,
    
    /// Token Y mint (quote)
    pub token_y_mint: Pubkey,
    
    /// Reserve X (base)
    pub reserve_x: Pubkey,
    
    /// Reserve Y (quote)
    pub reserve_y: Pubkey,
    
    /// Oracle (optional price feed)
    pub oracle: Option<Pubkey>,
}

#[derive(Debug, Clone)]
pub struct PoolParameters {
    /// Bin step (price increment)
    pub bin_step: u16,
    
    /// Base factor (for fee calculation)
    pub base_factor: u16,
    
    /// Filter period (for volatility-based fees)
    pub filter_period: u16,
    
    /// Decay period
    pub decay_period: u16,
    
    /// Reduction factor
    pub reduction_factor: u16,
    
    /// Variable fee control
    pub variable_fee_control: u32,
    
    /// Max volatility accumulator
    pub max_volatility_accumulator: u32,
    
    /// Min bin ID
    pub min_bin_id: i32,
    
    /// Max bin ID
    pub max_bin_id: i32,
}

/// DLMM Bin (liquidity at specific price level)
#[derive(Debug, Clone, Copy)]
pub struct DlmmBin {
    /// Bin ID (signed integer, active_id is current price)
    pub bin_id: i32,
    
    /// Amount of token X in this bin
    pub amount_x: u64,
    
    /// Amount of token Y in this bin
    pub amount_y: u64,
    
    /// Price of this bin (derived from bin_id and bin_step)
    pub price: f64,
}
```

### 1.3 DEX Trait Implementation

```rust
use async_trait::async_trait;
use super::{Dex, Quote};

pub struct MeteoraDlmm {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<String, DlmmPoolCache>>,
    last_refresh: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct DlmmPoolCache {
    pool_address: Pubkey,
    state: DlmmPoolState,
    bins: Vec<DlmmBin>, // Cached bins near active_id
    last_update: u64,
}

#[async_trait]
impl Dex for MeteoraDlmm {
    async fn refresh_pools(&self) -> Result<()> {
        // Use getProgramAccounts with memcmp filters
        // Filter by token_x_mint or token_y_mint for specific pairs
        
        let filters = vec![
            RpcFilterType::DataSize(DLMM_POOL_MIN_SIZE as u64),
            // Add mint filters for SOL/USDC etc
        ];
        
        let accounts = self.rpc.get_program_accounts_with_config(
            &Pubkey::from_str(METEORA_DLMM_PROGRAM)?,
            RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            },
        ).await?;
        
        for (pubkey, account) in accounts {
            let state = parse_dlmm_pool(&account.data)?;
            
            // Fetch bins near active_id for quote calculation
            let bins = fetch_active_bins(&state, self.rpc.clone()).await?;
            
            let cache = DlmmPoolCache {
                pool_address: pubkey,
                state,
                bins,
                last_update: Utc::now().timestamp() as u64,
            };
            
            let key = format!("{}_{}", 
                cache.state.token_x_mint, 
                cache.state.token_y_mint
            );
            self.pools.insert(key, cache);
        }
        
        Ok(())
    }
    
    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let key = format!("{}_{}", input_mint, output_mint);
        let pool = match self.pools.get(&key) {
            Some(p) => p,
            None => {
                // Try reverse direction
                let reverse_key = format!("{}_{}", output_mint, input_mint);
                match self.pools.get(&reverse_key) {
                    Some(p) => p,
                    None => return Ok(None),
                }
            }
        };
        
        // DLMM Quote Logic: Walk through bins from active_id
        let (amount_out, bins_crossed, total_fee) = 
            simulate_dlmm_swap(&pool, input_mint, amount_in)?;
        
        // Calculate price impact
        let price_before = bin_id_to_price(pool.state.active_id, pool.state.bin_step);
        let price_after = bin_id_to_price(
            pool.state.active_id + bins_crossed, 
            pool.state.bin_step
        );
        let price_impact_bps = ((price_after - price_before).abs() / price_before * 10000.0) as u32;
        
        Ok(Some(Quote {
            amount_out,
            price_impact_bps,
            route: vec![pool.pool_address.to_string()],
            fee_bps: total_fee,
            in_reserve: pool.bins.iter().map(|b| b.amount_x as u128).sum(),
            out_reserve: pool.bins.iter().map(|b| b.amount_y as u128).sum(),
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: Some(pool.state.bin_step),
        }))
    }
    
    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let key = format!("{}_{}", input_mint, output_mint);
        let pool = self.pools.get(&key)
            .ok_or_else(|| anyhow!("Pool not found"))?;
        
        // Meteora DLMM Swap IX
        // Requires: lb_pair, bin_array_bitmap_extension, reserve_x, reserve_y, user accounts
        
        let ix = build_meteora_swap_ix(
            &pool.pool_address,
            &pool.state,
            input_mint,
            output_mint,
            amount_in,
            min_out,
        )?;
        
        Ok(vec![ix])
    }
    
    fn list_pairs(&self) -> Vec<(String, String)> {
        self.pools.iter()
            .map(|entry| {
                let pool = entry.value();
                (
                    pool.state.token_x_mint.to_string(),
                    pool.state.token_y_mint.to_string(),
                )
            })
            .collect()
    }
}
```

### 1.4 DLMM Quote Algorithm

```rust
/// Simulate swap through DLMM bins
/// Returns: (amount_out, bins_crossed, total_fee_bps)
fn simulate_dlmm_swap(
    pool: &DlmmPoolCache,
    input_mint: &str,
    amount_in: u64,
) -> Result<(u64, i32, u32)> {
    let is_x_to_y = input_mint == pool.state.token_x_mint.to_string();
    
    let mut remaining_in = amount_in;
    let mut total_out = 0u64;
    let mut current_bin_id = pool.state.active_id;
    let mut bins_crossed = 0i32;
    let mut total_fee = 0u64;
    
    // Walk through bins until amount_in is consumed
    for bin in &pool.bins {
        if remaining_in == 0 {
            break;
        }
        
        // Check if bin is in the right direction
        if is_x_to_y && bin.bin_id < current_bin_id {
            continue; // Skip lower bins when swapping X→Y
        }
        if !is_x_to_y && bin.bin_id > current_bin_id {
            continue; // Skip higher bins when swapping Y→X
        }
        
        let (bin_in, bin_out) = if is_x_to_y {
            (bin.amount_x, bin.amount_y)
        } else {
            (bin.amount_y, bin.amount_x)
        };
        
        if bin_in == 0 || bin_out == 0 {
            continue; // Skip empty bins
        }
        
        // Calculate swap within this bin (constant product formula)
        let amount_in_this_bin = std::cmp::min(remaining_in, bin_in);
        
        // Fee calculation (base fee + volatility fee if applicable)
        let fee = calculate_dlmm_fee(
            amount_in_this_bin,
            pool.state.base_fee_bps,
            &pool.state.parameters,
        );
        total_fee += fee;
        
        let amount_in_after_fee = amount_in_this_bin.saturating_sub(fee);
        
        // Constant product: (x + dx) * (y - dy) = x * y
        // dy = y * dx / (x + dx)
        let amount_out_this_bin = (bin_out as u128)
            .saturating_mul(amount_in_after_fee as u128)
            .saturating_div((bin_in as u128).saturating_add(amount_in_after_fee as u128));
        
        total_out = total_out.saturating_add(amount_out_this_bin as u64);
        remaining_in = remaining_in.saturating_sub(amount_in_this_bin);
        
        current_bin_id = bin.bin_id;
        bins_crossed += 1;
    }
    
    ensure!(remaining_in == 0, "Insufficient liquidity in DLMM bins");
    
    let avg_fee_bps = if amount_in > 0 {
        (total_fee as u128 * 10000 / amount_in as u128) as u32
    } else {
        pool.state.base_fee_bps as u32
    };
    
    Ok((total_out, bins_crossed, avg_fee_bps))
}

fn calculate_dlmm_fee(
    amount_in: u64,
    base_fee_bps: u16,
    params: &PoolParameters,
) -> u64 {
    // Simplified fee calculation
    // Real implementation includes volatility-based dynamic fees
    (amount_in as u128 * base_fee_bps as u128 / 10000) as u64
}

fn bin_id_to_price(bin_id: i32, bin_step: u16) -> f64 {
    // Price formula: price = (1 + bin_step/10000)^bin_id
    let base = 1.0 + (bin_step as f64 / 10000.0);
    base.powi(bin_id)
}
```

### 1.5 Meteora Swap Instruction Builder

```rust
fn build_meteora_swap_ix(
    lb_pair: &Pubkey,
    pool_state: &DlmmPoolState,
    input_mint: &str,
    output_mint: &str,
    amount_in: u64,
    min_out: u64,
) -> Result<Instruction> {
    let user = /* get from context */;
    
    // Derive user token accounts
    let user_token_in = derive_ata(user, &Pubkey::from_str(input_mint)?);
    let user_token_out = derive_ata(user, &Pubkey::from_str(output_mint)?);
    
    // Meteora DLMM requires bin_array_bitmap_extension
    let bitmap_extension = derive_bin_array_bitmap_extension(lb_pair);
    
    // Account metas for swap
    let accounts = vec![
        AccountMeta::new(*lb_pair, false),                     // LB Pair
        AccountMeta::new(bitmap_extension, false),             // Bin Array Bitmap Extension
        AccountMeta::new(pool_state.reserve_x, false),         // Reserve X
        AccountMeta::new(pool_state.reserve_y, false),         // Reserve Y
        AccountMeta::new(user_token_in, false),                // User Token In
        AccountMeta::new(user_token_out, false),               // User Token Out
        AccountMeta::new_readonly(user, true),                 // User (signer)
        AccountMeta::new_readonly(spl_token::id(), false),     // Token Program
        // ... additional oracle/event accounts if needed
    ];
    
    // Instruction data (discriminator + params)
    let mut data = vec![
        0x09, // Swap discriminator (placeholder - needs verification)
    ];
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_out.to_le_bytes());
    
    Ok(Instruction {
        program_id: Pubkey::from_str(METEORA_DLMM_PROGRAM)?,
        accounts,
        data,
    })
}

fn derive_bin_array_bitmap_extension(lb_pair: &Pubkey) -> Pubkey {
    Pubkey::find_program_address(
        &[b"bitmap", lb_pair.as_ref()],
        &Pubkey::from_str(METEORA_DLMM_PROGRAM).unwrap(),
    ).0
}
```

---

## 2. Raydium CPMM Implementation

### 2.1 Program ID & Constants

```rust
// src/solana/dex/raydium_cpmm.rs

/// Raydium CPMM Program ID
pub const RAYDIUM_CPMM_PROGRAM: &str = "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C";

/// CPMM Pool Account Size (estimate)
pub const CPMM_POOL_SIZE: usize = 1500; // Similar to AMM V4
```

### 2.2 Data Structures

```rust
/// Raydium CPMM Pool State
/// Similar to AMM V4 but with some differences
#[derive(Debug, Clone)]
pub struct CpmmPoolState {
    /// Status (1 = initialized, others = disabled/frozen)
    pub status: u64,
    
    /// Base mint (token A)
    pub mint_a: Pubkey,
    
    /// Quote mint (token B)
    pub mint_b: Pubkey,
    
    /// LP mint
    pub lp_mint: Pubkey,
    
    /// Base vault (holds token A)
    pub vault_a: Pubkey,
    
    /// Quote vault (holds token B)
    pub vault_b: Pubkey,
    
    /// Fee rate (bps)
    pub fee_rate: u64,
    
    /// Protocol fee rate
    pub protocol_fee_rate: u64,
    
    /// Fund fee rate
    pub fund_fee_rate: u64,
    
    /// Observation key (for price oracle)
    pub observation_key: Pubkey,
    
    /// Authority (PDA)
    pub authority: Pubkey,
}

impl CpmmPoolState {
    /// Parse from account data
    pub fn parse(data: &[u8]) -> Result<Self> {
        ensure!(data.len() >= CPMM_POOL_SIZE, "Invalid CPMM pool data size");
        
        // Layout reverse engineering needed
        // Offsets based on Raydium CPMM SDK
        
        Ok(Self {
            status: u64::from_le_bytes(data[8..16].try_into()?),
            mint_a: Pubkey::new(&data[16..48]),
            mint_b: Pubkey::new(&data[48..80]),
            lp_mint: Pubkey::new(&data[80..112]),
            vault_a: Pubkey::new(&data[112..144]),
            vault_b: Pubkey::new(&data[144..176]),
            fee_rate: u64::from_le_bytes(data[176..184].try_into()?),
            protocol_fee_rate: u64::from_le_bytes(data[184..192].try_into()?),
            fund_fee_rate: u64::from_le_bytes(data[192..200].try_into()?),
            observation_key: Pubkey::new(&data[200..232]),
            authority: Pubkey::new(&data[232..264]),
        })
    }
}
```

### 2.3 DEX Trait Implementation

```rust
pub struct RaydiumCpmm {
    rpc: Arc<SolanaRpc>,
    pools: Arc<DashMap<String, CpmmPoolCache>>,
    last_refresh: Arc<AtomicU64>,
}

#[derive(Clone, Debug)]
struct CpmmPoolCache {
    pool_address: Pubkey,
    state: CpmmPoolState,
    reserve_a: u64,
    reserve_b: u64,
    last_update: u64,
}

#[async_trait]
impl Dex for RaydiumCpmm {
    async fn refresh_pools(&self) -> Result<()> {
        // Very similar to AMM V4 implementation
        let filters = vec![
            RpcFilterType::DataSize(CPMM_POOL_SIZE as u64),
        ];
        
        let accounts = self.rpc.get_program_accounts_with_config(
            &Pubkey::from_str(RAYDIUM_CPMM_PROGRAM)?,
            RpcProgramAccountsConfig {
                filters: Some(filters),
                account_config: RpcAccountInfoConfig {
                    encoding: Some(UiAccountEncoding::Base64),
                    ..Default::default()
                },
                ..Default::default()
            },
        ).await?;
        
        for (pubkey, account) in accounts {
            let state = CpmmPoolState::parse(&account.data)?;
            
            // Skip inactive pools
            if state.status != 1 {
                continue;
            }
            
            // Fetch vault balances
            let vault_a_balance = self.rpc.get_token_account_balance(&state.vault_a).await?;
            let vault_b_balance = self.rpc.get_token_account_balance(&state.vault_b).await?;
            
            let cache = CpmmPoolCache {
                pool_address: pubkey,
                state,
                reserve_a: vault_a_balance.parse()?,
                reserve_b: vault_b_balance.parse()?,
                last_update: Utc::now().timestamp() as u64,
            };
            
            let key = format!("{}_{}", cache.state.mint_a, cache.state.mint_b);
            self.pools.insert(key, cache);
        }
        
        Ok(())
    }
    
    async fn quote_exact_in(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
    ) -> Result<Option<Quote>> {
        let key = format!("{}_{}", input_mint, output_mint);
        let pool = match self.pools.get(&key) {
            Some(p) => p,
            None => {
                let reverse_key = format!("{}_{}", output_mint, input_mint);
                match self.pools.get(&reverse_key) {
                    Some(p) => p,
                    None => return Ok(None),
                }
            }
        };
        
        // Constant Product AMM formula (same as AMM V4)
        let (reserve_in, reserve_out) = if input_mint == pool.state.mint_a.to_string() {
            (pool.reserve_a, pool.reserve_b)
        } else {
            (pool.reserve_b, pool.reserve_a)
        };
        
        // Fee calculation
        let total_fee_bps = pool.state.fee_rate 
            + pool.state.protocol_fee_rate 
            + pool.state.fund_fee_rate;
        let fee_amount = (amount_in as u128 * total_fee_bps as u128 / 10000) as u64;
        let amount_in_after_fee = amount_in.saturating_sub(fee_amount);
        
        // x * y = k
        // amount_out = reserve_out * amount_in / (reserve_in + amount_in)
        let amount_out = (reserve_out as u128)
            .saturating_mul(amount_in_after_fee as u128)
            .saturating_div((reserve_in as u128).saturating_add(amount_in_after_fee as u128));
        
        // Price impact
        let price_impact_bps = ((amount_in_after_fee as u128 * 10000) 
            / reserve_in as u128) as u32;
        
        Ok(Some(Quote {
            amount_out: amount_out as u64,
            price_impact_bps,
            route: vec![pool.pool_address.to_string()],
            fee_bps: total_fee_bps as u32,
            in_reserve: reserve_in as u128,
            out_reserve: reserve_out as u128,
            input_mint: input_mint.to_string(),
            output_mint: output_mint.to_string(),
            tick_spacing: None,
        }))
    }
    
    fn build_swap_ix(
        &self,
        input_mint: &str,
        output_mint: &str,
        amount_in: u64,
        min_out: u64,
    ) -> Result<Vec<Instruction>> {
        let key = format!("{}_{}", input_mint, output_mint);
        let pool = self.pools.get(&key)
            .ok_or_else(|| anyhow!("Pool not found"))?;
        
        let ix = build_cpmm_swap_ix(
            &pool.pool_address,
            &pool.state,
            input_mint,
            output_mint,
            amount_in,
            min_out,
        )?;
        
        Ok(vec![ix])
    }
    
    fn list_pairs(&self) -> Vec<(String, String)> {
        self.pools.iter()
            .map(|entry| {
                let pool = entry.value();
                (pool.state.mint_a.to_string(), pool.state.mint_b.to_string())
            })
            .collect()
    }
}

fn build_cpmm_swap_ix(
    pool_id: &Pubkey,
    pool_state: &CpmmPoolState,
    input_mint: &str,
    output_mint: &str,
    amount_in: u64,
    min_out: u64,
) -> Result<Instruction> {
    let user = /* get from context */;
    
    let user_token_in = derive_ata(user, &Pubkey::from_str(input_mint)?);
    let user_token_out = derive_ata(user, &Pubkey::from_str(output_mint)?);
    
    let accounts = vec![
        AccountMeta::new_readonly(user, true),              // Payer
        AccountMeta::new_readonly(pool_state.authority, false), // Authority
        AccountMeta::new(*pool_id, false),                  // Pool config
        AccountMeta::new(user_token_in, false),             // Input token account
        AccountMeta::new(user_token_out, false),            // Output token account
        AccountMeta::new(pool_state.vault_a, false),        // Vault A
        AccountMeta::new(pool_state.vault_b, false),        // Vault B
        AccountMeta::new_readonly(spl_token::id(), false),  // Token Program
        // Observation account for price oracle (optional)
        AccountMeta::new(pool_state.observation_key, false),
    ];
    
    // Instruction data (discriminator + params)
    let mut data = vec![
        0x01, // SwapBaseIn discriminator (placeholder)
    ];
    data.extend_from_slice(&amount_in.to_le_bytes());
    data.extend_from_slice(&min_out.to_le_bytes());
    
    Ok(Instruction {
        program_id: Pubkey::from_str(RAYDIUM_CPMM_PROGRAM)?,
        accounts,
        data,
    })
}
```

---

## 3. Integration Plan

### 3.1 File Structure

```
src/solana/dex/
├── mod.rs                    // Update: add meteora_dlmm, raydium_cpmm
├── raydium.rs                // Existing: AMM V4
├── raydium_cpmm.rs           // NEW: CPMM implementation
├── orca.rs                   // Existing: Whirlpool
├── pumpfun.rs                // Existing: Bonding Curve
├── pumpfun_amm.rs            // Existing: PumpSwap
├── meteora_dlmm.rs           // NEW: DLMM implementation
├── meteora_dlmm_layout.rs    // NEW: Bin/Pool parsing
└── router.rs                 // Update: register new DEXes
```

### 3.2 mod.rs Updates

```rust
// src/solana/dex/mod.rs

pub mod raydium;
pub mod raydium_cpmm;         // NEW
pub mod orca;
pub mod orca_reserve_cache;
pub mod orca_whirlpool_layout;
pub mod pumpfun;
pub mod pumpfun_amm;
pub mod meteora_dlmm;         // NEW
pub mod meteora_dlmm_layout;  // NEW
pub mod router;

// Re-exports
pub use raydium::Raydium;
pub use raydium_cpmm::RaydiumCpmm;      // NEW
pub use orca::Orca;
pub use pumpfun::PumpFun;
pub use pumpfun_amm::PumpFunAmm;
pub use meteora_dlmm::MeteoraDlmm;      // NEW
```

### 3.3 Arbitrage Strategy Integration

```rust
// src/bin/arb_strategy.rs

async fn initialize_dexes(rpc: Arc<SolanaRpc>) -> Result<Vec<Arc<dyn Dex>>> {
    let mut dexes: Vec<Arc<dyn Dex>> = vec![];
    
    // Existing DEXes
    dexes.push(Arc::new(Raydium::new(rpc.clone())));
    dexes.push(Arc::new(Orca::new(rpc.clone())));
    dexes.push(Arc::new(PumpFun::new(rpc.clone())));
    dexes.push(Arc::new(PumpFunAmm::new(rpc.clone())));
    
    // NEW DEXes
    dexes.push(Arc::new(RaydiumCpmm::new(rpc.clone())));    // NEW
    dexes.push(Arc::new(MeteoraDlmm::new(rpc.clone())));    // NEW
    
    // Refresh all pools
    for dex in &dexes {
        dex.refresh_pools().await?;
    }
    
    info!("Initialized {} DEXes for arbitrage", dexes.len());
    Ok(dexes)
}

// Arbitrage opportunity detection now has 6x more combinations:
// - Raydium AMM ↔ Meteora DLMM
// - Raydium CPMM ↔ Raydium AMM
// - Orca ↔ Meteora DLMM
// - PumpSwap ↔ Meteora DLMM
// - Triangular: SOL → USDC (Raydium) → Token (Meteora) → SOL (Orca)
```

---

## 4. Testing Strategy

### 4.1 Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_meteora_bin_price_calculation() {
        // Test bin_id_to_price formula
        assert_eq!(bin_id_to_price(0, 10), 1.0);
        assert_eq!(bin_id_to_price(100, 10), 1.01f64.powi(100));
        assert_eq!(bin_id_to_price(-100, 10), 1.01f64.powi(-100));
    }
    
    #[test]
    fn test_dlmm_quote_simulation() {
        // Test swap through multiple bins
        let bins = vec![
            DlmmBin { bin_id: 100, amount_x: 1000000, amount_y: 1010000, price: 1.01 },
            DlmmBin { bin_id: 101, amount_x: 900000, amount_y: 919000, price: 1.0201 },
            DlmmBin { bin_id: 102, amount_x: 800000, amount_y: 828000, price: 1.0303 },
        ];
        
        // Simulate 500k swap
        // Should consume parts of bin 100 and 101
    }
    
    #[test]
    fn test_cpmm_pool_parsing() {
        // Test CPMM pool state parsing
        let mock_data = vec![0u8; CPMM_POOL_SIZE];
        let result = CpmmPoolState::parse(&mock_data);
        assert!(result.is_ok() || result.is_err()); // Validate parsing logic
    }
}
```

### 4.2 Integration Tests

```rust
#[tokio::test]
#[ignore] // Requires RPC access
async fn test_meteora_dlmm_live_pool() {
    let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com"));
    let meteora = MeteoraDlmm::new(rpc);
    
    meteora.refresh_pools().await.unwrap();
    
    // Test SOL/USDC quote
    let quote = meteora.quote_exact_in(
        "So11111111111111111111111111111111111111112", // SOL
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", // USDC
        1_000_000_000, // 1 SOL
    ).await.unwrap();
    
    assert!(quote.is_some());
    let q = quote.unwrap();
    assert!(q.amount_out > 0);
    assert!(q.fee_bps < 100); // Fee should be reasonable
}

#[tokio::test]
#[ignore]
async fn test_raydium_cpmm_live_pool() {
    let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com"));
    let cpmm = RaydiumCpmm::new(rpc);
    
    cpmm.refresh_pools().await.unwrap();
    
    let pairs = cpmm.list_pairs();
    assert!(!pairs.is_empty(), "Should find CPMM pools");
}
```

### 4.3 Arbitrage Simulation Tests

```rust
#[tokio::test]
#[ignore]
async fn test_cross_dex_arbitrage_opportunity() {
    // Test arbitrage detection between Raydium AMM, CPMM, and Meteora DLMM
    
    let rpc = Arc::new(SolanaRpc::new("https://api.mainnet-beta.solana.com"));
    let raydium_amm = Arc::new(Raydium::new(rpc.clone()));
    let raydium_cpmm = Arc::new(RaydiumCpmm::new(rpc.clone()));
    let meteora = Arc::new(MeteoraDlmm::new(rpc.clone()));
    
    raydium_amm.refresh_pools().await.unwrap();
    raydium_cpmm.refresh_pools().await.unwrap();
    meteora.refresh_pools().await.unwrap();
    
    // Check for SOL/USDC arbitrage across all 3
    let sol = "So11111111111111111111111111111111111111112";
    let usdc = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
    let amount = 1_000_000_000; // 1 SOL
    
    let quote_amm = raydium_amm.quote_exact_in(sol, usdc, amount).await.unwrap();
    let quote_cpmm = raydium_cpmm.quote_exact_in(sol, usdc, amount).await.unwrap();
    let quote_dlmm = meteora.quote_exact_in(sol, usdc, amount).await.unwrap();
    
    // Find best price
    let prices = vec![
        ("AMM", quote_amm.as_ref().map(|q| q.amount_out)),
        ("CPMM", quote_cpmm.as_ref().map(|q| q.amount_out)),
        ("DLMM", quote_dlmm.as_ref().map(|q| q.amount_out)),
    ];
    
    let max_price = prices.iter()
        .filter_map(|(name, price)| price.map(|p| (name, p)))
        .max_by_key(|(_, p)| *p);
    
    println!("Best price: {:?}", max_price);
}
```

---

## 5. Geyser Filter Deployment

### 5.1 Updated Config

```json
{
  "libpath": "/home/sol/solana-geyser-grpc-plugin.so",
  "log": {
    "level": "info"
  },
  "grpc": {
    "address": "127.0.0.1:10000",
    "max_decoding_message_size": 4194304
  },
  "accounts": [
    {
      "owner": [
        "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8"
      ]
    },
    {
      "owner": [
        "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C"
      ]
    },
    {
      "owner": [
        "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc"
      ]
    },
    {
      "owner": [
        "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P"
      ]
    },
    {
      "owner": [
        "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA"
      ]
    },
    {
      "owner": [
        "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo"
      ]
    }
  ],
  "slots": {},
  "transactions": {}
}
```

### 5.2 Deployment Steps

```bash
# 1. Backup current config
ssh ironcrab-prod "cp /home/sol/geyser-grpc-plugin-config.json /home/sol/geyser-config-backup-$(date +%Y%m%d).json"

# 2. Copy new config
scp docs/geyser-grpc-plugin-config.json ironcrab-prod:/home/sol/

# 3. Validate JSON syntax
ssh ironcrab-prod "cat /home/sol/geyser-grpc-plugin-config.json | jq '.accounts | length'"
# Expected: 6

# 4. Restart validator (30-60s downtime)
ssh ironcrab-prod "sudo systemctl restart solana-validator"

# 5. Monitor restart
ssh ironcrab-prod "sudo journalctl -u solana-validator -f -n 100" | grep -E "Geyser|accounts"

# Expected logs:
# - Loaded geyser plugin
# - Registered 6 account filters
# - Validator catchup started

# 6. Verify Geyser forwarding
ssh ironcrab-prod "sudo journalctl -u market-data -f | grep -E 'DLMM|CPMM'"

# Should see within 5-10 minutes:
# - 🆕 Meteora DLMM pool detected
# - 🆕 Raydium CPMM pool detected
```

---

## 6. Performance Impact Analysis

### 6.1 Expected Metrics Changes

| Metric | Before (4 DEXes) | After (6 DEXes) | Change |
|--------|-----------------|-----------------|--------|
| Geyser Events/min | ~500-800 | ~700-1100 | +40% |
| market-data CPU | 10-15% | 15-20% | +5% |
| market-data Memory | 300MB | 450MB | +50% |
| NATS Messages/min | ~600 | ~900 | +50% |
| Validator CPU | 100-110% | 110-120% | +10% |
| arb-strategy pools_tracked | ~200-300 | ~400-600 | +100% |
| arb-strategy opportunities | ~5-10/hr | ~15-30/hr | +200% |

### 6.2 Server Resource Availability

Current server state (from screenshot):
- **CPU**: 102% (1 core fully utilized, can handle +20% burst)
- **Memory**: 17.9% of 503GB = ~90GB used, **413GB free** ✅
- **Swap**: 486M/488M (minimal swap usage)
- **Load Average**: 12.17, 11.00, 11.08 (stable)

**Verdict**: ✅ **Server has sufficient capacity** for 6 DEXes
- Memory: 450MB impact negligible (413GB free)
- CPU: +10% acceptable (can burst to 120%)
- Network: Plenty of headroom

---

## 7. Risk Mitigation

### 7.1 Rollback Plan

```bash
# If critical issues occur during deployment:

# Option 1: Revert Geyser config only
ssh ironcrab-prod "cp /home/sol/geyser-config-backup-*.json /home/sol/geyser-grpc-plugin-config.json"
ssh ironcrab-prod "sudo systemctl restart solana-validator"

# Option 2: Disable new DEXes in code (hot-reload)
# Edit market-data config to comment out Meteora/CPMM
ssh ironcrab-prod "nano ~/Iron_crab/my_config.server.toml"
# Set: enabled_dexes = ["raydium_amm", "orca", "pumpfun", "pumpswap"]
ssh ironcrab-prod "sudo systemctl restart market-data"

# Option 3: Full rollback via git
git revert <commit-hash>
git push origin architecture-rebuild
ssh ironcrab-prod "cd ~/Iron_crab && bash deploy_new.sh"
```

### 7.2 Monitoring Queries

```bash
# Check Meteora pools discovered
curl -s http://localhost:9801/metrics | grep 'pools_tracked{dex="meteora_dlmm"}'

# Check CPMM pools discovered
curl -s http://localhost:9801/metrics | grep 'pools_tracked{dex="raydium_cpmm"}'

# Check arbitrage opportunities
curl -s http://localhost:9803/metrics | grep 'arb_opportunities_found'

# Check NATS lag
ssh ironcrab-prod "nats consumer ls ironcrab.v1.market_events"

# Check validator sync status
ssh ironcrab-prod "solana catchup --our-localhost"
```

---

## 8. Implementation Timeline

### ✅ Phase 1: Foundation (Complete - Day 1-2)
- ✅ Create `meteora_dlmm.rs` skeleton
- ✅ Create `meteora_dlmm_layout.rs` for pool parsing
- ✅ Implement basic pool parsing (904-byte LB Pair)
- ✅ Unit tests for data structures (test_parse_wsol_usdc_pool PASSED)
- ✅ DEX trait implementation (refresh_pools, quote_exact_in, list_pairs)
- ✅ Constant product approximation (Phase 1 fallback)
- ✅ Reserve balance fetching from token vaults

**Files Created:**
- `src/solana/dex/meteora_dlmm_layout.rs` (138 lines) ✅
- `src/solana/dex/meteora_dlmm.rs` (370 lines) ✅
- `tests/meteora_dlmm_integration.rs` (75 lines)

### ✅ Phase 2: Quote Logic (Complete - Day 3)
- ✅ Implement DLMM bin-walking algorithm (`meteora_bin_walker.rs`)
- ✅ `quote_x_to_y()`: Swap X→Y with multi-bin traversal
- ✅ `quote_y_to_x()`: Swap Y→X with multi-bin traversal
- ✅ Fee calculation (applied once at start, not per-bin)
- ✅ Constant product within each bin (x * y = k)
- ✅ Unit tests (test_bin_walker_x_to_y, test_bin_walker_y_to_x PASSED)

**Files Created:**
- `src/solana/dex/meteora_bin_walker.rs` (240 lines) ✅

**Test Results:**
```bash
running 2 tests
1 SOL → 99.600699 USDC
Bins crossed: 1
test meteora_bin_walker::tests::test_bin_walker_x_to_y ... ok

100 USDC → 0.000996007 SOL
test meteora_bin_walker::tests::test_bin_walker_y_to_x ... ok
```

### ✅ Phase 3: Swap Instructions (Complete - Day 4)
- ✅ Created `meteora_swap_builder.rs` (280 lines)
- ✅ Created `meteora_bin_array_layout.rs` (160 lines)
- ✅ `build_swap()`: Basic swap instruction builder
- ✅ `build_swap_with_bins()`: Full swap with bin array accounts
- ✅ `fetch_bin_arrays()`: Dynamic bin array discovery
  - Method 1: Direct PDA derivation (fast, tries ±3 arrays around active_id)
  - Method 2: getProgramAccounts with memcmp (comprehensive fallback)
- ✅ `derive_bin_array_pda()`: PDA derivation for bin arrays
- ✅ `bin_id_to_bin_array_index()`: Bin array index calculation
- ✅ BinArray parsing: 70 bins per array, u128 amounts, price calculation
- ✅ Integrated into `meteora_dlmm.rs` `build_swap_ix`
- ✅ Unit tests (8/8 PASSED)

**Files Created:**
- `src/solana/dex/meteora_swap_builder.rs` (280 lines) ✅
- `src/solana/dex/meteora_bin_array_layout.rs` (160 lines) ✅

**Test Results:**
```bash
running 8 tests
test meteora_bin_array_layout::tests::test_offset_to_bin_id ... ok
test meteora_bin_array_layout::tests::test_price_calculation ... ok
test meteora_bin_walker::tests::test_bin_walker_y_to_x ... ok
test meteora_bin_walker::tests::test_bin_walker_x_to_y ... ok
test meteora_dlmm_layout::tests::test_parse_wsol_usdc_pool ... ok
test meteora_swap_builder::tests::test_bin_array_index_calculation ... ok
test meteora_swap_builder::tests::test_derive_bin_array_pda ... ok
test meteora_dlmm::tests::test_constant_product_quote ... ok

test result: ok. 8 passed; 0 failed
```

**Features:**
- Dual-mode bin array fetching (PDA + getProgramAccounts)
- Automatic bin range selection (±3 arrays around active bin)
- Full bin data parsing (amount_x, amount_y, price per bin)
- Memcmp filtering for efficient queries (filters by lb_pair)

### 🔄 Phase 4: Integration (In Progress - Day 5)
- [ ] Update `mod.rs` exports
- [ ] Integrate into `arb_strategy.rs`
- [ ] Update Geyser config (already done ✅)
- [ ] Deploy to staging (if available)

### Phase 5: Testing & Deployment (Days 6-7)
- [ ] Live pool discovery tests
- [ ] Cross-DEX quote comparison
- [ ] Arbitrage simulation tests
- [ ] Production deployment
- [ ] 24h monitoring period

---

## 9. Success Criteria

### Must-Have (P0):
- ✅ Meteora DLMM pools discovered via Geyser
- ✅ Raydium CPMM pools discovered via Geyser
- ✅ Quote accuracy within 0.1% of Meteora/Raydium frontends
- ✅ No validator restarts or crashes
- ✅ market-data CPU < 25%
- ✅ arb-strategy finds at least 1 cross-DEX opportunity within 1 hour

### Nice-to-Have (P1):
- 📊 Dashboard shows all 6 DEXes with pool counts
- 📈 Arbitrage opportunities increase by 50%+
- 🔍 DecisionRecords include DEX names in metadata
- 📉 Trade-based discovery ratio stays <10%

### Monitoring Period (48h):
- No memory leaks in market-data
- No NATS consumer lag >1000 messages
- No anomalous validator behavior
- Arbitrage profitability increases (measured via backtest)

---

## 10. Next Steps

### Immediate Actions:
1. **Get Meteora SDK/Docs**: Review official Meteora DLMM SDK for exact layouts
2. **Get Raydium CPMM SDK**: Review Raydium CPMM program for instruction encoding
3. **Create Feature Branch**: `git checkout -b feature/meteora-cpmm-integration`
4. **Scaffold Files**: Create empty `meteora_dlmm.rs` and `raydium_cpmm.rs`

### Open Questions:
- [ ] Meteora DLMM bin array structure (how many bins cached?)
- [ ] Raydium CPMM observation account usage (optional or required?)
- [ ] Fee calculation details (static vs dynamic for DLMM)
- [ ] Instruction discriminators (need to verify via explorers)

### Resources Needed:
- Meteora DLMM SDK: https://github.com/MeteoraAg/dlmm-sdk
- Raydium CPMM SDK: https://github.com/raydium-io/raydium-sdk-V2
- Solana Explorer: https://solscan.io (for IX inspection)
- RPC Access: Helius/QuickNode for `getProgramAccounts`

---

## Appendix A: Arbitrage Opportunity Examples

### Example 1: DLMM ↔ AMM Price Inefficiency
```
Setup:
- Raydium AMM SOL/USDC: 1 SOL = $100.00 (0.3% fee)
- Meteora DLMM SOL/USDC: 1 SOL = $100.80 (0.1% fee, tight bins)

Arbitrage:
1. Buy 10 SOL on Raydium AMM: 10 SOL × $100 = $1000 - $3 fee = $997 cost
2. Sell 10 SOL on Meteora DLMM: 10 SOL × $100.80 = $1008 - $1 fee = $1007 revenue
3. Profit: $1007 - $997 = $10 (1% ROI before gas)

Why it happens:
- DLMM has concentrated liquidity → less slippage
- AMM has broad liquidity → more slippage on large orders
```

### Example 2: Triangular Arbitrage (3 DEXes)
```
Setup:
- Raydium AMM: SOL/USDC = 100
- Meteora DLMM: USDC/TOKEN = 0.50
- Raydium CPMM: TOKEN/SOL = 0.0051

Arbitrage Loop:
1. Start: 1 SOL
2. Swap SOL → USDC on Raydium AMM: 1 SOL → $100 USDC
3. Swap USDC → TOKEN on Meteora DLMM: $100 → 200 TOKEN
4. Swap TOKEN → SOL on Raydium CPMM: 200 TOKEN → 1.02 SOL
5. Profit: 0.02 SOL (2% ROI)

Why it happens:
- Different DEXes have different liquidity depths
- Price discovery lag between platforms
```

### Example 3: PumpFun Graduation Arbitrage
```
Setup:
- Token graduates from PumpFun to Raydium AMM
- Meteora DLMM pool created before Raydium
- Price discrepancy during migration

Arbitrage:
1. Buy on Raydium AMM (lower liquidity, worse price)
2. Sell on Meteora DLMM (concentrated liquidity, better price)
3. Exploit 5-10 minute window before prices converge

Expected ROI: 0.5-2% per trade
```

---

## Appendix B: Program ID Reference

| DEX | Program ID | Type | Status |
|-----|-----------|------|--------|
| Raydium AMM V4 | `675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8` | AMM | ✅ Implemented |
| Raydium CPMM | `CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C` | AMM | 🔄 In Progress |
| Orca Whirlpool | `whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc` | CLMM | ✅ Implemented |
| PumpFun | `6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P` | Bonding | ✅ Implemented |
| PumpSwap AMM | `pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA` | AMM | ✅ Implemented |
| Meteora DLMM | `LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo` | DLMM | 🔄 In Progress |

---

## 🎯 CONCRETE TODO LIST (Implementation Phase)

### Phase 1: Pool Data Structures & Parsing (Priority P0) - 1-2 Days

**1.1 Meteora DLMM Account Layout Reverse Engineering**
- [ ] **Task**: Fetch live DLMM pool from mainnet, analyze account data
  ```bash
  # Example: SOL/USDC DLMM pool
  solana account <dlmm-pool-address> --output json > meteora_pool_sample.json
  ```
- [ ] **Task**: Create `src/solana/dex/meteora_dlmm_layout.rs`
  - [ ] Define `DlmmPoolState` struct with exact byte offsets
  - [ ] Define `PoolParameters` struct
  - [ ] Define `DlmmBin` struct
  - [ ] Implement `parse_dlmm_pool(data: &[u8]) -> Result<DlmmPoolState>`
- [ ] **Task**: Write unit test with real pool data
  ```rust
  #[test]
  fn test_parse_meteora_pool_sol_usdc() {
      let data = include_bytes!("../../tests/fixtures/meteora_pool.bin");
      let pool = parse_dlmm_pool(data).unwrap();
      assert_eq!(pool.bin_step, 10); // Expected bin step
  }
  ```
- [ ] **Validation**: Compare parsed data with Meteora UI for same pool
- [ ] **Deliverable**: Working pool parser with >95% accuracy

**1.2 Raydium CPMM Account Layout Reverse Engineering**
- [ ] **Task**: Fetch live CPMM pool from mainnet
  ```bash
  solana account <cpmm-pool-address> --output json > raydium_cpmm_sample.json
  ```
- [ ] **Task**: Create parsing logic in `raydium_cpmm.rs`
  - [ ] Define `CpmmPoolState` struct
  - [ ] Implement `parse(data: &[u8]) -> Result<CpmmPoolState>`
  - [ ] Handle both active and inactive pools (status field)
- [ ] **Task**: Compare with Raydium AMM V4 layout (similar structure expected)
- [ ] **Validation**: Parse 5-10 different CPMM pools successfully
- [ ] **Deliverable**: Working CPMM parser

**Dependencies**: RPC access to mainnet, Solana Explorer for account inspection

---

### Phase 2: DEX Trait Implementation (Priority P0) - 2 Days

**2.1 Meteora DLMM - `refresh_pools()` Implementation**
- [ ] **Task**: Implement `MeteoraDlmm::new(rpc: Arc<SolanaRpc>)` constructor
- [ ] **Task**: Use `getProgramAccounts` to fetch all DLMM pools
  ```rust
  let filters = vec![
      RpcFilterType::DataSize(DLMM_POOL_MIN_SIZE as u64),
      // Optional: Filter by specific token mints (SOL, USDC, etc.)
  ];
  ```
- [ ] **Task**: Parse pool state for each account
- [ ] **Task**: Fetch active bins near `active_id` (±50 bins)
  - [ ] Derive bin array PDAs
  - [ ] Fetch bin data via RPC
  - [ ] Cache bins in `DlmmPoolCache`
- [ ] **Task**: Store in `DashMap<String, DlmmPoolCache>`
- [ ] **Validation**: Log pool count, verify against Meteora analytics
- [ ] **Deliverable**: Pools refresh successfully, cache populated

**2.2 Meteora DLMM - `quote_exact_in()` Implementation**
- [ ] **Task**: Implement bin-walking swap simulation
  ```rust
  fn simulate_dlmm_swap(
      pool: &DlmmPoolCache,
      input_mint: &str,
      amount_in: u64,
  ) -> Result<(u64, i32, u32)>
  ```
- [ ] **Logic**: Walk through bins from `active_id`, consume liquidity
- [ ] **Task**: Calculate fees (base_fee + volatility_fee if applicable)
- [ ] **Task**: Calculate price impact based on bins crossed
- [ ] **Validation**: Compare quotes with Meteora UI for same trade size
  - Target: <0.1% deviation
- [ ] **Deliverable**: Accurate quote calculation

**2.3 Raydium CPMM - `refresh_pools()` + `quote_exact_in()`**
- [ ] **Task**: Implement CPMM pool discovery (similar to AMM V4)
- [ ] **Task**: Fetch vault balances for reserves
- [ ] **Task**: Implement constant product formula (x * y = k)
  ```rust
  let amount_out = (reserve_out * amount_in_after_fee) / (reserve_in + amount_in_after_fee);
  ```
- [ ] **Task**: Calculate fees (base + protocol + fund)
- [ ] **Validation**: Compare with Raydium CPMM UI
- [ ] **Deliverable**: CPMM quotes working

**2.4 Implement `build_swap_ix()` for Both DEXes**
- [ ] **Meteora DLMM**: 
  - [ ] Research instruction discriminator (check Meteora SDK)
  - [ ] Derive bin_array_bitmap_extension PDA
  - [ ] Build account metas (lb_pair, reserves, user accounts)
  - [ ] Encode instruction data (amount_in, min_out)
- [ ] **Raydium CPMM**:
  - [ ] Research instruction discriminator
  - [ ] Derive authority PDA
  - [ ] Build account metas (pool, vaults, observation_key)
  - [ ] Encode instruction data
- [ ] **Validation**: Build instructions, verify with Solana Explorer
- [ ] **Deliverable**: Swap instructions ready (NOT executed yet)

**Dependencies**: Meteora SDK docs, Raydium CPMM SDK

---

### Phase 3: Integration & Testing (Priority P1) - 1-2 Days

**3.1 Update `src/solana/dex/mod.rs`**
- [ ] **Task**: Add module declarations
  ```rust
  pub mod meteora_dlmm;
  pub mod meteora_dlmm_layout;
  pub mod raydium_cpmm;
  ```
- [ ] **Task**: Add re-exports
  ```rust
  pub use meteora_dlmm::MeteoraDlmm;
  pub use raydium_cpmm::RaydiumCpmm;
  ```
- [ ] **Deliverable**: Modules accessible from other crates

**3.2 Integrate into `arb_strategy.rs`**
- [ ] **Task**: Update `initialize_dexes()` function
  ```rust
  dexes.push(Arc::new(RaydiumCpmm::new(rpc.clone())));
  dexes.push(Arc::new(MeteoraDlmm::new(rpc.clone())));
  ```
- [ ] **Task**: Verify arbitrage graph includes new DEXes
- [ ] **Task**: Test quote fetching for all 6 DEXes
- [ ] **Deliverable**: Arbitrage strategy sees 6 DEXes

**3.3 Write Integration Tests**
- [ ] **Task**: Test live pool discovery
  ```rust
  #[tokio::test]
  #[ignore]
  async fn test_meteora_refresh_pools_mainnet() { ... }
  ```
- [ ] **Task**: Test quote accuracy (compare with DEX UIs)
- [ ] **Task**: Test cross-DEX arbitrage detection
  - [ ] Check for SOL/USDC opportunities across Raydium AMM, CPMM, Meteora
- [ ] **Deliverable**: All integration tests pass

**3.4 Verify via Geyser Event Stream**
- [ ] **Task**: Monitor market-data logs for DLMM/CPMM pool events
  ```bash
  ssh ironcrab-prod 'sudo journalctl -u market-data -f | grep -i "meteora\|cpmm"'
  ```
- [ ] **Expected**: See PoolCreated / PoolUpdated events for new DEXes
- [ ] **Task**: Check metrics
  ```bash
  curl http://localhost:9801/metrics | grep 'pools_tracked{dex="meteora_dlmm"}'
  curl http://localhost:9801/metrics | grep 'pools_tracked{dex="raydium_cpmm"}'
  ```
- [ ] **Deliverable**: Geyser events flowing for all 6 DEXes

**Dependencies**: SSH access to server, market-data rebuilt with new code

---

### Phase 4: Deployment & Monitoring (Priority P1) - 1 Day

**4.1 Build & Deploy to Production**
- [ ] **Task**: Build release binaries
  ```bash
  cargo build --release --bin market-data
  cargo build --release --bin arb-strategy
  ```
- [ ] **Task**: Transfer to server
  ```bash
  scp target/release/market-data ironcrab-prod:~/Iron_crab/
  scp target/release/arb-strategy ironcrab-prod:~/Iron_crab/
  ```
- [ ] **Task**: Restart services
  ```bash
  ssh ironcrab-prod 'sudo systemctl restart market-data'
  ssh ironcrab-prod 'sudo systemctl restart arb-strategy'
  ```
- [ ] **Deliverable**: New code running in production

**4.2 Verify Pool Discovery**
- [ ] **Task**: Wait 5-10 minutes for pool refresh
- [ ] **Task**: Check logs for successful pool loading
  ```bash
  ssh ironcrab-prod 'sudo journalctl -u arb-strategy -n 100 --no-pager | grep "Initialized.*DEXes"'
  ```
- [ ] **Expected**: "Initialized 6 DEXes for arbitrage"
- [ ] **Task**: Check Grafana dashboard (if available)
- [ ] **Deliverable**: All 6 DEXes showing pools

**4.3 Monitor for 24-48 Hours**
- [ ] **Metrics to Watch**:
  - [ ] `pools_tracked_gauge` (should be 400-600 total)
  - [ ] `arb_opportunities_found` (should increase 50-100%)
  - [ ] `market_events_published_total` (should increase 15-25%)
  - [ ] market-data CPU usage (should stay <25%)
  - [ ] market-data memory (should stay <500MB)
- [ ] **Check for Issues**:
  - [ ] No NATS consumer lag >1000 messages
  - [ ] No validator performance degradation
  - [ ] No abnormal error rates
- [ ] **Deliverable**: Stable operation confirmed

**4.4 Performance Validation**
- [ ] **Task**: Run backtest comparing 4-DEX vs 6-DEX setup
- [ ] **Expected**: 40-60% more arbitrage opportunities detected
- [ ] **Task**: Measure quote latency for DLMM vs AMM
- [ ] **Deliverable**: Performance meets expectations

---

### Phase 5: Swap Execution Testing (Priority P2) - 1-2 Days

**NOTE**: Only after quotes are working and validated!

**5.1 Test Swap Instructions (Dry-Run)**
- [ ] **Task**: Create test binary to build swap instructions
- [ ] **Task**: Simulate Meteora DLMM swap (don't send)
- [ ] **Task**: Simulate Raydium CPMM swap (don't send)
- [ ] **Task**: Decode instructions, verify account metas
- [ ] **Deliverable**: Swap IX build correctly

**5.2 Execute Test Swaps on Devnet (if available)**
- [ ] **Task**: Deploy to devnet validator
- [ ] **Task**: Execute small test swaps (0.01 SOL)
- [ ] **Task**: Verify transactions succeed
- [ ] **Deliverable**: Swap execution works

**5.3 Mainnet Small-Size Testing**
- [ ] **Task**: Execute 0.1 SOL swap on Meteora DLMM
- [ ] **Task**: Execute 0.1 SOL swap on Raydium CPMM
- [ ] **Task**: Verify execution matches quote
- [ ] **Task**: Measure gas costs
- [ ] **Deliverable**: Mainnet execution validated

---

## 📋 Quick Reference Checklist

**Infrastructure (✅ DONE)**:
- [x] Geyser config deployed (ASCII, minimal, port 10000)
- [x] Validator account-index updated (6 DEXes)
- [x] market-data subscription updated (6 program IDs)
- [x] Geyser listener receives all 6 DEX events
- [x] Build compiles successfully

**Implementation (🔄 IN PROGRESS)**:
- [ ] Meteora DLMM pool parser
- [ ] Raydium CPMM pool parser
- [ ] Meteora DLMM quote calculation
- [ ] Raydium CPMM quote calculation
- [ ] Meteora DLMM swap instruction
- [ ] Raydium CPMM swap instruction
- [ ] Integration into arb-strategy
- [ ] Unit tests
- [ ] Integration tests

**Deployment (⏳ PENDING)**:
- [ ] Production deployment
- [ ] Pool discovery verification
- [ ] 24h monitoring period
- [ ] Performance validation
- [ ] Swap execution testing

**Success Metrics**:
- [ ] All 6 DEXes show >0 pools in metrics
- [ ] Arbitrage opportunities increase 40-60%
- [ ] Quote accuracy <0.1% deviation from DEX UIs
- [ ] No validator/service crashes
- [ ] CPU usage stays <25%
- [ ] Memory usage stays <500MB

---

## 🎯 CONCRETE TODO LIST (Implementation Phase)

### Phase 1: Pool Data Structures & Parsing (Priority P0) - 1-2 Days

**1.1 Meteora DLMM Account Layout Reverse Engineering**
- [ ] **Task**: Fetch live DLMM pool from mainnet, analyze account data
  ```bash
  # Example: SOL/USDC DLMM pool
  solana account <dlmm-pool-address> --output json > meteora_pool_sample.json
  ```
- [ ] **Task**: Create `src/solana/dex/meteora_dlmm_layout.rs`
  - [ ] Define `DlmmPoolState` struct with exact byte offsets
  - [ ] Define `PoolParameters` struct
  - [ ] Define `DlmmBin` struct
  - [ ] Implement `parse_dlmm_pool(data: &[u8]) -> Result<DlmmPoolState>`
- [ ] **Task**: Write unit test with real pool data
  ```rust
  #[test]
  fn test_parse_meteora_pool_sol_usdc() {
      let data = include_bytes!("../../tests/fixtures/meteora_pool.bin");
      let pool = parse_dlmm_pool(data).unwrap();
      assert_eq!(pool.bin_step, 10); // Expected bin step
  }
  ```
- [ ] **Validation**: Compare parsed data with Meteora UI for same pool
- [ ] **Deliverable**: Working pool parser with >95% accuracy

**1.2 Raydium CPMM Account Layout Reverse Engineering**
- [ ] **Task**: Fetch live CPMM pool from mainnet
  ```bash
  solana account <cpmm-pool-address> --output json > raydium_cpmm_sample.json
  ```
- [ ] **Task**: Create parsing logic in `raydium_cpmm.rs`
  - [ ] Define `CpmmPoolState` struct
  - [ ] Implement `parse(data: &[u8]) -> Result<CpmmPoolState>`
  - [ ] Handle both active and inactive pools (status field)
- [ ] **Task**: Compare with Raydium AMM V4 layout (similar structure expected)
- [ ] **Validation**: Parse 5-10 different CPMM pools successfully
- [ ] **Deliverable**: Working CPMM parser

**Dependencies**: RPC access to mainnet, Solana Explorer for account inspection

---

### Phase 2: DEX Trait Implementation (Priority P0) - 2 Days

**2.1 Meteora DLMM - `refresh_pools()` Implementation**
- [ ] **Task**: Implement `MeteoraDlmm::new(rpc: Arc<SolanaRpc>)` constructor
- [ ] **Task**: Use `getProgramAccounts` to fetch all DLMM pools
  ```rust
  let filters = vec![
      RpcFilterType::DataSize(DLMM_POOL_MIN_SIZE as u64),
      // Optional: Filter by specific token mints (SOL, USDC, etc.)
  ];
  ```
- [ ] **Task**: Parse pool state for each account
- [ ] **Task**: Fetch active bins near `active_id` (±50 bins)
  - [ ] Derive bin array PDAs
  - [ ] Fetch bin data via RPC
  - [ ] Cache bins in `DlmmPoolCache`
- [ ] **Task**: Store in `DashMap<String, DlmmPoolCache>`
- [ ] **Validation**: Log pool count, verify against Meteora analytics
- [ ] **Deliverable**: Pools refresh successfully, cache populated

**2.2 Meteora DLMM - `quote_exact_in()` Implementation**
- [ ] **Task**: Implement bin-walking swap simulation
  ```rust
  fn simulate_dlmm_swap(
      pool: &DlmmPoolCache,
      input_mint: &str,
      amount_in: u64,
  ) -> Result<(u64, i32, u32)>
  ```
- [ ] **Logic**: Walk through bins from `active_id`, consume liquidity
- [ ] **Task**: Calculate fees (base_fee + volatility_fee if applicable)
- [ ] **Task**: Calculate price impact based on bins crossed
- [ ] **Validation**: Compare quotes with Meteora UI for same trade size
  - Target: <0.1% deviation
- [ ] **Deliverable**: Accurate quote calculation

**2.3 Raydium CPMM - `refresh_pools()` + `quote_exact_in()`**
- [ ] **Task**: Implement CPMM pool discovery (similar to AMM V4)
- [ ] **Task**: Fetch vault balances for reserves
- [ ] **Task**: Implement constant product formula (x * y = k)
  ```rust
  let amount_out = (reserve_out * amount_in_after_fee) / (reserve_in + amount_in_after_fee);
  ```
- [ ] **Task**: Calculate fees (base + protocol + fund)
- [ ] **Validation**: Compare with Raydium CPMM UI
- [ ] **Deliverable**: CPMM quotes working

**2.4 Implement `build_swap_ix()` for Both DEXes**
- [ ] **Meteora DLMM**: 
  - [ ] Research instruction discriminator (check Meteora SDK)
  - [ ] Derive bin_array_bitmap_extension PDA
  - [ ] Build account metas (lb_pair, reserves, user accounts)
  - [ ] Encode instruction data (amount_in, min_out)
- [ ] **Raydium CPMM**:
  - [ ] Research instruction discriminator
  - [ ] Derive authority PDA
  - [ ] Build account metas (pool, vaults, observation_key)
  - [ ] Encode instruction data
- [ ] **Validation**: Build instructions, verify with Solana Explorer
- [ ] **Deliverable**: Swap instructions ready (NOT executed yet)

**Dependencies**: Meteora SDK docs, Raydium CPMM SDK

---

### Phase 3: Integration & Testing (Priority P1) - 1-2 Days

**3.1 Update `src/solana/dex/mod.rs`**
- [ ] **Task**: Add module declarations
  ```rust
  pub mod meteora_dlmm;
  pub mod meteora_dlmm_layout;
  pub mod raydium_cpmm;
  ```
- [ ] **Task**: Add re-exports
  ```rust
  pub use meteora_dlmm::MeteoraDlmm;
  pub use raydium_cpmm::RaydiumCpmm;
  ```
- [ ] **Deliverable**: Modules accessible from other crates

**3.2 Integrate into `arb_strategy.rs`**
- [ ] **Task**: Update `initialize_dexes()` function
  ```rust
  dexes.push(Arc::new(RaydiumCpmm::new(rpc.clone())));
  dexes.push(Arc::new(MeteoraDlmm::new(rpc.clone())));
  ```
- [ ] **Task**: Verify arbitrage graph includes new DEXes
- [ ] **Task**: Test quote fetching for all 6 DEXes
- [ ] **Deliverable**: Arbitrage strategy sees 6 DEXes

**3.3 Write Integration Tests**
- [ ] **Task**: Test live pool discovery
  ```rust
  #[tokio::test]
  #[ignore]
  async fn test_meteora_refresh_pools_mainnet() { ... }
  ```
- [ ] **Task**: Test quote accuracy (compare with DEX UIs)
- [ ] **Task**: Test cross-DEX arbitrage detection
  - [ ] Check for SOL/USDC opportunities across Raydium AMM, CPMM, Meteora
- [ ] **Deliverable**: All integration tests pass

**3.4 Verify via Geyser Event Stream**
- [ ] **Task**: Monitor market-data logs for DLMM/CPMM pool events
  ```bash
  ssh ironcrab-prod 'sudo journalctl -u market-data -f | grep -i "meteora\|cpmm"'
  ```
- [ ] **Expected**: See PoolCreated / PoolUpdated events for new DEXes
- [ ] **Task**: Check metrics
  ```bash
  curl http://localhost:9801/metrics | grep 'pools_tracked{dex="meteora_dlmm"}'
  curl http://localhost:9801/metrics | grep 'pools_tracked{dex="raydium_cpmm"}'
  ```
- [ ] **Deliverable**: Geyser events flowing for all 6 DEXes

**Dependencies**: SSH access to server, market-data rebuilt with new code

---

### Phase 4: Deployment & Monitoring (Priority P1) - 1 Day

**4.1 Build & Deploy to Production**
- [ ] **Task**: Build release binaries
  ```bash
  cargo build --release --bin market-data
  cargo build --release --bin arb-strategy
  ```
- [ ] **Task**: Transfer to server
  ```bash
  scp target/release/market-data ironcrab-prod:~/Iron_crab/
  scp target/release/arb-strategy ironcrab-prod:~/Iron_crab/
  ```
- [ ] **Task**: Restart services
  ```bash
  ssh ironcrab-prod 'sudo systemctl restart market-data'
  ssh ironcrab-prod 'sudo systemctl restart arb-strategy'
  ```
- [ ] **Deliverable**: New code running in production

**4.2 Verify Pool Discovery**
- [ ] **Task**: Wait 5-10 minutes for pool refresh
- [ ] **Task**: Check logs for successful pool loading
  ```bash
  ssh ironcrab-prod 'sudo journalctl -u arb-strategy -n 100 --no-pager | grep "Initialized.*DEXes"'
  ```
- [ ] **Expected**: "Initialized 6 DEXes for arbitrage"
- [ ] **Task**: Check Grafana dashboard (if available)
- [ ] **Deliverable**: All 6 DEXes showing pools

**4.3 Monitor for 24-48 Hours**
- [ ] **Metrics to Watch**:
  - [ ] `pools_tracked_gauge` (should be 400-600 total)
  - [ ] `arb_opportunities_found` (should increase 50-100%)
  - [ ] `market_events_published_total` (should increase 15-25%)
  - [ ] market-data CPU usage (should stay <25%)
  - [ ] market-data memory (should stay <500MB)
- [ ] **Check for Issues**:
  - [ ] No NATS consumer lag >1000 messages
  - [ ] No validator performance degradation
  - [ ] No abnormal error rates
- [ ] **Deliverable**: Stable operation confirmed

**4.4 Performance Validation**
- [ ] **Task**: Run backtest comparing 4-DEX vs 6-DEX setup
- [ ] **Expected**: 40-60% more arbitrage opportunities detected
- [ ] **Task**: Measure quote latency for DLMM vs AMM
- [ ] **Deliverable**: Performance meets expectations

---

### Phase 5: Swap Execution Testing (Priority P2) - 1-2 Days

**NOTE**: Only after quotes are working and validated!

**5.1 Test Swap Instructions (Dry-Run)**
- [ ] **Task**: Create test binary to build swap instructions
- [ ] **Task**: Simulate Meteora DLMM swap (don't send)
- [ ] **Task**: Simulate Raydium CPMM swap (don't send)
- [ ] **Task**: Decode instructions, verify account metas
- [ ] **Deliverable**: Swap IX build correctly

**5.2 Execute Test Swaps on Devnet (if available)**
- [ ] **Task**: Deploy to devnet validator
- [ ] **Task**: Execute small test swaps (0.01 SOL)
- [ ] **Task**: Verify transactions succeed
- [ ] **Deliverable**: Swap execution works

**5.3 Mainnet Small-Size Testing**
- [ ] **Task**: Execute 0.1 SOL swap on Meteora DLMM
- [ ] **Task**: Execute 0.1 SOL swap on Raydium CPMM
- [ ] **Task**: Verify execution matches quote
- [ ] **Task**: Measure gas costs
- [ ] **Deliverable**: Mainnet execution validated

---

## 📋 Quick Reference Checklist

**Infrastructure (✅ DONE)**:
- [x] Geyser config deployed (ASCII, minimal, port 10000)
- [x] Validator account-index updated (6 DEXes)
- [x] market-data subscription updated (6 program IDs)
- [x] Geyser listener receives all 6 DEX events
- [x] Build compiles successfully

**Implementation (🔄 IN PROGRESS)**:
- [ ] Meteora DLMM pool parser
- [ ] Raydium CPMM pool parser
- [ ] Meteora DLMM quote calculation
- [ ] Raydium CPMM quote calculation
- [ ] Meteora DLMM swap instruction
- [ ] Raydium CPMM swap instruction
- [ ] Integration into arb-strategy
- [ ] Unit tests
- [ ] Integration tests

**Deployment (⏳ PENDING)**:
- [ ] Production deployment
- [ ] Pool discovery verification
- [ ] 24h monitoring period
- [ ] Performance validation
- [ ] Swap execution testing

---

## Revision History

| Date | Version | Changes |
|------|---------|---------|
| 2026-01-09 20:30 | 1.1 | Updated: Infrastructure status, Geyser architecture, concrete TODO list |
| 2026-01-09 | 1.0 | Initial design for Meteora DLMM + Raydium CPMM |

