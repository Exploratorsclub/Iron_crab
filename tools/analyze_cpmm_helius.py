#!/usr/bin/env python3
"""Analyze Meteora CPMM (DAMM V2) pool layout via Helius RPC."""
import json
import urllib.request
import base64
import struct

# Helius RPC
RPC_URL = "https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1"

# Meteora CPMM Program ID (DAMM V2)
METEORA_CPMM_PROGRAM = "cpmmpPFsKiR4eeYnGSuXgkhLLgGL1j5FUZoJBJU9t9D"
DELPHI_MINT = "BFuy9AJYKekZ2hik7b5mPhsunGscegi9vPY2bwzzBAGS"
SOL_MINT = "So11111111111111111111111111111111111111112"

def rpc_call(method, params):
    req = urllib.request.Request(
        RPC_URL,
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode(),
        headers={"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as resp:
        return json.loads(resp.read().decode())

def bytes_to_pubkey(data):
    """Convert 32 bytes to base58 pubkey string."""
    import base58
    return base58.b58encode(data).decode()

print("=" * 70)
print("Meteora CPMM (DAMM V2) Pool Layout Analysis via Helius")
print("=" * 70)
print()

# First, find Delphi CPMM pool by checking token accounts
print(f"Searching for Delphi ({DELPHI_MINT[:8]}...) CPMM pools...")
print()

# Get largest token accounts for Delphi
result = rpc_call("getTokenLargestAccounts", [DELPHI_MINT])
if result.get("result") and result["result"]["value"]:
    print(f"Found {len(result['result']['value'])} token accounts for Delphi")
    
    for acc in result["result"]["value"][:15]:
        addr = acc["address"]
        amount = float(acc.get("uiAmountString", "0"))
        
        if amount < 1000:  # Skip small accounts
            continue
            
        # Get token account info to find owner
        acc_info = rpc_call("getAccountInfo", [addr, {"encoding": "jsonParsed"}])
        if not acc_info.get("result") or not acc_info["result"]["value"]:
            continue
            
        parsed = acc_info["result"]["value"].get("data", {})
        if isinstance(parsed, dict) and "parsed" in parsed:
            token_owner = parsed["parsed"]["info"].get("owner", "")
            
            # Check if owner is a CPMM pool
            owner_info = rpc_call("getAccountInfo", [token_owner, {"encoding": "base64"}])
            if owner_info.get("result") and owner_info["result"]["value"]:
                owner_program = owner_info["result"]["value"]["owner"]
                
                if owner_program == METEORA_CPMM_PROGRAM:
                    print(f"\n🎯 FOUND CPMM POOL: {token_owner}")
                    print(f"   Token Account: {addr}")
                    print(f"   Amount: {amount:,.2f}")
                    
                    # Analyze pool layout
                    pool_data = base64.b64decode(owner_info["result"]["value"]["data"][0])
                    print(f"   Data size: {len(pool_data)} bytes")
                    
                    # Import base58 for pubkey conversion
                    import base58
                    
                    # Parse CPMM Pool Layout
                    # Based on Meteora CPMM SDK (cp-swap)
                    print("\n   === Pool Layout Analysis ===")
                    
                    offset = 0
                    
                    # Discriminator (8 bytes)
                    discriminator = pool_data[offset:offset+8]
                    print(f"   [0:8] discriminator: {discriminator.hex()}")
                    offset += 8
                    
                    # amm_config (32 bytes)
                    amm_config = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [8:40] amm_config: {amm_config}")
                    offset += 32
                    
                    # pool_creator (32 bytes)
                    pool_creator = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [40:72] pool_creator: {pool_creator}")
                    offset += 32
                    
                    # token_0_vault (32 bytes)
                    token_0_vault = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [72:104] token_0_vault: {token_0_vault}")
                    offset += 32
                    
                    # token_1_vault (32 bytes)
                    token_1_vault = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [104:136] token_1_vault: {token_1_vault}")
                    offset += 32
                    
                    # lp_mint (32 bytes)
                    lp_mint = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [136:168] lp_mint: {lp_mint}")
                    offset += 32
                    
                    # token_0_mint (32 bytes)
                    token_0_mint = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [168:200] token_0_mint: {token_0_mint}")
                    offset += 32
                    
                    # token_1_mint (32 bytes)
                    token_1_mint = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [200:232] token_1_mint: {token_1_mint}")
                    offset += 32
                    
                    # token_0_program (32 bytes)
                    token_0_program = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [232:264] token_0_program: {token_0_program}")
                    offset += 32
                    
                    # token_1_program (32 bytes)
                    token_1_program = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [264:296] token_1_program: {token_1_program}")
                    offset += 32
                    
                    # observation_key (32 bytes)
                    observation_key = bytes_to_pubkey(pool_data[offset:offset+32])
                    print(f"   [296:328] observation_key: {observation_key}")
                    offset += 32
                    
                    # auth_bump (1 byte)
                    auth_bump = pool_data[offset]
                    print(f"   [328] auth_bump: {auth_bump}")
                    offset += 1
                    
                    # status (1 byte)  
                    status = pool_data[offset]
                    print(f"   [329] status: {status}")
                    offset += 1
                    
                    # lp_mint_decimals (1 byte)
                    lp_mint_decimals = pool_data[offset]
                    print(f"   [330] lp_mint_decimals: {lp_mint_decimals}")
                    offset += 1
                    
                    # mint_0_decimals (1 byte)
                    mint_0_decimals = pool_data[offset]
                    print(f"   [331] mint_0_decimals: {mint_0_decimals}")
                    offset += 1
                    
                    # mint_1_decimals (1 byte)
                    mint_1_decimals = pool_data[offset]
                    print(f"   [332] mint_1_decimals: {mint_1_decimals}")
                    offset += 1
                    
                    # lp_supply (8 bytes, u64)
                    lp_supply = struct.unpack_from("<Q", pool_data, offset)[0]
                    print(f"   [333:341] lp_supply: {lp_supply}")
                    offset += 8
                    
                    # protocol_fees_token_0 (8 bytes, u64)
                    protocol_fees_0 = struct.unpack_from("<Q", pool_data, offset)[0]
                    print(f"   [341:349] protocol_fees_token_0: {protocol_fees_0}")
                    offset += 8
                    
                    # protocol_fees_token_1 (8 bytes, u64)
                    protocol_fees_1 = struct.unpack_from("<Q", pool_data, offset)[0]
                    print(f"   [349:357] protocol_fees_token_1: {protocol_fees_1}")
                    offset += 8
                    
                    # fund_fees_token_0 (8 bytes, u64)
                    fund_fees_0 = struct.unpack_from("<Q", pool_data, offset)[0]
                    print(f"   [357:365] fund_fees_token_0: {fund_fees_0}")
                    offset += 8
                    
                    # fund_fees_token_1 (8 bytes, u64)
                    fund_fees_1 = struct.unpack_from("<Q", pool_data, offset)[0]
                    print(f"   [365:373] fund_fees_token_1: {fund_fees_1}")
                    offset += 8
                    
                    # open_time (8 bytes, i64)
                    open_time = struct.unpack_from("<q", pool_data, offset)[0]
                    print(f"   [373:381] open_time: {open_time}")
                    offset += 8
                    
                    # padding (16 bytes)
                    print(f"   [381:397] padding: {pool_data[offset:offset+16].hex()}")
                    
                    print(f"\n   Total parsed: {offset + 16} bytes")
                    
                    # Get vault balances for quote calculation
                    print("\n   === Vault Balances ===")
                    vault0_info = rpc_call("getTokenAccountBalance", [token_0_vault])
                    vault1_info = rpc_call("getTokenAccountBalance", [token_1_vault])
                    
                    if vault0_info.get("result") and vault0_info["result"]["value"]:
                        v0_balance = vault0_info["result"]["value"]["uiAmountString"]
                        print(f"   token_0_vault balance: {v0_balance}")
                    
                    if vault1_info.get("result") and vault1_info["result"]["value"]:
                        v1_balance = vault1_info["result"]["value"]["uiAmountString"]
                        print(f"   token_1_vault balance: {v1_balance}")
                    
                    # Only analyze first pool found
                    break

print()
print("=" * 70)
print("Layout Summary for Rust Implementation:")
print("=" * 70)
print("""
#[derive(Debug)]
pub struct CpmmPool {
    pub discriminator: [u8; 8],       // 0-8
    pub amm_config: Pubkey,           // 8-40
    pub pool_creator: Pubkey,         // 40-72
    pub token_0_vault: Pubkey,        // 72-104
    pub token_1_vault: Pubkey,        // 104-136
    pub lp_mint: Pubkey,              // 136-168
    pub token_0_mint: Pubkey,         // 168-200
    pub token_1_mint: Pubkey,         // 200-232
    pub token_0_program: Pubkey,      // 232-264
    pub token_1_program: Pubkey,      // 264-296
    pub observation_key: Pubkey,      // 296-328
    pub auth_bump: u8,                // 328
    pub status: u8,                   // 329
    pub lp_mint_decimals: u8,         // 330
    pub mint_0_decimals: u8,          // 331
    pub mint_1_decimals: u8,          // 332
    pub lp_supply: u64,               // 333-341
    pub protocol_fees_token_0: u64,   // 341-349
    pub protocol_fees_token_1: u64,   // 349-357
    pub fund_fees_token_0: u64,       // 357-365
    pub fund_fees_token_1: u64,       // 365-373
    pub open_time: i64,               // 373-381
    pub padding: [u8; 16],            // 381-397
}

// Total size: 397 bytes
""")
