import base58

# Lade die binary Datei
with open('meteora_lb_pair_real.bin', 'rb') as f:
    data = f.read()

print(f"Total size: {len(data)} bytes")
print()

# Token X Mint bei Offset 0x5B (91 decimal)
token_x_offset = 0x5B
token_x_bytes = data[token_x_offset:token_x_offset+32]
token_x_b58 = base58.b58encode(token_x_bytes).decode()

print(f"Token X (offset 0x{token_x_offset:02x}):")
print(f"  Hex: {token_x_bytes.hex()}")
print(f"  Base58: {token_x_b58}")
print(f"  Expected: So11111111111111111111111111111111111111112 (WSOL)")
print(f"  Match: {token_x_b58 == 'So11111111111111111111111111111111111111112'}")
print()

# Token Y Mint bei Offset 0x7B (123 decimal)
token_y_offset = 0x7B
token_y_bytes = data[token_y_offset:token_y_offset+32]
token_y_b58 = base58.b58encode(token_y_bytes).decode()

print(f"Token Y (offset 0x{token_y_offset:02x}):")
print(f"  Hex: {token_y_bytes.hex()}")
print(f"  Base58: {token_y_b58}")
print(f"  Expected: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v (USDC)")
print(f"  Match: {token_y_b58 == 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'}")
print()

print("=== CONCLUSION ===")
if (token_x_b58 == 'So11111111111111111111111111111111111111112' and 
    token_y_b58 == 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'):
    print("✅ 5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 is the WSOL-USDC LB Pair!")
    print("   Solscan label 'Market' is misleading - this is the actual pool state.")
else:
    print("❌ Mints don't match expected values")
