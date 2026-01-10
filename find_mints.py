import base58

with open('meteora_lb_pair_real.bin', 'rb') as f:
    data = f.read()

print(f"Searching for WSOL mint in {len(data)} bytes...")
print()

# WSOL mint = So11111111111111111111111111111111111111112
wsol_b58 = 'So11111111111111111111111111111111111111112'
wsol_bytes = base58.b58decode(wsol_b58)
print(f"WSOL bytes: {wsol_bytes.hex()}")

# USDC mint = EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v
usdc_b58 = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'
usdc_bytes = base58.b58decode(usdc_b58)
print(f"USDC bytes: {usdc_bytes.hex()}")
print()

# Suche WSOL
wsol_offset = data.find(wsol_bytes)
if wsol_offset != -1:
    print(f"✅ Found WSOL at offset 0x{wsol_offset:02x} ({wsol_offset})")
else:
    print("❌ WSOL not found")

# Suche USDC
usdc_offset = data.find(usdc_bytes)
if usdc_offset != -1:
    print(f"✅ Found USDC at offset 0x{usdc_offset:02x} ({usdc_offset})")
else:
    print("❌ USDC not found")

print()
print("=== First 256 bytes for reference ===")
for i in range(0, min(256, len(data)), 16):
    hex_str = ' '.join(f'{b:02x}' for b in data[i:i+16])
    print(f'{i:04x}:  {hex_str}')
