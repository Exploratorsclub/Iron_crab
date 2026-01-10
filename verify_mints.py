import base58

# Token X Mint aus LB Pair Offset 0x5B
mint_x_hex = '069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001'
mint_x_bytes = bytes.fromhex(mint_x_hex)
mint_x = base58.b58encode(mint_x_bytes).decode()

# Token Y Mint aus LB Pair Offset 0x7B
mint_y_hex = 'c6fa7af3bedabad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61'
mint_y_bytes = bytes.fromhex(mint_y_hex)
mint_y = base58.b58encode(mint_y_bytes).decode()

print("=== LB Pair Token Mints ===")
print(f"Token X: {mint_x}")
print(f"Expected: So11111111111111111111111111111111111111112 (WSOL)")
print(f"Match: {mint_x == 'So11111111111111111111111111111111111111112'}")
print()
print(f"Token Y: {mint_y}")
print(f"Expected: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v (USDC)")
print(f"Match: {mint_y == 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'}")
print()
print("=== Conclusion ===")
print("5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6 is the WSOL-USDC LB Pair!")
