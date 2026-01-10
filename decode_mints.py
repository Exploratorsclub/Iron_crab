import base58

# Hex dump aus fetch_lb_pair.py output:
# 0050:  04 00 00 00 00 00 00 00 06 9b 88 57 fe ab 81 84   ...........W....  
# 0060:  fb 68 7f 63 46 18 c0 35 da c4 39 dc 1a eb 3b 55   .h.cF..5..9...;U  
# 0070:  98 a0 f0 00 00 00 00 01 c6 fa 7a f3 be db ad 3a   ..........z....:  
# 0080:  3d 65 f3 6a ab c9 74 31 b1 bb e4 c2 d2 f6 e0 e4   =e.j..t1........  
# 0090:  7c a6 02 03 45 2f 5d 61

# Token X Mint (Offset 0x5B = 91, 32 bytes)
# Von 0x5B bis 0x7A (Position 91-122)
token_x_hex = '069b8857feab8184fb687f634618c035dac439dc1aeb3b5598a0f00000000001'

# Token Y Mint (Offset 0x7B = 123, 32 bytes)  
# Von 0x7B bis 0x9A (Position 123-154)
token_y_hex = 'c6fa7af3bedabad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61'

# Entferne Leerzeichen (Hex dump formatting)
token_x_hex = token_x_hex.replace(' ', '')
token_y_hex = token_y_hex.replace(' ', '')

print(f"Token X hex length: {len(token_x_hex)} (should be 64)")
print(f"Token Y hex length: {len(token_y_hex)} (should be 64)")
print()

if len(token_x_hex) == 64:
    token_x_bytes = bytes.fromhex(token_x_hex)
    token_x_b58 = base58.b58encode(token_x_bytes).decode()
    print(f"Token X: {token_x_b58}")
    print(f"Expected WSOL: So11111111111111111111111111111111111111112")
    print(f"Match: {token_x_b58 == 'So11111111111111111111111111111111111111112'}")
else:
    print(f"ERROR: Token X hex has wrong length")

print()

if len(token_y_hex) == 64:
    token_y_bytes = bytes.fromhex(token_y_hex)
    token_y_b58 = base58.b58encode(token_y_bytes).decode()
    print(f"Token Y: {token_y_b58}")
    print(f"Expected USDC: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v")
    print(f"Match: {token_y_b58 == 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'}")
else:
    print(f"ERROR: Token Y hex has wrong length")
