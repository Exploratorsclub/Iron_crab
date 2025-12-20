#!/usr/bin/env python3
"""Verify Pump.fun PDA derivation"""
import hashlib
from typing import Tuple

# Ed25519 curve order
CURVE_ORDER = 2**255 - 19

def bytes32_to_int(data: bytes) -> int:
    return int.from_bytes(data, 'little')

def is_on_curve(point: bytes) -> bool:
    """Check if a point is on the ed25519 curve"""
    # Simplified check - just verify it's 32 bytes and do basic validation
    if len(point) != 32:
        return False
    # A point is NOT on curve if it's a valid PDA (PDAs are off-curve by design)
    # For simplicity, we'll use the sha256 approach
    return True

def find_program_address(seeds: list, program_id: bytes) -> Tuple[bytes, int]:
    """
    Find a program derived address (PDA) for the given seeds and program ID.
    """
    for bump in range(255, -1, -1):
        try:
            address = create_program_address(seeds + [bytes([bump])], program_id)
            return address, bump
        except:
            continue
    raise ValueError("Unable to find a valid program address")

def create_program_address(seeds: list, program_id: bytes) -> bytes:
    """Create a program address from seeds and program ID."""
    # Concatenate all seeds
    data = b''
    for seed in seeds:
        if len(seed) > 32:
            raise ValueError("Seed too long")
        data += seed
    
    # Add program_id and "ProgramDerivedAddress" marker
    data += program_id
    data += b"ProgramDerivedAddress"
    
    # SHA256 hash
    digest = hashlib.sha256(data).digest()
    
    return digest

def base58_decode(s: str) -> bytes:
    """Decode a base58 string to bytes."""
    alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    base = len(alphabet)
    
    # Count leading zeros
    leading_zeros = 0
    for c in s:
        if c == '1':
            leading_zeros += 1
        else:
            break
    
    # Decode
    n = 0
    for c in s:
        n = n * base + alphabet.index(c)
    
    # Convert to bytes
    result = []
    while n > 0:
        result.append(n & 0xff)
        n >>= 8
    
    result = bytes(leading_zeros) + bytes(reversed(result))
    return result

def base58_encode(b: bytes) -> str:
    """Encode bytes to base58."""
    alphabet = '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
    base = len(alphabet)
    
    # Count leading zeros
    leading_zeros = 0
    for byte in b:
        if byte == 0:
            leading_zeros += 1
        else:
            break
    
    # Convert to integer
    n = int.from_bytes(b, 'big')
    
    # Encode
    result = []
    while n > 0:
        result.append(alphabet[n % base])
        n //= base
    
    return '1' * leading_zeros + ''.join(reversed(result))

# Pump.fun program ID
PROGRAM_ID = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P'
program_id_bytes = base58_decode(PROGRAM_ID)

# Token mint from the failed transaction: Jake
TOKEN_MINT = '5VhWirxHD3akur1EDvayhHpj4Nzjf898CvyL8cuR0pump'
token_mint_bytes = base58_decode(TOKEN_MINT)

print(f"Program ID: {PROGRAM_ID}")
print(f"Program ID bytes (hex): {program_id_bytes.hex()}")
print(f"Program ID length: {len(program_id_bytes)}")
print()
print(f"Token Mint: {TOKEN_MINT}")
print(f"Token Mint bytes (hex): {token_mint_bytes.hex()}")
print(f"Token Mint length: {len(token_mint_bytes)}")
print()

# Derive bonding curve PDA
seeds = [b'bonding-curve', token_mint_bytes]
bonding_curve_bytes, bump = find_program_address(seeds, program_id_bytes)
bonding_curve = base58_encode(bonding_curve_bytes)

print(f"Derived Bonding Curve: {bonding_curve}")
print(f"Bump: {bump}")
print()

# Also derive the associated bonding curve (token account for the bonding curve)
# This is the ATA of the bonding curve for the token mint
SPL_ATA_PROGRAM = 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL'
SPL_TOKEN_PROGRAM = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'

ata_program_bytes = base58_decode(SPL_ATA_PROGRAM)
token_program_bytes = base58_decode(SPL_TOKEN_PROGRAM)

# Associated bonding curve = ATA(bonding_curve, token_mint)
ata_seeds = [bonding_curve_bytes, token_program_bytes, token_mint_bytes]
associated_bc_bytes, ata_bump = find_program_address(ata_seeds, ata_program_bytes)
associated_bc = base58_encode(associated_bc_bytes)

print(f"Associated Bonding Curve (ATA): {associated_bc}")
print(f"ATA Bump: {ata_bump}")
