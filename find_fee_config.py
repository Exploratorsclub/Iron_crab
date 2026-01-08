import requests

# All account keys from the reference TX
account_keys = [
    # Base accounts (0-16)
    'CoFVrE6YuWj2Z3Pufhkzer8m1gtWd2t872kJAxyYvKc1',  # 0
    '9uBxfjGgysJVuBBhnKK9GryAmZcsqkNWaMDBtorexAfc',  # 1
    '3zAgKE7aYLiSpGFmxrZSNGNPKi5e7JzYncmLDfZSbj1F',  # 2 (market)
    '6doFSmYCMJB4udgx8iBr5kXPEDceCy72qEsNzN3rRLGJ',  # 3 (base_mint = Browser Zer0)
    '7acQqCeeWrVW8TVDe3ojVUAC22ichJ1rfHgsCaLPDL7K',  # 4
    'FvwcdC2D6HiHYYxNUqbPpijDYeyH3M92GsQThBPWFwn5',  # 5
    'DWpvfqzGWuVy9jVSKSShdM2733nrEsnnhsUStYbkj6Nn',  # 6
    '48NNDwsKkXXpJw4PcDHRJtNZ65iU39vqKG3RXQk5v9BE',  # 7 (coin_creator_vault_ata)
    'HYeXVHVyMDYY8UkYQ9ZNpm128unSCsVw19WG6Vr3EiUE',  # 8 (coin_creator_vault_authority)
    '5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx',  # 9
    'ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw',  # 10 (global_config)
    'JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU',  # 11 (protocol_fee_recipient)
    'BSfD6SHZigAfDWSjzD5Q41jw8LmKwtmjskPH9XW1mrRW',  # 12
    'AVUCZyuT35YSuj4RH7fwiyPu82Djn2Hfg7y2ND2XcnZH',  # 13
    '5PHirr8joyTMp9JMm6nW7hNDVyEYdkzDqazxPD7RaTjx',  # 14 (dup!)
    'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ',  # 15 (Fee Program - executable!)
    'GS4CU59F31iL7aR2Q8zVS8DRrcRnXX1yjQ66TqNVQnaR',  # 16
    # Address lookup table accounts (17-24)
    'BSfD6SHZigAfDWSjzD5Q41jw8LmKwtmjskPH9XW1mrRW',  # 17 (dup!)
    'AVUCZyuT35YSuj4RH7fwiyPu82Djn2Hfg7y2ND2XcnZH',  # 18 (dup!)
    'jitodontfront111111111111111tradewithPhoton',  # 19
    'So11111111111111111111111111111111111111112',  # 20 (WSOL)
    'SysvarRent111111111111111111111111111111111',  # 21
    'ADyA8hdefvWN2dbGGWFotbzWxrAvLW83WG6QCVXvJKqw',  # 22 (global_config dup!)
    'JCRGumoE9Qi5BBgULTgdgTLjSgkCMSbF62ZZfGs84JeU',  # 23 (protocol_fee_recipient dup!)
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL',  # 24
]

# PumpSwap swap instruction uses these indices (from earlier analysis)
instruction_accounts = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]

rpc = 'https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1'
fee_program = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'

print("=== Checking which accounts are owned by Fee Program ===\n")

for idx in instruction_accounts:
    acc = account_keys[idx]
    
    # Skip duplicates we already checked
    if idx > 0 and acc == account_keys[idx-1]:
        continue
    
    response = requests.post(rpc, json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [acc, {"encoding": "json"}]
    })
    
    result = response.json().get("result", {})
    value = result.get("value")
    
    if value:
        owner = value.get("owner")
        executable = value.get("executable", False)
        data_size = len(value.get("data", [""])[0]) if value.get("data") else 0
        
        if owner == fee_program:
            print(f"✅ accounts[{idx}] (accountKeys[{idx}]) = {acc}")
            print(f"   Owner: {owner}")
            print(f"   Executable: {executable}")
            print(f"   Data size: {data_size}")
            print()
    else:
        print(f"❌ accounts[{idx}] = {acc} - ACCOUNT NOT FOUND")
        print()

print("\n=== Looking for PDA owned by Fee Program ===")
# Check all accounts in instruction for Fee Program ownership
for idx in instruction_accounts:
    acc = account_keys[idx]
    response = requests.post(rpc, json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [acc, {"encoding": "json"}]
    })
    
    result = response.json().get("result", {})
    value = result.get("value")
    
    if value and value.get("owner") == fee_program and not value.get("executable"):
        print(f"\n🎯 FOUND FEE CONFIG DATA ACCOUNT!")
        print(f"   Index: accounts[{idx}]")
        print(f"   Pubkey: {acc}")
        print(f"   Owner: {value.get('owner')}")
        print(f"   Executable: {value.get('executable')}")
        print(f"   Data: {value.get('data')}")
