import requests
import base64

rpc = 'https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1'
fee_program = 'pfeeUxB6jkeY1Hxd7CsFCAjcbHA9rWtchMGdZ6VojVZ'

# Fetch the reference TX
ref_tx = '3nj499thZ6JrdrC2WGGGRKoSC5Ydrat9gxP3XEnW5JK5ZWnXPzHE2QuAX8y7gvfsjRaLxCy3qkn6BYc1sxtfYiiY'

print(f"Fetching reference TX: {ref_tx}\n")

response = requests.post(rpc, json={
    "jsonrpc": "2.0",
    "id": 1,
    "method": "getTransaction",
    "params": [
        ref_tx,
        {
            "encoding": "jsonParsed",
            "maxSupportedTransactionVersion": 0
        }
    ]
})

result = response.json()

if "result" not in result or not result["result"]:
    print(f"ERROR: Could not fetch transaction!")
    print(result)
    exit(1)

tx = result["result"]["transaction"]["message"]
account_keys = tx.get("accountKeys", [])

# accountKeys are objects in jsonParsed format
account_pubkeys = [acc if isinstance(acc, str) else acc.get("pubkey") for acc in account_keys]

print(f"Total accountKeys: {len(account_pubkeys)}\n")

# Find the PumpSwap AMM swap instruction
instructions = result["result"]["transaction"]["message"]["instructions"]
pump_amm_program = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'

pump_ix = None
for ix in instructions:
    if ix.get("programId") == pump_amm_program:
        pump_ix = ix
        break

if not pump_ix:
    print("ERROR: No PumpSwap instruction found!")
    exit(1)

# Get the account indices used in the instruction
ix_accounts = pump_ix.get("accounts", [])
print(f"PumpSwap instruction uses {len(ix_accounts)} accounts (indices): {ix_accounts}\n")

# Now check which account is owned by Fee Program
print("=== Checking ownership of accounts used in PumpSwap instruction ===\n")

for i, pubkey in enumerate(ix_accounts):
    acc_response = requests.post(rpc, json={
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getAccountInfo",
        "params": [pubkey, {"encoding": "base64"}]
    })
    
    acc_json = acc_response.json()
    
    # Debug first one
    if i == 0:
        print(f"DEBUG: First account response: {acc_json}\n")
    
    acc_result = acc_json.get("result", {})
    acc_value = acc_result.get("value") if acc_result else None
    
    if acc_value:
        owner = acc_value.get("owner")
        executable = acc_value.get("executable", False)
        
        if owner == fee_program:
            print(f"[+] accounts[{i}] = {pubkey}")
            print(f"   Owner: {owner}")
            print(f"   Executable: {executable}")
            
            if not executable:
                print(f"   >>> THIS IS THE FEE CONFIG DATA ACCOUNT!")
            print()
    else:
        print(f"[-] accounts[{i}] = {pubkey} - ACCOUNT NOT FOUND\n")
