import requests
token_account = 'EiYjmozEXcbYLLMhrUtu15j1VG4fHPkvkwgnrET391gZ'
rpc_url = 'https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1'
req = {'jsonrpc': '2.0', 'id': 1, 'method': 'getAccountInfo', 'params': [token_account, {'encoding': 'jsonParsed'}]}
resp = requests.post(rpc_url, json=req).json()
info = resp['result']['value']['data']['parsed']['info']
print(f"Mint: {info['mint']}")
print(f"Amount: {info['tokenAmount']['uiAmountString']}")
print(f"Decimals: {info['tokenAmount']['decimals']}")
