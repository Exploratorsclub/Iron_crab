import requests
import base64

pool_pubkey = '5rCf1DM8LjKTw4YqhnoLcngyZYeNnQqztScTogYHAS6'
url = 'https://mainnet.helius-rpc.com/?api-key=96755862-7b83-484a-9f7a-2c0620253cc1'

payload = {
    'jsonrpc': '2.0',
    'id': 1,
    'method': 'getAccountInfo',
    'params': [pool_pubkey, {'encoding': 'base64'}]
}

r = requests.post(url, json=payload)
result = r.json()

if result.get('result') and result['result']['value']:
    value = result['result']['value']
    owner = value['owner']
    data_b64 = value['data'][0]
    data = base64.b64decode(data_b64)
    
    print(f'=== LB Pair from Transaction ===')
    print(f'Pubkey: {pool_pubkey}')
    print(f'Owner: {owner}')
    print(f'Size: {len(data)} bytes')
    print(f'Lamports: {value["lamports"]}')
    print()
    print(f'First 256 bytes (hex):')
    for i in range(0, min(256, len(data)), 16):
        hex_str = ' '.join(f'{b:02x}' for b in data[i:i+16])
        ascii_str = ''.join(chr(b) if 32 <= b < 127 else '.' for b in data[i:i+16])
        print(f'{i:04x}:  {hex_str:48s}  {ascii_str}')
    
    with open('meteora_lb_pair_real.bin', 'wb') as f:
        f.write(data)
    print()
    print('Saved to meteora_lb_pair_real.bin')
else:
    print('Account not found or null')
