#!/usr/bin/env python3
"""Send config update to control-plane."""
import json
import urllib.request
import sys

def main():
    component = sys.argv[1] if len(sys.argv) > 1 else "arb-strategy"
    
    config = {
        "two_hop_enabled": False,
        "multi_hop_enabled": False
    }
    
    payload = {
        "component": component,
        "config": config
    }
    
    url = "http://127.0.0.1:8080/config"
    data = json.dumps(payload).encode('utf-8')
    
    req = urllib.request.Request(
        url,
        data=data,
        headers={'Content-Type': 'application/json'},
        method='POST'
    )
    
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            result = response.read().decode('utf-8')
            print(f"Success: {result}")
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.read().decode('utf-8')}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
