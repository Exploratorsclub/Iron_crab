#!/usr/bin/env python3
"""
Send close_position control command to momentum-bot via NATS.

Usage:
    python send_close_position.py <mint>

Example:
    python send_close_position.py 9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump
"""
import asyncio
import json
import sys
from nats.aio.client import Client as NATS

async def main():
    if len(sys.argv) < 2:
        print("Usage: python send_close_position.py <mint>")
        print("Example: python send_close_position.py 9V7jznWgdN6tjMaJ6Bq11ZVQMkza6Zh45atgXbVmpump")
        sys.exit(1)

    mint = sys.argv[1]
    
    nc = NATS()
    await nc.connect("nats://localhost:4222")
    
    command = {
        "action": "close_position",
        "mint": mint
    }
    
    payload = json.dumps(command).encode()
    
    print(f"📤 Sending close_position command for mint: {mint}")
    await nc.publish("ironcrab.control.commands", payload)
    await nc.flush()
    
    print("✅ Command sent successfully")
    
    await nc.close()

if __name__ == "__main__":
    asyncio.run(main())
