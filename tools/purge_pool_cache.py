#!/usr/bin/env python3
"""Purge NATS POOL_CACHE stream to clear pools with incorrect fee_config"""
import asyncio
import nats

async def purge():
    nc = await nats.connect('nats://127.0.0.1:4222')
    js = nc.jetstream()
    try:
        info = await js.stream_info('POOL_CACHE')
        msgs_before = info.state.messages
        print(f'Before purge: {msgs_before} messages')
        await js.purge_stream('POOL_CACHE')
        info2 = await js.stream_info('POOL_CACHE')
        print(f'After purge: {info2.state.messages} messages')
        print(f'Purged {msgs_before - info2.state.messages} messages')
    finally:
        await nc.drain()

if __name__ == '__main__':
    asyncio.run(purge())
