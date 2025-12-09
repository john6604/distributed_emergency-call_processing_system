# dispatcher_to_redis.py
import json
from redis import asyncio as aioredis
from pathlib import Path

REDIS_URL = "redis://192.168.3.30:6379"
STREAM = "stream:convs"

JSONL_PATH = "../dataset/conversaciones1.jsonl"

async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    count = 0
    with open(JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip(): continue
            obj = json.loads(line)
            # XADD stream field=value ...
            await r.xadd(STREAM, {"id": str(obj["id"]), "text": obj["text"]})
            count += 1
    print(f"Pushed {count} messages to {STREAM}")
    await r.close()

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
