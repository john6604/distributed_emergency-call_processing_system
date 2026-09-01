# producer_dynamic.py
import asyncio
import json
import os

from redis import asyncio as aioredis

from emergency_processing.config import env_path, require_env

REDIS_URL = os.getenv("DYNAMIC_REDIS_URL") or require_env("REDIS_URL")
JSONL_PATH = env_path("JSONL_PATH", "data/conversaciones1.jsonl")
STREAM_IN = os.getenv("DYNAMIC_STREAM_IN") or os.getenv("STREAM_IN", "stream:convs")


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    msgs = 0

    try:
        with open(JSONL_PATH, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue

                obj = json.loads(line)
                await r.xadd(
                    STREAM_IN,
                    {
                        "id": str(obj["id"]),
                        "text": obj["text"],
                    },
                )
                msgs += 1

        print(f"Enviados {msgs} mensajes a {STREAM_IN}")
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
