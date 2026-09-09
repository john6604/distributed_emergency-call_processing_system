import asyncio
import json
import os

from redis import asyncio as aioredis

from emergency_processing.config import env_path, require_env

REDIS_URL = os.getenv("DYNAMIC_REDIS_URL") or require_env("REDIS_URL")
JSONL_PATH = env_path("JSONL_PATH", "data/sample_calls_en.jsonl")
STREAM_IN = os.getenv("DYNAMIC_STREAM_IN") or os.getenv("STREAM_IN", "stream:convs")


async def main():
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    message_count = 0

    try:
        with open(JSONL_PATH, "r", encoding="utf-8") as input_file:
            for line in input_file:
                if not line.strip():
                    continue

                record = json.loads(line)
                await redis_client.xadd(
                    STREAM_IN,
                    {
                        "id": str(record["id"]),
                        "text": record["text"],
                    },
                )
                message_count += 1

        print(f"Sent {message_count} messages to {STREAM_IN}.")
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
