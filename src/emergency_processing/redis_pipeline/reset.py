import os

from redis import asyncio as aioredis

from ..config import require_env
from . import pipeline_metadata_key

REDIS_URL = os.getenv("RESET_REDIS_URL") or require_env("REDIS_URL")
STREAM_IN = os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
WORKERS_SET = os.getenv("WORKERS_SET", "workers:active")
PIPELINE_METADATA_KEY = pipeline_metadata_key()


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    owned_keys = list(
        dict.fromkeys(
            (STREAM_IN, STREAM_OUT, PIPELINE_METADATA_KEY, WORKERS_SET)
        )
    )

    try:
        print("Explicit pipeline reset requested. Clearing Redis runtime keys:")
        for key in owned_keys:
            print(f"- {key}")

        deleted = await r.delete(*owned_keys)
        print(
            f"Pipeline reset complete: {deleted} existing keys removed. "
            "Deleting the input stream also removes its consumer-group state."
        )
    finally:
        await r.aclose()

    return 0


if __name__ == "__main__":
    import asyncio

    raise SystemExit(asyncio.run(main()))
