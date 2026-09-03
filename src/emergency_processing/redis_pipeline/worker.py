import asyncio
import json
import os
import time

import torch
from redis import asyncio as aioredis
from redis.exceptions import ResponseError
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from ..config import env_float, env_int, require_env
from ..keyword_extraction import extract_keywords_from_model

REDIS_URL = os.getenv("DYNAMIC_REDIS_URL") or require_env("REDIS_URL")
STREAM_IN = os.getenv("DYNAMIC_STREAM_IN") or os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
CONSUMER_GROUP = os.getenv("DYNAMIC_CONSUMER_GROUP") or os.getenv(
    "CONSUMER_GROUP", "group2:convs"
)
CONSUMER_NAME = f"worker-{os.getenv('HOSTNAME', 'node')}-{os.getpid()}"

WORKERS_SET = os.getenv("WORKERS_SET", "workers:active")
WORKER_HEARTBEAT_SECONDS = env_int("WORKER_HEARTBEAT_SECONDS", 5)

CONSUMER_BATCH = env_int(
    "DYNAMIC_CONSUMER_BATCH", env_int("CONSUMER_BATCH", 4)
)
CLAIM_MILLIS = env_int("CLAIM_MILLIS", 30000)
RECLAIM_INTERVAL = env_float("RECLAIM_INTERVAL", 20.0)
SLEEP_EMPTY = env_float("SLEEP_EMPTY", 1.0)

MODEL_NAME = os.getenv("MODEL_NAME", "UDA-LIDI/barto_emergency_multi_purpose")
HF_TOKEN = os.getenv("HF_TOKEN") or os.getenv("HUGGINGFACE_TOKEN")
MODEL_AUTH = {"use_auth_token": HF_TOKEN} if HF_TOKEN else {}

device = "cuda" if torch.cuda.is_available() else "cpu"
tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, **MODEL_AUTH)
model = AutoModelForSeq2SeqLM.from_pretrained(MODEL_NAME, **MODEL_AUTH).to(device)
model.eval()


async def ensure_consumer_group(redis_client):
    try:
        await redis_client.xgroup_create(
            STREAM_IN, CONSUMER_GROUP, id="0", mkstream=True
        )
        print(
            f"Consumer group '{CONSUMER_GROUP}' created for stream '{STREAM_IN}'."
        )
    except ResponseError as error:
        if "BUSYGROUP" in str(error):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists.")
            return
        raise


async def heartbeat(redis_client):
    while True:
        await redis_client.hset(WORKERS_SET, CONSUMER_NAME, int(time.time()))
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


def extract_keywords(text):
    return extract_keywords_from_model(text, tokenizer, model, device)


async def process_message(redis_client, message_id, fields, label="NEW"):
    conversation_id = fields.get("id")
    text = fields.get("text", "")

    if isinstance(text, bytes):
        text = text.decode("utf-8")

    try:
        keywords = await asyncio.to_thread(extract_keywords, text)
        await redis_client.xadd(
            STREAM_OUT,
            {
                "id": conversation_id,
                "keywords": json.dumps(keywords, ensure_ascii=False),
            },
        )
        # Publish before acknowledging so interrupted work remains reclaimable
        # through the pending entries list.
        await redis_client.xack(STREAM_IN, CONSUMER_GROUP, message_id)
        print(
            f"[{CONSUMER_NAME}] Processed {label} task id={conversation_id}; "
            f"acknowledged message {message_id}."
        )
    except Exception as error:
        print(
            f"[{CONSUMER_NAME}] Error processing {label} message "
            f"{message_id}: {error}"
        )


async def process_messages(redis_client, messages, label):
    for _, stream_messages in messages:
        for message_id, fields in stream_messages:
            await process_message(redis_client, message_id, fields, label)


async def reclaim_stale_messages(redis_client):
    next_id, messages, _ = await redis_client.xautoclaim(
        STREAM_IN,
        CONSUMER_GROUP,
        CONSUMER_NAME,
        min_idle_time=CLAIM_MILLIS,
        start_id="0-0",
        count=CONSUMER_BATCH,
    )

    if messages:
        print(
            f"[{CONSUMER_NAME}] Reclaimed {len(messages)} stale messages; "
            f"next={next_id}."
        )
        await process_messages(redis_client, [(STREAM_IN, messages)], "STALE")

    return len(messages)


async def main():
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    print(
        f"Worker ready: consumer={CONSUMER_NAME}, group={CONSUMER_GROUP}, "
        f"stream={STREAM_IN}."
    )

    await ensure_consumer_group(redis_client)
    heartbeat_task = asyncio.create_task(heartbeat(redis_client))
    last_reclaim_at = 0.0

    try:
        while True:
            try:
                now = time.time()
                if now - last_reclaim_at >= RECLAIM_INTERVAL:
                    last_reclaim_at = now
                    if await reclaim_stale_messages(redis_client):
                        continue

                messages = await redis_client.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {STREAM_IN: ">"},
                    count=CONSUMER_BATCH,
                    block=5000,
                )

                if messages:
                    await process_messages(redis_client, messages, "NEW")
                else:
                    await asyncio.sleep(SLEEP_EMPTY)

            except ResponseError as error:
                if "NOGROUP" in str(error):
                    await ensure_consumer_group(redis_client)
                    continue
                raise
            except Exception as error:
                print(f"[{CONSUMER_NAME}] Read loop error: {error}")
                await asyncio.sleep(SLEEP_EMPTY)
    finally:
        heartbeat_task.cancel()
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
