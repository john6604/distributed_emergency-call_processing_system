# worker_dinamico.py
import asyncio
import json
import os
import time

import torch
from redis import asyncio as aioredis
from redis.exceptions import ResponseError
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

try:
    from keyword_extraction import extract_keywords_from_model
except ImportError:
    import sys
    from pathlib import Path

    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from keyword_extraction import extract_keywords_from_model

try:
    from config import env_float, env_int, require_env
except ImportError:
    from .config import env_float, env_int, require_env

REDIS_URL = os.getenv("DYNAMIC_REDIS_URL") or require_env("REDIS_URL")
STREAM_IN = os.getenv("DYNAMIC_STREAM_IN") or os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
GROUP = os.getenv("DYNAMIC_CONSUMER_GROUP") or os.getenv("CONSUMER_GROUP", "group2:convs")
CONSUMER = f"worker-{os.getenv('HOSTNAME', 'node')}-{os.getpid()}"

WORKERS_SET = os.getenv("WORKERS_SET", "workers:active")
WORKER_HEARTBEAT_SECONDS = env_int("WORKER_HEARTBEAT_SECONDS", 5)

BATCH = env_int("DYNAMIC_CONSUMER_BATCH", env_int("CONSUMER_BATCH", 4))
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


async def ensure_group(r):
    try:
        await r.xgroup_create(STREAM_IN, GROUP, id="0", mkstream=True)
        print(f"Consumer group '{GROUP}' creado para {STREAM_IN}")
    except ResponseError as error:
        if "BUSYGROUP" in str(error):
            print(f"Consumer group '{GROUP}' ya existe")
            return
        raise


async def heartbeat(r):
    while True:
        await r.hset(WORKERS_SET, CONSUMER, int(time.time()))
        await asyncio.sleep(WORKER_HEARTBEAT_SECONDS)


def extract_keywords(text):
    return extract_keywords_from_model(text, tokenizer, model, device)


async def process_message(r, msg_id, fields, label="NEW"):
    conv_id = fields.get("id")
    text = fields.get("text", "")

    if isinstance(text, bytes):
        text = text.decode("utf-8")

    try:
        keywords = await asyncio.to_thread(extract_keywords, text)
        await r.xadd(
            STREAM_OUT,
            {
                "id": conv_id,
                "keywords": json.dumps(keywords, ensure_ascii=False),
            },
        )
        await r.xack(STREAM_IN, GROUP, msg_id)
        print(f"[{CONSUMER}] processed {label} id={conv_id} acked {msg_id}")
    except Exception as error:
        print(f"[{CONSUMER}] error processing {label} {msg_id}: {error}")


async def process_messages(r, messages, label):
    for _, stream_messages in messages:
        for msg_id, fields in stream_messages:
            await process_message(r, msg_id, fields, label)


async def reclaim_stale_messages(r):
    next_id, messages, _ = await r.xautoclaim(
        STREAM_IN,
        GROUP,
        CONSUMER,
        min_idle_time=CLAIM_MILLIS,
        start_id="0-0",
        count=BATCH,
    )

    if messages:
        print(f"[{CONSUMER}] reclaimed {len(messages)} stale messages, next={next_id}")
        await process_messages(r, [(STREAM_IN, messages)], "STALE")

    return len(messages)


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    print(f"Worker {CONSUMER} activo en group={GROUP}, stream={STREAM_IN}")

    await ensure_group(r)
    heartbeat_task = asyncio.create_task(heartbeat(r))
    last_reclaim_at = 0.0

    try:
        while True:
            try:
                now = time.time()
                if now - last_reclaim_at >= RECLAIM_INTERVAL:
                    last_reclaim_at = now
                    if await reclaim_stale_messages(r):
                        continue

                messages = await r.xreadgroup(
                    GROUP,
                    CONSUMER,
                    {STREAM_IN: ">"},
                    count=BATCH,
                    block=5000,
                )

                if messages:
                    await process_messages(r, messages, "NEW")
                else:
                    await asyncio.sleep(SLEEP_EMPTY)

            except ResponseError as error:
                if "NOGROUP" in str(error):
                    await ensure_group(r)
                    continue
                raise
            except Exception as error:
                print(f"[{CONSUMER}] read loop error: {error}")
                await asyncio.sleep(SLEEP_EMPTY)
    finally:
        heartbeat_task.cancel()
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
