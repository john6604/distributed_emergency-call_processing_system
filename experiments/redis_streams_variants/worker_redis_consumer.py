import asyncio
import json
import os

import torch
from redis import asyncio as aioredis
from redis.exceptions import ResponseError
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from emergency_processing.config import env_float, env_int, require_env
from emergency_processing.keyword_extraction import extract_keywords_from_model

REDIS_URL = os.getenv("WORKER_REDIS_URL") or require_env("REDIS_URL")
STREAM_IN = os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "group2:convs")
CONSUMER_NAME = f"worker-{os.getenv('HOSTNAME', 'local')}-{os.getpid()}"
CONSUMER_BATCH = env_int("CONSUMER_BATCH", 4)
CLAIM_MILLIS = env_int("CLAIM_MILLIS", 30000)
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
        # Starting at 0 lets this historical variant process existing entries.
        await redis_client.xgroup_create(
            STREAM_IN, CONSUMER_GROUP, id="0", mkstream=True
        )
        print(f"Consumer group '{CONSUMER_GROUP}' created.")
    except ResponseError as error:
        if "BUSYGROUP" in str(error):
            print(f"Consumer group '{CONSUMER_GROUP}' already exists.")
        else:
            raise


def extract_keywords(text):
    return extract_keywords_from_model(text, tokenizer, model, device)


async def process_message(fields):
    conversation_id = fields.get("id")
    text = fields.get("text", "")

    if isinstance(text, bytes):
        text = text.decode()

    keywords = await asyncio.to_thread(extract_keywords, text)

    return {"id": conversation_id, "keywords": keywords}


async def consumer_loop():
    redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
    await ensure_consumer_group(redis_client)

    async def read_and_process_messages():
        while True:
            try:
                # Recover stale pending work before reading new deliveries.
                pending_entries = await redis_client.xpending_range(
                    STREAM_IN,
                    CONSUMER_GROUP,
                    min="-",
                    max="+",
                    count=CONSUMER_BATCH,
                )

                reclaimed_any = False

                if pending_entries:
                    for pending_entry in pending_entries:
                        message_id = pending_entry["message_id"]
                        claimed_messages = await redis_client.xclaim(
                            STREAM_IN,
                            CONSUMER_GROUP,
                            CONSUMER_NAME,
                            min_idle_time=CLAIM_MILLIS,
                            message_ids=[message_id],
                        )
                        if claimed_messages:
                            message_id, fields = claimed_messages[0]

                            try:
                                result = await process_message(fields)
                                await redis_client.xadd(
                                    STREAM_OUT,
                                    {
                                        "id": result["id"],
                                        "keywords": json.dumps(result["keywords"]),
                                    },
                                )
                                await redis_client.xack(
                                    STREAM_IN, CONSUMER_GROUP, message_id
                                )
                                print(
                                    f"[{CONSUMER_NAME}] Processed pending task "
                                    f"id={result['id']}; acknowledged message "
                                    f"{message_id}."
                                )
                                reclaimed_any = True
                            except Exception as error:
                                print(
                                    f"Error processing reclaimed message "
                                    f"{message_id}: {error}"
                                )

                    if reclaimed_any:
                        continue

                streams = await redis_client.xreadgroup(
                    CONSUMER_GROUP,
                    CONSUMER_NAME,
                    {STREAM_IN: ">"},
                    count=CONSUMER_BATCH,
                    block=5000,
                )

                if not streams:
                    await asyncio.sleep(SLEEP_EMPTY)
                    continue

                for _, messages in streams:
                    for message_id, fields in messages:
                        try:
                            result = await process_message(fields)
                            await redis_client.xadd(
                                STREAM_OUT,
                                {
                                    "id": result["id"],
                                    "keywords": json.dumps(result["keywords"]),
                                },
                            )
                            await redis_client.xack(
                                STREAM_IN, CONSUMER_GROUP, message_id
                            )
                            print(
                                f"[{CONSUMER_NAME}] Processed task "
                                f"id={result['id']}; acknowledged message "
                                f"{message_id}."
                            )
                        except Exception as error:
                            print(
                                f"Error processing message {message_id}: {error}"
                            )

            except ResponseError as error:
                if "NOGROUP" in str(error):
                    print("Consumer group is missing; recreating it.")
                    try:
                        await redis_client.xgroup_create(
                            STREAM_IN,
                            CONSUMER_GROUP,
                            id="0-0",
                            mkstream=True,
                        )
                        print("Consumer group recreated.")
                    except ResponseError as group_error:
                        if "BUSYGROUP" in str(group_error):
                            pass
                    continue

            except Exception as error:
                print("Read loop error:", error)
                await asyncio.sleep(1)

    try:
        await read_and_process_messages()
    finally:
        await redis_client.aclose()


if __name__ == "__main__":
    asyncio.run(consumer_loop())
