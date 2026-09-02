import hashlib
import json
import os
import sys

from redis import asyncio as aioredis
from redis.exceptions import ResponseError

from ..config import env_path, require_env
from . import (
    BATCH_STATUS_INITIALIZING,
    BATCH_STATUS_READY,
    pipeline_metadata_key,
    reset_instruction,
)

REDIS_URL = os.getenv("DISPATCHER_REDIS_URL") or require_env("REDIS_URL")
STREAM_IN = os.getenv("STREAM_IN", "stream:convs")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
WORKERS_SET = os.getenv("WORKERS_SET", "workers:active")
PIPELINE_METADATA_KEY = pipeline_metadata_key()
JSONL_PATH = env_path("JSONL_PATH", "data/conversaciones1.jsonl")


class PipelineStateError(RuntimeError):
    pass


def load_dataset(path=JSONL_PATH):
    content = path.read_bytes()
    fingerprint = hashlib.sha256(content).hexdigest()
    tasks = []
    task_ids = set()

    for line_number, line in enumerate(content.decode("utf-8").splitlines(), 1):
        if not line.strip():
            continue

        obj = json.loads(line)
        task_id = str(obj["id"])
        if task_id in task_ids:
            raise ValueError(
                f"Duplicate task id {task_id!r} in {path} at line {line_number}."
            )

        task_ids.add(task_id)
        tasks.append({"id": task_id, "text": obj["text"]})

    if not tasks:
        raise ValueError(f"Dataset {path} contains no tasks.")

    return tasks, fingerprint


def state_error(message):
    return PipelineStateError(f"{message} {reset_instruction()}")


async def read_metadata(r):
    try:
        return await r.hgetall(PIPELINE_METADATA_KEY)
    except ResponseError as error:
        raise state_error(
            f"Redis key {PIPELINE_METADATA_KEY!r} is not a valid batch metadata hash."
        ) from error


async def validate_existing_batch(r, metadata, tasks, fingerprint):
    status = metadata.get("status")
    if status != BATCH_STATUS_READY:
        raise state_error(
            f"Existing batch metadata has status {status!r}; only a ready batch can be resumed."
        )

    stored_fingerprint = metadata.get("dataset_fingerprint")
    if stored_fingerprint != fingerprint:
        raise state_error(
            "Existing Redis batch does not match the configured dataset."
        )

    try:
        expected_tasks = int(metadata["expected_tasks"])
    except (KeyError, TypeError, ValueError) as error:
        raise state_error("Existing batch metadata has an invalid expected_tasks value.") from error

    if expected_tasks != len(tasks):
        raise state_error(
            "Existing batch task count does not match the configured dataset."
        )

    try:
        stream_entries = await r.xrange(STREAM_IN, min="-", max="+")
    except ResponseError as error:
        raise state_error(f"Redis key {STREAM_IN!r} is not a valid input stream.") from error

    stream_task_ids = [fields.get("id") for _, fields in stream_entries]
    dataset_task_ids = [task["id"] for task in tasks]
    if stream_task_ids != dataset_task_ids:
        raise state_error(
            "Existing input stream is missing or incompatible with the initialized workload."
        )

    print(
        f"Existing batch detected: {expected_tasks} tasks. "
        "Dataset matches Redis state; no tasks were enqueued."
    )


async def initialize_batch(r, tasks, fingerprint):
    owned_keys = list(dict.fromkeys((STREAM_IN, STREAM_OUT, WORKERS_SET)))
    existing_keys = [key for key in owned_keys if await r.exists(key)]
    if existing_keys:
        raise state_error(
            "Pipeline metadata is missing while runtime keys already exist: "
            + ", ".join(existing_keys)
            + "."
        )

    claimed = await r.hsetnx(
        PIPELINE_METADATA_KEY,
        "status",
        BATCH_STATUS_INITIALIZING,
    )
    if not claimed:
        metadata = await read_metadata(r)
        await validate_existing_batch(r, metadata, tasks, fingerprint)
        return

    await r.hset(
        PIPELINE_METADATA_KEY,
        mapping={
            "expected_tasks": str(len(tasks)),
            "dataset_fingerprint": fingerprint,
        },
    )

    try:
        for task in tasks:
            await r.xadd(STREAM_IN, task)
    except Exception as error:
        raise state_error(
            "Batch initialization failed and Redis may contain a partial workload."
        ) from error

    await r.hset(PIPELINE_METADATA_KEY, "status", BATCH_STATUS_READY)
    print(f"Initialized batch with {len(tasks)} tasks.")


async def run_producer(r, tasks, fingerprint):
    metadata = await read_metadata(r)
    if metadata:
        await validate_existing_batch(r, metadata, tasks, fingerprint)
        return

    await initialize_batch(r, tasks, fingerprint)


async def main():
    try:
        tasks, fingerprint = load_dataset()
    except (OSError, UnicodeError, json.JSONDecodeError, KeyError, ValueError) as error:
        print(f"Producer could not read the dataset: {error}", file=sys.stderr)
        return 2

    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        await run_producer(r, tasks, fingerprint)
    except PipelineStateError as error:
        print(f"Producer refused to modify Redis state: {error}", file=sys.stderr)
        return 2
    finally:
        await r.aclose()

    return 0


if __name__ == "__main__":
    import asyncio

    raise SystemExit(asyncio.run(main()))
