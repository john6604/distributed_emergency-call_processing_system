import asyncio
import json
import os
import sys

from redis import asyncio as aioredis
from redis.exceptions import ResponseError

from ..config import env_int, env_path, require_env
from . import (
    BATCH_STATUS_INITIALIZING,
    BATCH_STATUS_READY,
    pipeline_metadata_key,
    reset_instruction,
)

REDIS_URL = os.getenv("COLLECTOR_REDIS_URL") or require_env("REDIS_URL")
STREAM_OUT = os.getenv("STREAM_OUT", "stream:results")
PIPELINE_METADATA_KEY = pipeline_metadata_key()
COLLECTOR_BLOCK_MS = max(1, env_int("COLLECTOR_BLOCK_MS", 5000))
OUTPUT_JSONL = env_path("REDIS_RESULTS_JSONL", "outputs/redis_results.jsonl")


class CollectorStateError(RuntimeError):
    pass


async def wait_for_ready_batch(r):
    last_wait_message = None

    while True:
        try:
            metadata = await r.hgetall(PIPELINE_METADATA_KEY)
        except ResponseError as error:
            raise CollectorStateError(
                f"Redis key {PIPELINE_METADATA_KEY!r} is not a valid batch metadata hash. "
                f"{reset_instruction()}"
            ) from error

        if not metadata:
            wait_message = "Waiting for batch metadata..."
        elif metadata.get("status") == BATCH_STATUS_INITIALIZING:
            wait_message = "Waiting for batch initialization to finish..."
        elif metadata.get("status") != BATCH_STATUS_READY:
            raise CollectorStateError(
                f"Batch metadata has unsupported status {metadata.get('status')!r}. "
                f"{reset_instruction()}"
            )
        else:
            try:
                expected_tasks = int(metadata["expected_tasks"])
            except (KeyError, TypeError, ValueError) as error:
                raise CollectorStateError(
                    "Ready batch metadata has an invalid expected_tasks value. "
                    f"{reset_instruction()}"
                ) from error

            if expected_tasks < 0 or not metadata.get("dataset_fingerprint"):
                raise CollectorStateError(
                    f"Ready batch metadata is incomplete. {reset_instruction()}"
                )

            print(f"Ready batch detected: expecting {expected_tasks} unique results.")
            return expected_tasks

        if wait_message != last_wait_message:
            print(wait_message)
            last_wait_message = wait_message
        await asyncio.sleep(min(COLLECTOR_BLOCK_MS / 1000, 5.0))


def progress_step(expected_tasks):
    return max(1, expected_tasks // 10)


async def collect_unique_results(r, expected_tasks):
    results = {}
    last_stream_id = "0-0"
    last_reported = 0
    report_every = progress_step(expected_tasks)
    print(f"Processing progress: 0/{expected_tasks} unique results")

    while len(results) < expected_tasks:
        streams = await r.xread(
            {STREAM_OUT: last_stream_id},
            count=1000,
            block=COLLECTOR_BLOCK_MS,
        )
        if not streams:
            continue

        for _, entries in streams:
            for entry_id, fields in entries:
                last_stream_id = entry_id
                task_id = fields.get("id")
                if task_id is None:
                    print(f"Ignoring result entry {entry_id} without a task id.")
                    continue

                if task_id not in results:
                    results[task_id] = fields

        completed = len(results)
        if completed > expected_tasks:
            raise CollectorStateError(
                "Result stream contains more unique task IDs than the active batch expects. "
                f"{reset_instruction()}"
            )

        if completed == expected_tasks or completed - last_reported >= report_every:
            print(
                f"Processing progress: {completed}/{expected_tasks} unique results"
            )
            last_reported = completed

    return results


def numeric_order(fields):
    try:
        return int(fields["id"])
    except (KeyError, TypeError, ValueError):
        return 999999999


def write_results_atomically(results):
    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = OUTPUT_JSONL.with_name(f"{OUTPUT_JSONL.name}.tmp")
    ordered_results = sorted(results.values(), key=numeric_order)

    with temporary_path.open("w", encoding="utf-8") as output_file:
        for fields in ordered_results:
            record = {
                "id": fields["id"],
                "keywords": json.loads(fields.get("keywords", "[]")),
            }
            output_file.write(json.dumps(record, ensure_ascii=False) + "\n")

    os.replace(temporary_path, OUTPUT_JSONL)
    print(f"Results written to {OUTPUT_JSONL}")


async def main():
    r = aioredis.from_url(REDIS_URL, decode_responses=True)
    try:
        expected_tasks = await wait_for_ready_batch(r)
        results = await collect_unique_results(r, expected_tasks)
        write_results_atomically(results)
    except CollectorStateError as error:
        print(f"Collector stopped: {error}", file=sys.stderr)
        return 2
    finally:
        await r.aclose()

    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
