import asyncio
import json

import pytest
from redis import asyncio as aioredis


pytestmark = pytest.mark.redis


def write_dataset(path, records):
    path.write_text(
        "".join(json.dumps(record) + "\n" for record in records),
        encoding="utf-8",
    )
    return path


def three_record_dataset(tmp_path):
    return write_dataset(
        tmp_path / "calls.jsonl",
        [
            {"id": 1, "text": "call one"},
            {"id": 2, "text": "call two"},
            {"id": 3, "text": "call three"},
        ],
    )


def run_producer(pipeline, tasks, fingerprint):
    async def run():
        client = aioredis.from_url(pipeline.redis_url, decode_responses=True)
        try:
            await pipeline.producer.run_producer(client, tasks, fingerprint)
        finally:
            await client.aclose()

    return asyncio.run(run())


def test_producer_initializes_fresh_batch(pipeline, tmp_path):
    dataset = three_record_dataset(tmp_path)
    tasks, fingerprint = pipeline.producer.load_dataset(dataset)

    run_producer(pipeline, tasks, fingerprint)

    metadata = pipeline.client.hgetall(pipeline.keys.metadata)
    entries = pipeline.client.xrange(pipeline.keys.input_stream)
    assert metadata["status"] == "ready"
    assert metadata["expected_tasks"] == "3"
    assert metadata["dataset_fingerprint"] == fingerprint
    assert len(fingerprint) == 64
    assert [fields["id"] for _, fields in entries] == ["1", "2", "3"]


def test_producer_does_not_reenqueue_existing_batch(pipeline, tmp_path):
    dataset = three_record_dataset(tmp_path)
    tasks, fingerprint = pipeline.producer.load_dataset(dataset)
    run_producer(pipeline, tasks, fingerprint)
    pipeline.client.xadd(
        pipeline.keys.result_stream, {"id": "1", "keywords": '["first"]'}
    )
    original_entries = pipeline.client.xrange(pipeline.keys.input_stream)
    original_metadata = pipeline.client.hgetall(pipeline.keys.metadata)
    original_results = pipeline.client.xrange(pipeline.keys.result_stream)

    run_producer(pipeline, tasks, fingerprint)

    assert pipeline.client.xrange(pipeline.keys.input_stream) == original_entries
    assert pipeline.client.hgetall(pipeline.keys.metadata) == original_metadata
    assert pipeline.client.xrange(pipeline.keys.result_stream) == original_results
    assert pipeline.client.xlen(pipeline.keys.input_stream) == 3


def test_producer_rejects_mismatched_dataset(pipeline, tmp_path):
    dataset_a = three_record_dataset(tmp_path)
    tasks_a, fingerprint_a = pipeline.producer.load_dataset(dataset_a)
    run_producer(pipeline, tasks_a, fingerprint_a)
    original_entries = pipeline.client.xrange(pipeline.keys.input_stream)
    original_metadata = pipeline.client.hgetall(pipeline.keys.metadata)
    original_results = pipeline.client.xrange(pipeline.keys.result_stream)
    dataset_b = write_dataset(
        tmp_path / "changed-calls.jsonl",
        [
            {"id": 1, "text": "changed call"},
            {"id": 2, "text": "call two"},
            {"id": 3, "text": "call three"},
        ],
    )
    tasks_b, fingerprint_b = pipeline.producer.load_dataset(dataset_b)

    with pytest.raises(
        pipeline.producer.PipelineStateError,
        match="does not match the configured dataset",
    ):
        run_producer(pipeline, tasks_b, fingerprint_b)

    assert pipeline.client.hgetall(pipeline.keys.metadata) == original_metadata
    assert pipeline.client.xrange(pipeline.keys.input_stream) == original_entries
    assert pipeline.client.xrange(pipeline.keys.result_stream) == original_results


def test_producer_rejects_duplicate_task_ids_before_writing_redis(
    pipeline, tmp_path
):
    dataset = write_dataset(
        tmp_path / "duplicate-calls.jsonl",
        [
            {"id": 1, "text": "call one"},
            {"id": 1, "text": "duplicate call"},
        ],
    )

    with pytest.raises(ValueError, match="Duplicate task id '1'"):
        pipeline.producer.load_dataset(dataset)

    assert not any(pipeline.client.exists(key) for key in pipeline.keys.owned)


def test_producer_refuses_interrupted_initialization_state(pipeline, tmp_path):
    dataset = three_record_dataset(tmp_path)
    tasks, fingerprint = pipeline.producer.load_dataset(dataset)
    pipeline.client.hset(
        pipeline.keys.metadata,
        mapping={
            "status": "initializing",
            "expected_tasks": "3",
            "dataset_fingerprint": fingerprint,
        },
    )
    pipeline.client.xadd(
        pipeline.keys.input_stream, {"id": "sentinel", "text": "existing"}
    )
    pipeline.client.xadd(
        pipeline.keys.result_stream,
        {"id": "sentinel", "keywords": '["existing"]'},
    )
    pipeline.client.sadd(pipeline.keys.workers_set, "test-worker")
    original_state = {
        "metadata": pipeline.client.hgetall(pipeline.keys.metadata),
        "input": pipeline.client.xrange(pipeline.keys.input_stream),
        "results": pipeline.client.xrange(pipeline.keys.result_stream),
        "workers": pipeline.client.smembers(pipeline.keys.workers_set),
    }

    with pytest.raises(
        pipeline.producer.PipelineStateError,
        match="only a ready batch can be resumed",
    ):
        run_producer(pipeline, tasks, fingerprint)

    assert pipeline.client.hgetall(pipeline.keys.metadata) == original_state["metadata"]
    assert pipeline.client.xrange(pipeline.keys.input_stream) == original_state["input"]
    assert pipeline.client.xrange(pipeline.keys.result_stream) == original_state["results"]
    assert pipeline.client.smembers(pipeline.keys.workers_set) == original_state["workers"]
