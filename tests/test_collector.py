import asyncio
import json

import pytest


pytestmark = pytest.mark.redis


def set_ready_batch(pipeline, expected_tasks):
    pipeline.client.hset(
        pipeline.keys.metadata,
        mapping={
            "status": "ready",
            "expected_tasks": str(expected_tasks),
            "dataset_fingerprint": "test-fingerprint",
        },
    )


def add_result(pipeline, task_id, keywords):
    pipeline.client.xadd(
        pipeline.keys.result_stream,
        {"id": str(task_id), "keywords": json.dumps(keywords)},
    )


def read_output(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def assert_complete_output(pipeline, expected_ids):
    records = read_output(pipeline.output_path)
    ids = [record["id"] for record in records]
    assert ids == [str(task_id) for task_id in expected_ids]
    assert len(ids) == len(set(ids))
    assert not pipeline.output_path.with_name(
        f"{pipeline.output_path.name}.tmp"
    ).exists()
    return records


async def assert_collector_is_waiting(task):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), timeout=0.1)


def run_collector_scenario(scenario):
    return asyncio.run(asyncio.wait_for(scenario(), timeout=2))


def test_collector_waits_for_expected_unique_results(pipeline):
    set_ready_batch(pipeline, 3)
    add_result(pipeline, 2, ["two"])
    add_result(pipeline, 1, ["one"])

    async def scenario():
        task = asyncio.create_task(pipeline.collector.main())
        await assert_collector_is_waiting(task)
        add_result(pipeline, 3, ["three"])
        assert await task == 0

    run_collector_scenario(scenario)
    assert_complete_output(pipeline, [1, 2, 3])


def test_collector_ignores_duplicate_results_for_completion(pipeline):
    set_ready_batch(pipeline, 3)
    add_result(pipeline, 1, ["one"])
    add_result(pipeline, 2, ["first-two"])
    add_result(pipeline, 2, ["duplicate-two"])

    async def scenario():
        task = asyncio.create_task(pipeline.collector.main())
        await assert_collector_is_waiting(task)
        add_result(pipeline, 3, ["three"])
        assert await task == 0

    run_collector_scenario(scenario)
    records = assert_complete_output(pipeline, [1, 2, 3])
    assert len(records) == 3


def test_collector_keeps_first_observed_result(pipeline):
    set_ready_batch(pipeline, 2)
    add_result(pipeline, 1, ["first"])
    add_result(pipeline, 1, ["second"])
    add_result(pipeline, 2, ["other"])

    assert asyncio.run(pipeline.collector.main()) == 0

    records = assert_complete_output(pipeline, [1, 2])
    assert records[0]["keywords"] == ["first"]


def test_collector_replays_results_that_predate_startup(pipeline):
    set_ready_batch(pipeline, 3)
    add_result(pipeline, 10, ["ten"])
    add_result(pipeline, 2, ["two"])
    add_result(pipeline, 1, ["one"])

    assert asyncio.run(pipeline.collector.main()) == 0

    assert_complete_output(pipeline, [1, 2, 10])


def test_collector_reconstructs_progress_after_restart(pipeline):
    set_ready_batch(pipeline, 3)
    add_result(pipeline, 1, ["one"])

    async def interrupt_partial_collection():
        task = asyncio.create_task(pipeline.collector.main())
        await assert_collector_is_waiting(task)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    run_collector_scenario(interrupt_partial_collection)
    assert not pipeline.output_path.exists()
    add_result(pipeline, 2, ["two"])
    add_result(pipeline, 3, ["three"])

    assert asyncio.run(pipeline.collector.main()) == 0

    assert_complete_output(pipeline, [1, 2, 3])
