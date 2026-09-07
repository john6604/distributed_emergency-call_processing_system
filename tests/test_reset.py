import asyncio

import pytest


pytestmark = pytest.mark.redis


def test_reset_removes_only_pipeline_owned_keys(pipeline):
    pipeline.client.xadd(
        pipeline.keys.input_stream, {"id": "1", "text": "call one"}
    )
    pipeline.client.xgroup_create(
        pipeline.keys.input_stream, pipeline.keys.consumer_group, id="0"
    )
    pipeline.client.xreadgroup(
        pipeline.keys.consumer_group,
        "test-consumer",
        {pipeline.keys.input_stream: ">"},
        count=1,
    )
    pipeline.client.xadd(
        pipeline.keys.result_stream, {"id": "1", "keywords": "[]"}
    )
    pipeline.client.hset(
        pipeline.keys.metadata,
        mapping={"status": "ready", "expected_tasks": "1"},
    )
    pipeline.client.sadd(pipeline.keys.workers_set, "test-consumer")
    pipeline.client.set(pipeline.keys.unrelated, "preserve-me")

    assert asyncio.run(pipeline.reset.main()) == 0

    assert not any(pipeline.client.exists(key) for key in pipeline.keys.owned)
    assert pipeline.client.get(pipeline.keys.unrelated) == "preserve-me"
