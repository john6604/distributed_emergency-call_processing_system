import os
import uuid
from dataclasses import dataclass
from types import SimpleNamespace

import pytest
from redis import Redis
from redis.exceptions import RedisError


DEFAULT_TEST_REDIS_URL = "redis://127.0.0.1:6379/15"
os.environ.setdefault(
    "REDIS_URL", os.getenv("TEST_REDIS_URL", DEFAULT_TEST_REDIS_URL)
)


@dataclass(frozen=True)
class RedisKeys:
    input_stream: str
    result_stream: str
    consumer_group: str
    workers_set: str
    metadata: str
    unrelated: str

    @property
    def owned(self):
        return (
            self.input_stream,
            self.result_stream,
            self.workers_set,
            self.metadata,
        )

    @property
    def all(self):
        return (*self.owned, self.unrelated)


@pytest.fixture(scope="session")
def test_redis_url():
    redis_url = os.getenv("TEST_REDIS_URL")
    if not redis_url:
        pytest.skip(
            "Redis tests require TEST_REDIS_URL; "
            "use redis://127.0.0.1:6379/15 or the Docker test service."
        )

    client = Redis.from_url(redis_url, decode_responses=True)
    try:
        client.ping()
    except RedisError as error:
        pytest.skip(f"Redis tests require an available TEST_REDIS_URL: {error}")
    finally:
        client.close()

    return redis_url


@pytest.fixture
def redis_test_state(test_redis_url):
    namespace = f"test:emergency-processing:{uuid.uuid4().hex}"
    keys = RedisKeys(
        input_stream=f"{namespace}:input",
        result_stream=f"{namespace}:results",
        consumer_group=f"{namespace}:group",
        workers_set=f"{namespace}:workers",
        metadata=f"{namespace}:metadata",
        unrelated=f"{namespace}:unrelated",
    )
    client = Redis.from_url(test_redis_url, decode_responses=True)
    client.delete(*keys.all)

    try:
        yield SimpleNamespace(client=client, keys=keys, redis_url=test_redis_url)
    finally:
        client.delete(*keys.all)
        client.close()


@pytest.fixture
def pipeline(monkeypatch, redis_test_state, tmp_path):
    from emergency_processing.redis_pipeline import collector, producer, reset

    state = redis_test_state
    output_path = tmp_path / "results.jsonl"

    monkeypatch.setattr(producer, "REDIS_URL", state.redis_url)
    monkeypatch.setattr(producer, "STREAM_IN", state.keys.input_stream)
    monkeypatch.setattr(producer, "STREAM_OUT", state.keys.result_stream)
    monkeypatch.setattr(producer, "WORKERS_SET", state.keys.workers_set)
    monkeypatch.setattr(producer, "PIPELINE_METADATA_KEY", state.keys.metadata)

    monkeypatch.setattr(collector, "REDIS_URL", state.redis_url)
    monkeypatch.setattr(collector, "STREAM_OUT", state.keys.result_stream)
    monkeypatch.setattr(collector, "PIPELINE_METADATA_KEY", state.keys.metadata)
    monkeypatch.setattr(collector, "COLLECTOR_BLOCK_MS", 20)
    monkeypatch.setattr(collector, "OUTPUT_JSONL", output_path)

    monkeypatch.setattr(reset, "REDIS_URL", state.redis_url)
    monkeypatch.setattr(reset, "STREAM_IN", state.keys.input_stream)
    monkeypatch.setattr(reset, "STREAM_OUT", state.keys.result_stream)
    monkeypatch.setattr(reset, "WORKERS_SET", state.keys.workers_set)
    monkeypatch.setattr(reset, "PIPELINE_METADATA_KEY", state.keys.metadata)

    return SimpleNamespace(
        client=state.client,
        keys=state.keys,
        redis_url=state.redis_url,
        output_path=output_path,
        producer=producer,
        collector=collector,
        reset=reset,
    )
