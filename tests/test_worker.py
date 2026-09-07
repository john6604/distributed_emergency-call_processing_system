import asyncio
import importlib
import json
import sys
from unittest.mock import MagicMock

import pytest
from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


@pytest.fixture
def worker_module(monkeypatch):
    module_name = "emergency_processing.redis_pipeline.worker"
    sys.modules.pop(module_name, None)
    tokenizer = MagicMock(name="tokenizer")
    model = MagicMock(name="model")
    model.to.return_value = model
    monkeypatch.setattr(AutoTokenizer, "from_pretrained", lambda *args, **kwargs: tokenizer)
    monkeypatch.setattr(
        AutoModelForSeq2SeqLM,
        "from_pretrained",
        lambda *args, **kwargs: model,
    )
    worker = importlib.import_module(module_name)
    yield worker
    sys.modules.pop(module_name, None)


def test_worker_publishes_result_before_acknowledging_message(
    monkeypatch, worker_module
):
    worker = worker_module
    events = []

    class RecordingRedis:
        async def xadd(self, stream, fields):
            events.append(("publish", stream, fields))

        async def xack(self, stream, group, message_id):
            events.append(("acknowledge", stream, group, message_id))

    monkeypatch.setattr(worker, "extract_keywords", lambda text: ["medical", "urgent"])

    asyncio.run(
        worker.process_message(
            RecordingRedis(),
            "123-0",
            {"id": "42", "text": "medical emergency"},
        )
    )

    assert [event[0] for event in events] == ["publish", "acknowledge"]
    assert events[0][1] == worker.STREAM_OUT
    assert events[0][2] == {
        "id": "42",
        "keywords": json.dumps(["medical", "urgent"], ensure_ascii=False),
    }
    assert events[1] == (
        "acknowledge",
        worker.STREAM_IN,
        worker.CONSUMER_GROUP,
        "123-0",
    )
