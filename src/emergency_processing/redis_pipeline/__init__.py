import os


BATCH_STATUS_INITIALIZING = "initializing"
BATCH_STATUS_READY = "ready"
DEFAULT_PIPELINE_METADATA_KEY = "emergency_processing:batch"


def pipeline_metadata_key() -> str:
    return os.getenv("PIPELINE_METADATA_KEY", DEFAULT_PIPELINE_METADATA_KEY)


def reset_instruction() -> str:
    return (
        "Run `python -m emergency_processing.redis_pipeline.reset` or "
        "`docker compose run --rm reset`, then initialize the batch again."
    )
