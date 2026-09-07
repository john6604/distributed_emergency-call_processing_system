FROM python:3.11-slim-bookworm AS base

ARG TORCH_INDEX_URL=https://download.pytorch.org/whl/cpu

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    EMERGENCY_PROCESSING_ROOT=/app \
    HF_HOME=/cache/huggingface

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN python -m pip install --index-url "${TORCH_INDEX_URL}" "torch>=2.5,<3" \
    && python -m pip install .

FROM base AS test

RUN python -m pip install ".[dev]"

COPY tests ./tests

CMD ["python", "-m", "pytest"]

FROM base AS runtime

CMD ["python", "-m", "emergency_processing.redis_pipeline.worker"]
