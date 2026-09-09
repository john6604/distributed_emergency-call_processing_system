# Distributed Emergency Call Processing System

![Python](https://img.shields.io/badge/Python-3776AB?style=flat\&logo=python\&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?style=flat&logo=docker&logoColor=white)
![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)

A distributed emergency-call processing system that extracts keywords from transcripts of a dataset. Its pipeline uses Redis Streams and consumer groups to enqueue NLP inference that are processed by independent workers, with pending-task reclamation and restart-safe result collection.

---

## Key Features

* Redis Streams–based asynchronous task distribution.
* Multiple independent NLP workers through Redis consumer groups.
* Real BART-based keyword extraction.
* `At least once` task processing.
* Recovery of stale pending work using XAUTOCLAIM.
* Restart-safe/idempotent batch initialization.
* Restart-safe collector with result deduplication.
* Docker Compose deployment and horizontal worker scaling.

---

## Architecture Overview

- **Producer:** Validates the dataset, fingerprints it using SHA-256 and initializes tasks.
- **Redis:** Stores task and result streams. Manages the consumer group delivery state.
- **Workers:** Receive tasks and process them, using BART inference. Delivers ACK states when a task is completed.
- **Collector**: Waits for all unique IDs, deduplicates results because `at least once` processing can produce duplicated IDs and writes the output to a JSON file.

---

## Quick Start

### Prerequisites

To start the application your computer must have `Docker` and `Git` installed. If any of those is not installed, you can install them in the following links:
* [Docker](https://docs.docker.com/get-started/get-docker/)
* [Git](https://git-scm.com/install/)

### Run the application

1. Clone the repository

`git clone https://github.com/john6604/distributed_emergency-call_processing_system.git`

2. Go to the root directory

`cd distributed_emergency-call_processing_system`

3. Configure environment file (If required configure HF_TOKEN)

`cp .env.example .env`

4. Run the container (You can scale the nummber of workers according to the available CPU and memory resources)

`docker compose up --build --scale worker=2`

After these steps are completed the outputs will be under the `/outputs` directory. The Hugging Face model implemented requires token authentication, the first execution downloads the model and stores it in the Hugging Face shared cache. 

### Reset

If you need to reset the application follow these steps:

1. Stop the collector

`docker compose stop worker collector`

2. Reset the application

`docker compose run --rm reset`

---

## Failure Recovery

When a worker receives a task to process, the task enters Redis PEL(Pending Entries List). After the worker marks a task as completed, it will need to acknowledge the task sending `XACK`. In the eventuality that the worker stops before sending `XACK` the task remains pending. 

Each task has an idle threshold that is of 30 seconds, after it expires, the surviving workers constantly use `XAUTOCLAIM` to reclaim tasks that have been idling for a long time. Eventually, the pending task is processed and marked as completed.

Each worker send the output before `XACK` is sent. In order to avoid duplicated IDs in the final output, the collector deduplicates the result, considering the first observed object with each ID.

---

## Experimental Results

| Workers | Tasks | Time | Throughput | Speedup | Result |
|---------|-------|------|------------|---------|--------|
| 1 | 988 | 407.110 s | 2.4269 tasks/s | 1.00x | 988/988 |
| 2 | 988 | 204.041 s | 4.8422 tasks/s | 1.9952x | 988/988 |

* **2-worker efficiency:** 99.76%.
* **1-worker sampled max RAM:** approximately 721.8 MiB.
* **2-worker sampled aggregate peak:** approximately 1.53 GiB.
* **Failure recovery:** Demonstrated with `SIGKILL` and observed the transfer using `XAUTOCLAIM`.
* **Final PEL:** 0.
* **Missing IDs:** 0.

These measures were obtained using the host CPU and represent the tested environment rather than a general performance. 

---

## Repository Structure

```text
src/emergency_processing/
├── redis_pipeline/
├── keyword_extraction.py
└── config.py

experiments/
├── centralized_http/
├── fastapi_sqlite/
└── redis_streams_variants/

tests/
data/
docs/
outputs/
```

The `experiments` directory represents early architectures and the `data` contains a `jsonl` file, which is a synthetic dataset generated to keep the application fully functional.

---

## Tests

The tests were made to protect the main deterministic lifecycle.

The proven properties were:
* producer idempotency and initialization.
* mismatched dataset rejection.
* duplicate task-ID rejection.
* collector unique completion.
* duplicate result handling.
* restart reconstruction.
* reset isolation.
* result-before-ACK ordering.

### Testing

To initialize test with docker follow the following steps:

1. Build tests

`docker compose build tests`

2. Run Tests

`docker compose run --rm tests`

The result must be: `15 passed`

These are deterministic tests. Expensive tests such as Transformer inference, SIGKILL recovery and performance when scaling are validated as controlled experiments in another section.

---

## Limitations

- Redis is not persistent nor replicated.
- Only one active batch is supported for processing.
- A producer crash during initialization requires an entire pipeline reset.
- When first ran, each worker downloads an own model copy into RAM. The Hugging Face cache avoid downloading the same model every time but does not share model memory across the workers. 
- `At least once` processing can process one task more than one time.
- 3-worker scaling was not attempted due to available memory.

---

## Documentation

* [Architecture](/docs/architecture.md)
* [Fault-Tolerance](/docs/fault-tolerance.md)
* [Experiments](/docs/experiments.md)