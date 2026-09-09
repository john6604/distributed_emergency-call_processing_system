# Architecture

## System Overview

The final system is a distributed NLP pipeline that extracts keywords from emergency-call transcripts. Its central architectural boundary is that Redis coordinates task delivery and processing state, Redis does not perform NLP inference. Inference runs independently for each worker using the BART model configured by the variable MODEL_NAME.

The pipeline connects a JSONL input dataset, a producer, a Redis task stream, a consumer group, one or more workers, a result stream, and a collector that creates the final JSONL output. The public repository uses a synthetic sample dataset. The original benchmark dataset contained 988 records and is not included in the public repository because it is subject to confidentiality restrictions.

---

## Components

### Producer

The producer reads the input JSONL in order and requires each non-empty record to contain an application task ID and text. It rejects duplicate task IDs before writing to Redis. A SHA-256 fingerprint is calculated over the dataset's exact bytes, so content or formatting changes select a different workload even when the task count is the same.

For a new workload, the producer claims a single batch metadata hash with status initializing, records the fingerprint and expected task count, appends each task to the input stream with `XADD`, and finally changes the status to ready.

A restart against a ready batch is idempotent only when the fingerprint, expected count, and ordered task IDs in the input stream still match. In that case, no tasks are enqueued again. A different dataset is rejected. An interrupted initialization remains in initializing state and requires the explicit reset operation, which is a known limitation. This is due to Redis containing only part of the workload. One metadata key represents one active batch, so concurrent independent batches are not currently supported.

### Redis Streams and Consumer Group

The input stream stores task ID and text fields. The result stream stores task ID and serialized keywords, the formatting for the keywords is `["keyword1", "keyword2", keyword3", ...]`. All workers join one consumer group. Redis distributes new stream entries among its consumers when they call `XREADGROUP`.

Delivery through `XREADGROUP` adds an unacknowledged entry to the group's Pending Entries List (PEL). The PEL records ownership and idle time, which enables recovery if a worker crashes. `XACK` removes a delivery from the PEL for that group, but it does not delete the original entry from the Redis Stream.

### Workers

Each worker is an independent process with a consumer identity derived from its host name and process ID. At startup, it creates the consumer group if necessary, loads its own tokenizer and BART model instance, and then alternates between reclaiming eligible pending entries and reading new entries with `XREADGROUP`.

Messages are processed sequentially within each worker: inference is moved to a thread so it does not block the async event loop, but the worker awaits each result before advancing. Horizontal concurrency therefore comes primarily from running multiple worker processes. Each worker publishes its output to the result stream with `XADD` before acknowledging the input with `XACK`. If processing fails before acknowledgement, the input remains pending and can later be reclaimed. However, the result is already sent, which is the `at least once` processing that the collector manages.

The Compose configuration mounts one Hugging Face cache volume into all workers. This shares downloaded model files on disk and reduces repeated downloads, but it does not share loaded model memory.

### Collector

The collector first waits for ready batch metadata and obtains the expected task count. It reads the result stream from 0-0, including results that existed before the collector started. This replay means that everytime the application is restarted, the collector will reconstruct progress without any local chekpoint.

Completion is based on unique application task IDs rather than the raw number of Redis entries. If duplicate results exist, the first result observed in stream order wins; this is the result of using `at least once` processing. The collector waits until the expected number of unique IDs has been collected, sorts records numerically by task ID, and writes JSONL to a temporary file. An atomic replacement then exposes the completed output.

Deduplication controls the exported representation. It does not make task execution exactly once. A worker can execute and publish the same task more than once within the `at least once` processing model.

---

## Data Flow

1. The producer reads the JSONL dataset, validates unique task IDs, and calculates its dataset fingerprint.
2. It creates batch metadata in initializing state with the expected task count.
3. It appends tasks to the input Redis Stream with `XADD` and marks the batch state as ready.
4. Workers join the consumer group and request new tasks with `XREADGROUP`.
5. Redis assigns deliveries and records unacknowledged entries in the PEL.
6. A worker runs BART inference for one transcript.
7. The worker publishes keywords to the result stream with `XADD`, then acknowledges the input with `XACK`.
8. Other workers may reclaim stale pending work with `XAUTOCLAIM`.
9. The collector replays the result stream and counts unique task IDs until the expected total is present.
10. The collector writes the numerically ordered final JSONL through atomic file replacement.

---

## Architecture Evolution

* **Centralized HTTP dispatcher.** The first iteration held an explicit list of inference servers, checked their health, and routed batches in round-robin order. Task routing and retry decisions therefore remained in a dispatcher that needed direct awareness of worker endpoints.

* **FastAPI and SQLite orchestrator.** The next iteration represented tasks as SQLite rows owned by a FastAPI service. Workers claimed tasks and returned results through HTTP, while the application orchestrator serialized claims and exposed stale-task reclamation. This used the Microservices architecture.

* **Redis Streams variants.** The Redis experiments moved delivery, consumer ownership, and pending state from an application API into Redis Streams and consumer groups. The historical worker used PEL inspection and `XCLAIM` to recover stale entries.

* **Final Redis Streams architecture.** The current implementation uses `XAUTOCLAIM` and adds explicit batch metadata, dataset validation, producer resumability, restart-safe result collection, atomic output replacement, and a scoped reset operation.

---

## Design Trade-offs

| Decision | Benefit | Trade-off |
| --- | --- | --- |
| Redis consumer groups | Coordinate delivery and expose pending ownership without a custom task API | Correct recovery still depends on acknowledgement order and surviving Redis state |
| Independent worker models | Isolate workers and make process-level scaling straightforward | Model memory grows with worker count |
| `At least once` execution | Keeps unacknowledged work recoverable after worker failure | Inference and result publication may be repeated |
| Collector replay from 0-0 | Reconstructs progress after restart without a checkpoint file | Startup work grows with result-stream history |
| One active batch | Keeps metadata and completion rules simple | Another dataset is rejected when there is already an active batch |
