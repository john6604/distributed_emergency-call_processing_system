# Fault Tolerance

## Normal Processing

Normal task processing follows this sequence:

XREADGROUP -> pending entry -> inference -> publish result with XADD -> XACK

`XREADGROUP` assigns an input entry to a worker and places the delivery in the consumer group's Pending Entries List (PEL). The worker runs inference, publishes the result, and only then acknowledges the input with `XACK`. This ordering is relevant: acknowledging first would create a failure window in which a worker could crash before publishing, marking a task as completed without an actual output.

`XACK` removes the delivery from the PEL; it does not remove the original input-stream entry. The collector separately consumes the result stream.

## Worker Failure and XAUTOCLAIM

If a worker stops after receiving a task but before `XACK`, its delivery remains in the PEL under the dead consumer's name. Worker termination does not turn that entry into a new stream message, and `XREADGROUP` requests for new messages do not take it into account.

The final worker periodically calls `XAUTOCLAIM` to transfer idle pending entries to itself. The variable CLAIM_MILLIS defaults to 30,000 ms, the minimum pending idle time before an entry is eligible. The variable RECLAIM_INTERVAL defaults to 20 seconds, which controls how often a worker checks. These values have different purposes: a check may occur before the 30 second threshold and find nothing, so observed recovery latency can exceed either value alone.

The recovery sequence is:

dead worker owns pending task -> idle time overpasses 30 seconds -> surviving worker reclaims ownership with `XAUTOCLAIM` -> task is processed again -> result is published -> task is acknowledged

Workers also write timestamped heartbeats to a Redis hash, providing liveness visibility. However, the current implementation does not consult those heartbeats to reassign work.

## At Least Once Processing and Deduplication

The protocol provides `at least once` execution rather than exactly once execution. The main duplicate window is:

worker publishes result -> worker crashes before XACK -> task remains pending -> another worker reclaims it -> task executes and publishes again

Failures during or before result publication can also cause a pending task to be retried. Redis cannot atomically combine model inference, publication to a separate stream, and acknowledgement of the input delivery.

The collector tolerates repeated result entries by indexing them by application task ID. It replays results in stream order and keeps the first observed result for each ID. Raw result-stream entries can therefore contain duplicates even though the final exported JSONL selects one result per task ID.

Execution may occur more than once. The final exported output contains one selected result per task ID.

This is output deduplication, not exactly once processing. It also means that if repeated nondeterministic executions differ, the earliest published result is used for the final output.

## Restart Safety

### Producer

Before enqueuing, the producer computes a SHA-256 fingerprint of the input file's exact bytes and counts its validated and unique task IDs. It stores the fingerprint and expected count in batch metadata, moving the status from initializing to ready only after all `XADD` operations are  completed.

Restarting with the same ready batch validates the fingerprint, count, and ordered IDs already in the input stream, then exits without adding tasks. A different dataset or incompatible stream is rejected. If the producer stops while metadata is initializing, it does not guess whether the stream is complete. In this case a complete reset is required.

### Collector

The collector starts each run at result-stream ID 0-0. It reconstructs its memory map from existing entries, then waits for new results until the expected number of unique task IDs is completed. It does not depend on a local progress file (checkpoint). Final output uses a temporary file and atomic replacement, preventing a partial export from being presented as complete.

### Reset

Reset is an explicit destructive lifecycle operation scoped to the configured input stream, result stream, batch metadata hash, and worker-heartbeat hash. Deleting the input stream also removes its consumer group state. The implementation leaves unrelated Redis keys untouched. It does not delete the dataset or exported output file.

---

## Guarantees and Limitations

The following behavior is supported while Redis state survives:

- worker-crash recovery when another worker continues running.
- reclamation of stale entries from the PEL.
- collector restart and result-stream replay.
- idempotent producer restart for the same ready batch.
- tolerance of duplicate result entries in final export.

The current implementation does not guarantee:

- exactly once task execution or exactly once result publication.
- Redis data durability after Redis or container state is lost.
- automatic repair of a producer crash during initialization.
- multiple simultaneous batches within the same configured stream and metadata keys.

Recovery time is also threshold and polling dependent. A task must first exceed CLAIM_MILLIS, and a surviving worker must subsequently execute its periodic reclaim check.
