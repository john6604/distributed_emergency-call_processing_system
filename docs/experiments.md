# Experimental Validation

## Environment

The final Redis Streams pipeline was validated with real model inference in the following recorded environment:

| Property | Recorded value |
| --- | --- |
| Host logical CPUs | 16 |
| Host RAM | 15.71 GiB |
| Docker memory limit | 7.614 GiB |
| Inference device | CPU only |
| Python | 3.11.16 |
| Transformers | 4.57.6 |
| Redis server | 7.4.11 |

The original benchmark dataset contained 988 records and is not included in the public repository because it is subject to confidentiality restrictions. Only aggregate measurements from that workload are reported below. The public repository uses a fully synthetic sample dataset.

## Real-Model Validation

Validation used UDA-LIDI/barto_emergency_multi_purpose, a gated BART model loaded through authenticated Hugging Face access. The experiment records confirm that the real BartForConditionalGeneration model loaded and performed inference. Credentials and token values were not captured in the metrics.

A ten task smoke test produced 10/10 unique results in both cold and warm runs. Cold startup to model readiness took 74.459 seconds and included the model download. The Hugging Face cache grew to approximately 537.9 MiB. Warm startup took 4.621 seconds, reused that disk cache, and showed no model redownload. The cache avoids transferring the same model files again, but each worker still constructs its own memory model instance.

## Scaling Results

Both scaling runs processed the same 988 records that belonged to the original benchmark. The timing boundary was successful producer completion to collector exit and included worker model loading.

| Workers | Tasks | Time | Throughput | Speedup |
| ------: | ----: | ---: | ---------: | ------: |
| 1 | 988 | 407.110 s | 2.4269 tasks/s | 1.00x |
| 2 | 988 | 204.041 s | 4.8422 tasks/s | 1.9952x |

The two-worker parallel efficiency was 99.76%. The one-worker run had an observed sampled maximum of approximately 721.8 MiB. The two-worker sampled aggregate peak was approximately 1.53 GiB, consistent with independent model copies rather than shared model memory.

These are measurements from the tested CPU environment, not universal performance guarantees. A three-worker real-model scaling run was not attempted because there was not enough memory available in the host CPU.

## Worker Failure Recovery

The failure experiment tested the mechanism directly rather than inferring recovery from eventual completion. Two real-model workers processed the 988 tasks. The experiment first observed pending work in Redis under one worker's consumer identity, then terminated that worker's Python process with an actual `SIGKILL`. Redis inspection immediately after termination showed that its unacknowledged task remained in the PEL under the dead consumer's name.

After the configured idle threshold elapsed, the surviving worker reclaimed the task from PEL with `XAUTOCLAIM`. It processed the reclaimed work, published the result, and acknowledged the input delivery. The collector subsequently completed all 988 expected unique results, and the final PEL count was zero. No confidential application task ID, Redis message ID, transcript, or worker identifier is reproduced here.

The observed sequence was:

batch initialized -> worker owns pending task -> worker terminated with SIGKILL -> task remains pending -> idle threshold expires -> surviving worker reclaims stale task -> task processed and acknowledged -> collector reaches 988/988

This establishes that ownership and pending state survived worker death and that `XAUTOCLAIM` performed the transfer. Heartbeat data was available for visibility but was not the recovery trigger.

## Output Integrity

The completed failure run had the following aggregate integrity results:

| Check | Result |
| --- | ---: |
| Expected tasks | 988 |
| Final records | 988 |
| Unique final IDs | 988 |
| Missing IDs | 0 |
| Unexpected IDs | 0 |
| Final duplicate IDs | 0 |
| Final PEL entries | 0 |
| Collector exit | Successful |

The raw result stream also contained 988 entries with 988 unique task IDs in this particular run, so no duplicate result occurred during the observed failure. Duplicates nevertheless remain valid under the `at least once` processing, especially if a crash occurs after result publication but before `XACK`. Collector deduplication is therefore still required.

## Measurement Limitations

- Measurements came from one host and one Docker environment.
- Inference was CPU-only, so no accelerator comparison was performed.
- Three-worker scaling was not tested because of memory constraints.
- Memory values were sampled observations, not exact theoretical peaks.
- Throughput and startup values are specific to the recorded environment, model, and workload.
- The original benchmark is confidential and unavailable in the public repository.
- The public dataset is fully synthetic and is intended for functional use, not reproduction of the confidential benchmark.
