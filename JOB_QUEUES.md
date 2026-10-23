# Job Queues with Retries: Architecture & Guide

This document explains the design principles, retry strategies, and failure-handling mechanisms for robust job queues in Python, referencing runnable implementations in the [`job_queues/`](https://github.com/termie/interviewprep-2026-10/tree/main/job_queues) directory.

GitHub Repository: [`termie/interviewprep-2026-10`](https://github.com/termie/interviewprep-2026-10)

---

## Core Components of a Job Queue with Retries

A reliable background job queue consists of five core architectural layers:

```
+--------------------------------------------------------------------------+
|                               PRODUCER                                   |
|                        queue.enqueue(task, payload)                      |
+--------------------------------------------------------------------------+
                                     |
                                     v
+------------------------+   Failure   +------------------------------------+
|      READY QUEUE       | ----------> |           DELAYED HEAP             |
| (Immediate execution)  |             | (run_at = now + exponential_delay) |
+------------------------+             +------------------------------------+
           |                                             |
           | Worker claim                                | Timer elapsed
           v                                             v
+------------------------+                     +--------------------+
|      WORKER POOL       |                     |     SCHEDULER      |
|  (Executes task coros) |                     +--------------------+
+------------------------+
           |
           +--> Success: status = 'COMPLETED'
           |
           +--> Attempts >= max_retries:
                   v
         +--------------------+
         | DEAD-LETTER QUEUE  |
         |       (DLQ)        |
         +--------------------+
```

1. **Ready Queue**: Holds tasks that can be executed immediately by available workers.
2. **Worker Pool**: One or more worker threads, processes, or async coroutines consuming from the ready queue.
3. **Delayed Heap / Scheduled Index**: Holds retrying tasks waiting for their backoff countdown to elapse before re-entering the ready queue.
4. **Retry Engine with Exponential Backoff + Jitter**: Calculates retry delays dynamically.
5. **Dead-Letter Queue (DLQ)**: Quarantine queue holding poison pills (tasks that permanently exceeded `max_retries`) so they can be inspected, alerted on, or replayed manually.

---

## Retry Strategies

### 1. Exponential Backoff with Jitter
Hardcoded retry intervals (e.g., retrying every 1 second) can cause the **thundering herd problem** if a downstream service recovers from an outage. Instead, use exponential backoff with randomized jitter:

$$\text{delay} = \min\left(\text{max\_delay}, \text{base\_delay} \times 2^{(\text{attempt} - 1)}\right) \times \text{jitter}$$

* **Full Jitter**: $\text{jitter} \sim \text{Uniform}(0, 1)$
* **Equal / Decorrelated Jitter**: $\text{jitter} \sim \text{Uniform}(0.8, 1.2)$

### 2. Transient vs. Permanent Failures
Not all errors should be retried:
* **Retryable (Transient)**: Network timeouts, HTTP 500/502/503/504, database deadlock, rate limit 429.
* **Non-Retryable (Permanent)**: Invalid JSON schema, HTTP 400 Bad Request, 401 Unauthorized, 404 Not Found. These should immediately move to the DLQ to avoid wasting worker capacity.

---

## Implementations in this Workspace

### 1. In-Memory Async Job Queue ([`job_queues/async_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/async_job_queue.py))
* **Design**:
  * Decouples the ready queue (`asyncio.Queue`) from delayed retries (`heapq`).
  * A dedicated background scheduler promotes delayed retries into the ready queue as their timer expires, preventing workers from blocking.
  * In-memory `dead_letter_queue` for poisoned tasks.
* **Run Demo**:
  ```bash
  python3 job_queues/async_job_queue.py
  ```

### 2. Persistent SQLite Job Queue ([`job_queues/sqlite_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/sqlite_job_queue.py))
* **Design**:
  * **Zero dependencies**: Uses Python's built-in `sqlite3`.
  * **Crash-resilient**: Tasks survive application crashes and server reboots.
  * **Atomic claiming**: Uses `BEGIN IMMEDIATE` transactions to prevent duplicate worker pickup.
  * **Indexed polling**: Uses `(status, run_at)` index so workers pull ready jobs efficiently.
  * **DLQ State**: Tasks transition from `'pending'` $\rightarrow$ `'running'` $\rightarrow$ `'completed'` or `'dead_letter'`.
* **Run Demo**:
  ```bash
  python3 job_queues/sqlite_job_queue.py
  ```

### 3. Interview-Ready Minimal Implementations
* [`job_queues/interview_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_job_queue.py) - Minimal (~40 lines) queue with sentinel shutdown.
* [`job_queues/interview_priority_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_priority_queue.py) - Priority queue with tie-breaking sequence counter and `inf` sentinel shutdown.
* [`job_queues/interview_dedup_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_dedup_queue.py) - In-flight deduplication rejecting duplicate job IDs across execution and retry backoff.

---

## Comparison: Architecture Selection

| Feature | In-Memory Async Queue | SQLite Persistent Queue | Redis / Celery / BullMQ |
| :--- | :--- | :--- | :--- |
| **Persistence** | None (Lost on restart) | Full (ACID on disk) | Full (AOF / RDB snapshot) |
| **Throughput** | $100,000+$ ops/sec | $5,000 - 20,000$ ops/sec (WAL) | $50,000+$ ops/sec |
| **Dependencies** | None (pure Python standard library) | None (pure Python standard library) | Redis server + client libs |
| **Multi-Process Safe** | No (single event loop) | Yes (via SQLite file locking) | Yes (distributed across network) |
| **Best Used For** | Fast in-process task offloading | Microservices, CLI tools, embedded workers | Large distributed systems |
