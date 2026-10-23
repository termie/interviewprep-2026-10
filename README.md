# Interview Preparation (2026-10)

A curated collection of production-grade and interview-ready Python implementations covering core systems programming and backend engineering topics.

GitHub Repository: [`termie/interviewprep-2026-10`](https://github.com/termie/interviewprep-2026-10)

---

## Setup & Running Examples

The project requires **Python 3.15** and uses [`uv`](https://github.com/astral-sh/uv) for dependency and environment management.

Run any example directly using `uv run`:
```bash
uv run job_queues/async_job_queue.py
uv run url_shortener/interview_url_shortener.py
uv run distributed_cron/interview_distributed_cron.py
uv run client_server/client_server_websocket.py
uv run rate_limiters/async_token_bucket.py
```
`uv` will automatically create and sync the virtual environment using Python 3.15.

---

## Topic Guides & Categories

### 1. Rate Limiters ([`RATE_LIMITERS.md`](https://github.com/termie/interviewprep-2026-10/blob/main/RATE_LIMITERS.md))
Deep dive into rate limiting algorithms, trade-offs, and blocking vs. non-blocking (denying) modes.
* [`rate_limiters/async_token_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_token_bucket.py) - Classic token bucket with burst capacity, throttling (`blocking=True`), and fast denial (`blocking=False`).
* [`rate_limiters/async_leaky_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_leaky_bucket.py) - Traffic-shaping queue providing constant outflow pacing and overflow rejection.
* [`rate_limiters/async_sliding_counter.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_sliding_counter.py) - Memory-efficient sliding window counter using weighted time decay.
* [`rate_limiters/async_gcra.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_gcra.py) - Generic Cell Rate Algorithm tracking a single Theoretical Arrival Time (TAT).

### 2. Job Queues with Retries ([`JOB_QUEUES.md`](https://github.com/termie/interviewprep-2026-10/blob/main/JOB_QUEUES.md))
Architecture guide on retry loops, exponential backoff, jitter, and dead-letter queues (DLQ).
* [`job_queues/async_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/async_job_queue.py) - Dual-queue async job queue with decoupled scheduler heap, exponential backoff with jitter, and DLQ.
* [`job_queues/sqlite_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/sqlite_job_queue.py) - Persistent, crash-resilient job queue with ACID transactions and WAL mode.
* [`job_queues/interview_job_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_job_queue.py) - Minimal (~40 lines) async job queue with sentinel shutdown.
* [`job_queues/interview_priority_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_priority_queue.py) - Priority queue with tie-breaking sequence counter and `inf` sentinel shutdown.
* [`job_queues/interview_dedup_queue.py`](https://github.com/termie/interviewprep-2026-10/blob/main/job_queues/interview_dedup_queue.py) - In-flight deduplication rejecting duplicate job IDs across execution and retry backoff.

### 3. Client-Server Networking ([`CLIENT_SERVER.md`](https://github.com/termie/interviewprep-2026-10/blob/main/CLIENT_SERVER.md))
Networking guide comparing raw TCP message framing, ZeroMQ, WebSockets, and Asyncio Streams.
* [`client_server/client_server_raw_tcp.py`](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_raw_tcp.py) - BSD TCP sockets using `socket.makefile()` for newline-delimited stream framing.
* [`client_server/client_server_zmq.py`](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_zmq.py) - ZeroMQ REQ/REP pattern with discrete message framing and automatic reconnections.
* [`client_server/client_server_websocket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_websocket.py) - Async full-duplex WebSocket client and server.
* [`client_server/client_server_asyncio_streams.py`](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_asyncio_streams.py) - Standard library async TCP streams with `readline()` framing.

### 4. URL Shortening Service ([`URL_SHORTENER.md`](https://github.com/termie/interviewprep-2026-10/blob/main/URL_SHORTENER.md))
System design guide covering Base62 encoding, 301 vs. 302 redirects, KGS range allocation, caching, and capacity planning.
* [`url_shortener/interview_url_shortener.py`](https://github.com/termie/interviewprep-2026-10/blob/main/url_shortener/interview_url_shortener.py) - Compact (~60 lines) in-memory Base62 URL shortener with custom alias and TTL support.
* [`url_shortener/base62_range_shortener.py`](https://github.com/termie/interviewprep-2026-10/blob/main/url_shortener/base62_range_shortener.py) - Distributed range-allocated Key Generation Service (KGS) simulation with multi-node leases and LRU cache-aside redirects.
* [`url_shortener/hash_collision_shortener.py`](https://github.com/termie/interviewprep-2026-10/blob/main/url_shortener/hash_collision_shortener.py) - SHA-256 truncation approach with database collision detection and salting/probing.

### 5. Distributed Cron Service ([`DISTRIBUTED_CRON.md`](https://github.com/termie/interviewprep-2026-10/blob/main/DISTRIBUTED_CRON.md))
Architecture guide on distributed scheduling, clock drift, at-least-once execution, fencing tokens, and sharding.
* [`distributed_cron/interview_distributed_cron.py`](https://github.com/termie/interviewprep-2026-10/blob/main/distributed_cron/interview_distributed_cron.py) - Compact (~70 lines) heartbeat lease election and priority heap cron scheduler with automatic failover.
* [`distributed_cron/leader_lease_cron.py`](https://github.com/termie/interviewprep-2026-10/blob/main/distributed_cron/leader_lease_cron.py) - Active-passive leader lease scheduler with fencing tokens to defeat zombie leaders and worker deduplication keys.
* [`distributed_cron/partitioned_ring_cron.py`](https://github.com/termie/interviewprep-2026-10/blob/main/distributed_cron/partitioned_ring_cron.py) - Decentralized partitioned cron using a Consistent Hashing Ring with virtual nodes and dynamic rebalancing.

