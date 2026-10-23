# Interview Preparation Tasks

## Rate Limiters
- [x] Research rate limiting algorithms and trade-offs
- [x] Create async token bucket rate limiter example (`rate_limiters/async_token_bucket.py`)
- [x] Create async leaky bucket rate limiter example (`rate_limiters/async_leaky_bucket.py`)
- [x] Create async sliding window counter rate limiter example (`rate_limiters/async_sliding_counter.py`)
- [x] Create async GCRA rate limiter example (`rate_limiters/async_gcra.py`)
- [x] Create rate limiting comparison table markdown file (`RATE_LIMITERS.md`)
- [x] Add non-blocking / immediate denial mode (raising `RateLimitExceeded` with `retry_after`) to all 4 limiters
- [x] Update `RATE_LIMITERS.md` with documentation on blocking vs. denying modes
- [x] Test and verify all 4 rate limiter scripts

## Job Queue with Retries
- [x] Create async in-memory job queue with exponential backoff & Dead-Letter Queue (DLQ) (`job_queues/async_job_queue.py`)
- [x] Create persistent / transactional SQLite job queue with retries (`job_queues/sqlite_job_queue.py`)
- [x] Document job queue architectures and retry patterns in `JOB_QUEUES.md`
- [x] Create interview-friendly minimal job queue example (`job_queues/interview_job_queue.py`)
- [x] Create interview-friendly priority job queue example (`job_queues/interview_priority_queue.py`)
- [x] Create interview-friendly deduplicated job queue example (`job_queues/interview_dedup_queue.py`)
- [x] Test and verify all job queue scripts

## Client-Server Networking (Interview Implementations)
- [x] Create Raw TCP socket client/server with message framing (`client_server/client_server_raw_tcp.py`)
- [x] Create ZeroMQ REQ/REP client/server (`client_server/client_server_zmq.py`)
- [x] Create WebSocket client/server (`client_server/client_server_websocket.py`)
- [x] Create asyncio Streams client/server (`client_server/client_server_asyncio_streams.py`)
- [x] Test and verify runnable client/server examples
- [x] Document client/server architecture & framing in `CLIENT_SERVER.md`
- [x] Add repository overview in `README.md`
- [x] Reorganize into category top-level directories (`rate_limiters/`, `job_queues/`, `client_server/`)
- [x] Update links to GitHub scheme `termie/interviewprep-2026-10`
