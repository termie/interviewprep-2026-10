# Rate Limiting Algorithms: Guide & Comparison

This document provides a comparative reference for rate limiting algorithms commonly used in API clients, gateways, and distributed services, along with links to their runnable Python implementations in the [`rate_limiters/`](https://github.com/termie/interviewprep-2026-10/tree/main/rate_limiters) directory.

GitHub Repository: [`termie/interviewprep-2026-10`](https://github.com/termie/interviewprep-2026-10)

---

## Comparison Matrix

| Algorithm | Burst Support | State / Memory Overhead | Window Accuracy | Implementation Complexity | Distributed Friendly? | Primary Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **[Token Bucket](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_token_bucket.py)** | Yes (up to capacity) | Minimal ($O(1)$) | High | Low | Yes (Atomic update of token count + timestamp) | General API clients, handling third-party REST APIs |
| **[Leaky Bucket](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_leaky_bucket.py)** | No (strictly smooth egress) | Moderate ($O(\text{queue capacity})$) | Exact pacing | Medium | Harder (requires distributed queue) | Traffic shaping, database write throttling, video streaming |
| **[Sliding Window Counter](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_sliding_counter.py)** | Moderate (weighted decay) | Minimal ($O(1)$) | High (~99% approximation) | Low | Yes (Hash/two counters per user key) | High-scale API gateways (Cloudflare, Kong) |
| **[GCRA (Generic Cell Rate)](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_gcra.py)** | Yes (defined by burst tolerance) | Minimal (single timestamp $O(1)$) | Exact | Low-Medium | Excellent (Single key write in Redis `CL.THROTTLE`) | Redis-backed distributed services, high-throughput microservices |
| **Sliding Window Log** | No | High ($O(N)$ requests in window) | 100% exact | Low | Poor at scale (sorted sets grow with volume) | Low-volume, security-critical endpoints (e.g. login brute-force prevention) |
| **Fixed Window Counter** | Yes (2x spike at window boundary) | Minimal ($O(1)$ single integer) | Low (susceptible to boundary bursts) | Very Low | Yes (`INCR` with TTL) | Coarse quotas (e.g. daily/monthly tier limits) |
| **Concurrency / In-Flight Limiter** | N/A (limits simultaneous active tasks) | Minimal ($O(1)$ semaphore) | N/A | Very Low | Yes (distributed semaphore) | Resource protection (LLM inference, DB pool exhaustion) |

---

## Blocking (Throttling) vs. Denying (Fail-Fast) Modes

All example implementations support two distinct operational behaviors controlled via the `blocking` flag:

| Mode | Behavior | When to Use |
| :--- | :--- | :--- |
| **Blocking / Waiting** (`blocking=True`) | Pauses the coroutine (`await asyncio.sleep(...)`) until capacity or window decay allows the request to pass. | **Outbound API clients** calling third-party services (e.g., Stripe, OpenAI) to prevent hitting HTTP 429 errors. |
| **Denying / Rejecting** (`blocking=False`) | Immediately raises a `RateLimitExceeded` exception carrying a `retry_after` float attribute. | **Inbound API servers and gateways** (e.g. FastAPI, Starlette) returning HTTP `429 Too Many Requests` with a `Retry-After` header. |

### Handling Denials in API Servers
When in denying mode, catch `RateLimitExceeded` to form standard HTTP 429 responses:

```python
from rate_limiters.async_token_bucket import AsyncTokenBucket, RateLimitExceeded

limiter = AsyncTokenBucket(rate=10.0, capacity=20.0, blocking=False)

async def api_handler(request):
    try:
        async with limiter:
            return await process_request(request)
    except RateLimitExceeded as exc:
        return Response(
            status_code=429,
            content="Too Many Requests",
            headers={"Retry-After": str(int(exc.retry_after) + 1)}
        )
```

---

## Detailed Algorithm Breakdown

### 1. Token Bucket
* **Reference Implementation**: [`rate_limiters/async_token_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_token_bucket.py)
* **How it works**: Tokens replenish continuously at a constant `rate` up to a maximum `capacity`. Incoming calls immediately deduct tokens.
  * *Blocking mode*: Waits until tokens replenish.
  * *Denying mode*: Raises `RateLimitExceeded(retry_after=...)` if tokens are insufficient.
* **When to use**: Standard default choice for outbound API clients and standard rate limits.

### 2. Leaky Bucket
* **Reference Implementation**: [`rate_limiters/async_leaky_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_leaky_bucket.py)
* **How it works**: Incoming calls enter a fixed-size FIFO queue. A background worker drains items at a strictly constant rate.
  * *Blocking mode*: Enqueues up to capacity; raises `BucketOverflowError` if capacity is exceeded.
  * *Denying mode*: Rejects immediately if the bucket is currently busy or queued.
* **When to use**: Traffic shaping when downstream systems require a strictly constant outflow rate.

### 3. Sliding Window Counter
* **Reference Implementation**: [`rate_limiters/async_sliding_counter.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_sliding_counter.py)
* **How it works**: Approximates a sliding window by taking a weighted sum of the current and previous window counts.
  * *Blocking mode*: Waits until the previous window count decays enough to grant capacity.
  * *Denying mode*: Rejects immediately with `retry_after` set to the exact decay/window duration.
* **When to use**: Multi-tenant API gateways needing low-memory rolling rate limits without boundary spike vulnerabilities.

### 4. GCRA (Generic Cell Rate Algorithm)
* **Reference Implementation**: [`rate_limiters/async_gcra.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_gcra.py)
* **How it works**: Tracks a single Theoretical Arrival Time (TAT). If a request arrives further ahead than the allowed burst tolerance:
  * *Blocking mode*: Waits for TAT to decay within tolerance.
  * *Denying mode*: Raises `RateLimitExceeded` with `retry_after = (TAT - burst_tolerance) - now`.
* **When to use**: Distributed rate limiters backed by Redis (e.g., Redis Cell `CL.THROTTLE`).

---

## How to Choose

1. **Calling external APIs from a Python client?** $\rightarrow$ Use **Token Bucket** ([`rate_limiters/async_token_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_token_bucket.py)) with `blocking=True`.
2. **Protecting an incoming FastAPI/Starlette endpoint?** $\rightarrow$ Use **Token Bucket** ([`rate_limiters/async_token_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_token_bucket.py)) or **Sliding Window Counter** ([`rate_limiters/async_sliding_counter.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_sliding_counter.py)) with `blocking=False`.
3. **Need strict constant rate egress without spikes?** $\rightarrow$ Use **Leaky Bucket** ([`rate_limiters/async_leaky_bucket.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_leaky_bucket.py)).
4. **Distributed rate limiting using Redis state?** $\rightarrow$ Use **GCRA** ([`rate_limiters/async_gcra.py`](https://github.com/termie/interviewprep-2026-10/blob/main/rate_limiters/async_gcra.py)).
