"""Async Leaky Bucket Rate Limiter Example.

The Leaky Bucket algorithm smooths bursty traffic into a strictly constant
egress rate (traffic shaping).

Supports:
1. Blocking / Queueing mode: incoming requests enter a FIFO queue up to capacity
   and are dispatched at a constant leak rate. If the queue overflows, requests
   are denied with RateLimitExceeded (BucketOverflowError).
2. Denying mode (blocking=False): if the bucket/worker is already busy or cannot
   serve immediately, requests are rejected immediately with RateLimitExceeded
   and a calculated retry_after.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable, Coroutine, TypeVar

T = TypeVar("T")


class RateLimitExceeded(Exception):
    """Raised when an operation is denied because the rate limit has been exceeded."""

    def __init__(self, retry_after: float, message: str | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(
            message or f"Rate limit exceeded. Retry after {retry_after:.2f}s."
        )


class BucketOverflowError(RateLimitExceeded):
    """Raised when the leaky bucket queue exceeds capacity."""

    pass


class AsyncLeakyBucket:
    """Async Leaky Bucket dispatcher.

    :param leak_rate: Number of requests to leak/dispatch per second.
    :param capacity: Maximum number of pending requests the bucket can hold.
    :param blocking: If True, queues requests up to capacity and leaks them smoothly.
                     If False, rejects requests immediately when the queue is non-empty.
    """

    def __init__(
        self, leak_rate: float, capacity: int = 1, blocking: bool = True
    ) -> None:
        if leak_rate <= 0:
            raise ValueError("leak_rate must be positive")
        if capacity <= 0:
            raise ValueError("capacity must be a positive integer")

        self.leak_rate = float(leak_rate)
        self.leak_interval = 1.0 / self.leak_rate
        self.capacity = capacity
        self.blocking = blocking

        self._queue: asyncio.Queue[
            tuple[Callable[[], Coroutine[Any, Any, Any]], asyncio.Future[Any]]
        ] = asyncio.Queue(maxsize=capacity)
        self._worker_task: asyncio.Task[None] | None = None
        self._running = False
        self._last_leak_time = 0.0
        self._is_processing = False

    def start(self) -> None:
        """Start the background leak consumer."""
        if not self._running:
            self._running = True
            self._worker_task = asyncio.create_task(self._worker())

    async def _worker(self) -> None:
        """Continuously leaks requests at the strictly constant leak_interval."""
        while self._running:
            try:
                task_fn, future = await self._queue.get()
            except asyncio.CancelledError:
                break

            self._is_processing = True
            if not future.cancelled():
                try:
                    result = await task_fn()
                    future.set_result(result)
                except Exception as exc:
                    future.set_exception(exc)

            self._queue.task_done()
            self._last_leak_time = time.monotonic()
            await asyncio.sleep(self.leak_interval)
            self._is_processing = False

    async def submit(
        self,
        coro_fn: Callable[..., Coroutine[Any, Any, T]],
        *args: Any,
        blocking: bool | None = None,
        **kwargs: Any,
    ) -> T:
        """Submit a coroutine function to the bucket.

        :param blocking: If False, denies immediately unless queue is empty and idle.
        :raises RateLimitExceeded: When limit/capacity is hit in non-blocking mode.
        :raises BucketOverflowError: When queue capacity is full in blocking mode.
        """
        if not self._running:
            self.start()

        should_block = self.blocking if blocking is None else blocking
        current_queue_len = self._queue.qsize()

        # Denying mode: reject immediately if busy or has queued items
        if not should_block and (current_queue_len > 0 or self._is_processing):
            now = time.monotonic()
            elapsed_since_last = now - self._last_leak_time if self._last_leak_time else 0.0
            retry_after = max(
                0.01,
                (current_queue_len + (1 if self._is_processing else 0)) * self.leak_interval
                - elapsed_since_last,
            )
            raise RateLimitExceeded(
                retry_after=retry_after,
                message=f"Leaky bucket busy (denying mode). Retry after {retry_after:.2f}s.",
            )

        loop = asyncio.get_running_loop()
        future: asyncio.Future[T] = loop.create_future()

        async def call_wrapper() -> T:
            return await coro_fn(*args, **kwargs)

        try:
            self._queue.put_nowait((call_wrapper, future))
        except asyncio.QueueFull:
            retry_after = (self.capacity + 1) * self.leak_interval
            raise BucketOverflowError(
                retry_after=retry_after,
                message=f"Leaky bucket overflow: capacity of {self.capacity} items reached.",
            )

        return await future

    async def close(self) -> None:
        """Gracefully drain remaining queued requests and shut down worker."""
        if self._running:
            await self._queue.join()
            self._running = False
            if self._worker_task:
                self._worker_task.cancel()
                try:
                    await self._worker_task
                except asyncio.CancelledError:
                    pass

    async def __aenter__(self) -> AsyncLeakyBucket:
        self.start()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        await self.close()


# --- Demonstration ---
async def simulate_api_call(request_id: int, start_time: float) -> str:
    elapsed = time.monotonic() - start_time
    print(f"[{elapsed:5.2f}s] Processing API call #{request_id}")
    return f"Response #{request_id}"


async def run_client(
    bucket: AsyncLeakyBucket, req_id: int, start_time: float, blocking: bool
) -> None:
    try:
        res = await bucket.submit(simulate_api_call, req_id, start_time, blocking=blocking)
        elapsed = time.monotonic() - start_time
        print(f"[{elapsed:5.2f}s] Client #{req_id} received: {res}")
    except RateLimitExceeded as exc:
        elapsed = time.monotonic() - start_time
        print(f"[{elapsed:5.2f}s] [DENIED] Request #{req_id} rejected! ({exc})")


async def main() -> None:
    print("=== Demo 1: Denying Mode (blocking=False) ===")
    print("Settings: Leak Rate = 2 req/sec (0.5s interval), Capacity = 3, blocking=False")
    start_time = time.monotonic()

    async with AsyncLeakyBucket(leak_rate=2.0, capacity=3, blocking=False) as bucket:
        # Request 1 runs immediately; requests 2-5 arrive while busy and are rejected
        await asyncio.gather(*(run_client(bucket, i + 1, start_time, blocking=False) for i in range(5)))

    print("\n=== Demo 2: Queueing / Overflow Mode (blocking=True) ===")
    print("Settings: Leak Rate = 2 req/sec, Capacity = 3 items in queue, blocking=True")
    start_time = time.monotonic()

    async with AsyncLeakyBucket(leak_rate=2.0, capacity=3, blocking=True) as bucket:
        # Requests 1-3 enter queue and leak at 0.5s intervals; requests 4-6 overflow and are denied
        await asyncio.gather(*(run_client(bucket, i + 1, start_time, blocking=True) for i in range(6)))


if __name__ == "__main__":
    asyncio.run(main())
