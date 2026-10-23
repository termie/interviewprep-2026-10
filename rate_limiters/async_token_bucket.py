"""Async Token Bucket Rate Limiter Example.

The Token Bucket algorithm accumulates tokens at a steady rate up to a max
capacity (burst limit). When an operation is executed, tokens are deducted.

Supports both:
1. Blocking / Waiting mode: pauses execution until enough tokens replenish.
2. Denying mode: immediately raises RateLimitExceeded (with retry_after)
   when tokens are insufficient.
"""

from __future__ import annotations

import asyncio
import functools
import time
from typing import Any, Callable, Coroutine


class RateLimitExceeded(Exception):
    """Raised when an operation is denied because the rate limit has been exceeded."""

    def __init__(self, retry_after: float, message: str | None = None) -> None:
        self.retry_after = retry_after
        super().__init__(
            message or f"Rate limit exceeded. Retry after {retry_after:.2f}s."
        )


class AsyncTokenBucket:
    """Async Token Bucket rate limiter.

    :param rate: Number of tokens replenished per second.
    :param capacity: Maximum number of tokens the bucket can hold (burst limit).
                     Defaults to `rate` if omitted.
    :param blocking: Default behavior when tokens are exhausted. If True, waits;
                     if False, raises RateLimitExceeded.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        blocking: bool = True,
    ) -> None:
        if rate <= 0:
            raise ValueError("Rate must be positive")
        self.rate = float(rate)
        self.capacity = float(capacity if capacity is not None else rate)
        if self.capacity <= 0:
            raise ValueError("Capacity must be positive")
        self.blocking = blocking

        self.tokens = self.capacity
        self.last_updated = time.monotonic()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self.last_updated
        self.last_updated = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    async def acquire(
        self, tokens: float = 1.0, blocking: bool | None = None
    ) -> None:
        """Acquire `tokens`.

        If tokens are available, deducts them immediately.
        If unavailable:
          - If blocking is True (or default True): waits until available.
          - If blocking is False: immediately raises RateLimitExceeded.
        """
        if tokens > self.capacity:
            raise ValueError(
                f"Requested tokens ({tokens}) exceeds capacity ({self.capacity})"
            )

        should_block = self.blocking if blocking is None else blocking

        async with self._lock:
            while True:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                needed = tokens - self.tokens
                wait_time = needed / self.rate

                if not should_block:
                    raise RateLimitExceeded(retry_after=wait_time)

                await asyncio.sleep(wait_time)

    async def try_acquire(self, tokens: float = 1.0) -> bool:
        """Attempt to acquire tokens immediately without waiting. Returns True if successful."""
        async with self._lock:
            self._refill()
            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def __aenter__(self) -> AsyncTokenBucket:
        await self.acquire()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        pass

    def limit(
        self,
        func: Callable[..., Coroutine[Any, Any, Any]] | None = None,
        *,
        blocking: bool | None = None,
    ) -> Any:
        """Decorator to rate-limit an async function.

        Can be used as `@limiter.limit` or `@limiter.limit(blocking=False)`.
        """

        def decorator(fn: Callable[..., Coroutine[Any, Any, Any]]) -> Callable[..., Coroutine[Any, Any, Any]]:
            @functools.wraps(fn)
            async def wrapper(*args: Any, **kwargs: Any) -> Any:
                await self.acquire(blocking=blocking)
                return await fn(*args, **kwargs)

            return wrapper

        if func is not None:
            return decorator(func)
        return decorator


# --- Demonstration ---
async def simulate_api_call(request_id: int, start_time: float) -> None:
    elapsed = time.monotonic() - start_time
    print(f"[{elapsed:5.2f}s] Executed API request #{request_id}")


async def main() -> None:
    print("=== Demo 1: Denying Mode (blocking=False) ===")
    print("Settings: 2 tokens/sec, Burst Capacity = 3 tokens (blocking=False)")
    denying_limiter = AsyncTokenBucket(rate=2.0, capacity=3.0, blocking=False)
    start_time = time.monotonic()

    async def run_denying_request(req_id: int) -> None:
        try:
            async with denying_limiter:
                await simulate_api_call(req_id, start_time)
        except RateLimitExceeded as exc:
            elapsed = time.monotonic() - start_time
            print(f"[{elapsed:5.2f}s] [DENIED] Request #{req_id} rejected! ({exc})")

    # 3 succeed immediately, remaining 3 denied with retry_after
    await asyncio.gather(*(run_denying_request(i + 1) for i in range(6)))

    print("\n=== Demo 2: Waiting/Blocking Mode (blocking=True) ===")
    print("Settings: 2 tokens/sec, Burst Capacity = 3 tokens (blocking=True)")
    blocking_limiter = AsyncTokenBucket(rate=2.0, capacity=3.0, blocking=True)
    start_time = time.monotonic()

    @blocking_limiter.limit
    async def run_blocking_request(req_id: int) -> None:
        await simulate_api_call(req_id, start_time)

    # 3 burst immediately; 3 wait smoothly
    await asyncio.gather(*(run_blocking_request(i + 1) for i in range(6)))


if __name__ == "__main__":
    asyncio.run(main())
