"""Async Sliding Window Counter Rate Limiter Example.

The Sliding Window Counter algorithm estimates traffic within a rolling window
by weighting the current window and previous window counters.

Supports both:
1. Blocking / Waiting mode: pauses execution until window decays enough.
2. Denying mode: immediately raises RateLimitExceeded with calculated retry_after.
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


class AsyncSlidingWindowCounter:
    """Async Sliding Window Counter rate limiter.

    :param max_requests: Maximum allowed requests within the sliding window.
    :param window_seconds: Duration of the sliding window in seconds.
    :param blocking: Default behavior when limit is reached. If True, waits;
                     if False, raises RateLimitExceeded.
    """

    def __init__(
        self,
        max_requests: int,
        window_seconds: float,
        blocking: bool = True,
    ) -> None:
        if max_requests <= 0:
            raise ValueError("max_requests must be positive")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be positive")

        self.max_requests = max_requests
        self.window_seconds = float(window_seconds)
        self.blocking = blocking

        self._curr_window_start = time.monotonic()
        self._curr_count = 0
        self._prev_count = 0
        self._lock = asyncio.Lock()

    def _update_windows(self, now: float) -> None:
        """Roll over window buckets as time progresses."""
        elapsed = now - self._curr_window_start
        if elapsed >= 2 * self.window_seconds:
            self._prev_count = 0
            self._curr_count = 0
            self._curr_window_start = now
        elif elapsed >= self.window_seconds:
            self._prev_count = self._curr_count
            self._curr_count = 0
            self._curr_window_start += self.window_seconds

    def _estimate_count(self, now: float) -> float:
        """Estimate the request count over the last `window_seconds`."""
        self._update_windows(now)
        time_into_curr_window = now - self._curr_window_start
        prev_weight = max(
            0.0, (self.window_seconds - time_into_curr_window) / self.window_seconds
        )
        return self._prev_count * prev_weight + self._curr_count

    def _calculate_retry_after(self, now: float, estimated: float, weight: int) -> float:
        """Compute the duration in seconds until enough capacity opens up."""
        time_into_curr_window = now - self._curr_window_start
        remaining_in_window = self.window_seconds - time_into_curr_window

        if self._curr_count + weight > self.max_requests:
            return max(0.01, remaining_in_window + 0.005)

        needed_decay = (estimated + weight) - self.max_requests
        if self._prev_count > 0:
            decay_time = (needed_decay / self._prev_count) * self.window_seconds
            return max(0.01, min(decay_time, remaining_in_window))
        return max(0.01, remaining_in_window)

    async def try_acquire(self, weight: int = 1) -> bool:
        """Attempt to acquire permission immediately without waiting. Returns True if allowed."""
        async with self._lock:
            now = time.monotonic()
            estimated = self._estimate_count(now)
            if estimated + weight <= self.max_requests:
                self._curr_count += weight
                return True
            return False

    async def acquire(
        self, weight: int = 1, blocking: bool | None = None
    ) -> None:
        """Acquire capacity in the sliding window.

        If capacity is exceeded:
          - If blocking is True: waits until window decays.
          - If blocking is False: raises RateLimitExceeded.
        """
        should_block = self.blocking if blocking is None else blocking

        async with self._lock:
            while True:
                now = time.monotonic()
                estimated = self._estimate_count(now)
                if estimated + weight <= self.max_requests:
                    self._curr_count += weight
                    return

                retry_after = self._calculate_retry_after(now, estimated, weight)

                if not should_block:
                    raise RateLimitExceeded(retry_after=retry_after)

                await asyncio.sleep(retry_after)

    async def __aenter__(self) -> AsyncSlidingWindowCounter:
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
        """Decorator to rate-limit an async function."""

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
    print("Settings: Limit = 3 requests per 2.0s window (blocking=False)")
    denying_limiter = AsyncSlidingWindowCounter(max_requests=3, window_seconds=2.0, blocking=False)
    start_time = time.monotonic()

    async def run_denying_request(req_id: int) -> None:
        try:
            async with denying_limiter:
                await simulate_api_call(req_id, start_time)
        except RateLimitExceeded as exc:
            elapsed = time.monotonic() - start_time
            print(f"[{elapsed:5.2f}s] [DENIED] Request #{req_id} rejected! ({exc})")

    # 3 succeed immediately; remaining 3 rejected
    await asyncio.gather(*(run_denying_request(i + 1) for i in range(6)))

    print("\n=== Demo 2: Waiting/Blocking Mode (blocking=True) ===")
    print("Settings: Limit = 3 requests per 2.0s window (blocking=True)")
    blocking_limiter = AsyncSlidingWindowCounter(max_requests=3, window_seconds=2.0, blocking=True)
    start_time = time.monotonic()

    @blocking_limiter.limit
    async def run_blocking_request(req_id: int) -> None:
        await simulate_api_call(req_id, start_time)

    # 3 burst immediately; remaining wait for window decay
    await asyncio.gather(*(run_blocking_request(i + 1) for i in range(6)))


if __name__ == "__main__":
    asyncio.run(main())
