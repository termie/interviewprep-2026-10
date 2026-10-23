"""Async GCRA (Generic Cell Rate Algorithm) Rate Limiter Example.

GCRA is an exact rate limiting algorithm standardized in ATM networking and widely
used in high-performance systems (such as the Redis `redis-cell` / CL.THROTTLE module).
Instead of tracking token counts or request queues, GCRA tracks a single variable:
the Theoretical Arrival Time (TAT).

Supports both:
1. Blocking / Waiting mode: pauses execution until TAT decays into the burst tolerance.
2. Denying mode: immediately raises RateLimitExceeded with exact retry_after.
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


class AsyncGCRA:
    """Async Generic Cell Rate Algorithm rate limiter.

    :param rate: Allowed rate in operations per second.
    :param burst_capacity: Maximum allowed burst capacity (number of requests that
                           can be made instantaneously from idle). Defaults to 1.
    :param blocking: Default behavior when limit is exceeded. If True, waits;
                     if False, raises RateLimitExceeded.
    """

    def __init__(
        self,
        rate: float,
        burst_capacity: int = 1,
        blocking: bool = True,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst_capacity < 1:
            raise ValueError("burst_capacity must be at least 1")

        self.rate = float(rate)
        self.burst_capacity = burst_capacity
        self.blocking = blocking

        # Emission Interval (T): time between requests under constant rate
        self.emission_interval = 1.0 / self.rate

        # Burst Tolerance (tau): allowable advance in TAT beyond now
        self.burst_tolerance = (self.burst_capacity - 1) * self.emission_interval

        # Theoretical Arrival Time (TAT)
        self._tat = time.monotonic()
        self._lock = asyncio.Lock()

    async def try_acquire(self, cost: int = 1) -> bool:
        """Attempt to acquire permission without waiting. Returns True if accepted."""
        async with self._lock:
            now = time.monotonic()
            tat = max(now, self._tat)
            increment = cost * self.emission_interval

            if (tat + increment) - now <= self.burst_tolerance + self.emission_interval:
                self._tat = tat + increment
                return True
            return False

    async def acquire(self, cost: int = 1, blocking: bool | None = None) -> None:
        """Acquire permission under GCRA.

        If burst tolerance is exceeded:
          - If blocking is True: waits until TAT decays into tolerance.
          - If blocking is False: raises RateLimitExceeded with exact retry_after.
        """
        should_block = self.blocking if blocking is None else blocking

        async with self._lock:
            while True:
                now = time.monotonic()
                tat = max(now, self._tat)
                increment = cost * self.emission_interval
                new_tat = tat + increment

                # If within burst tolerance, permit and advance TAT
                if new_tat - now <= self.burst_tolerance + self.emission_interval:
                    self._tat = new_tat
                    return

                # Calculate required sleep time for TAT to decay within tolerance
                wait_time = (new_tat - self.burst_tolerance - self.emission_interval) - now

                if not should_block:
                    raise RateLimitExceeded(retry_after=max(0.001, wait_time))

                if wait_time > 0:
                    await asyncio.sleep(wait_time)

    async def __aenter__(self) -> AsyncGCRA:
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
        """Decorator to rate-limit an async function using GCRA."""

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
    print("Settings: Rate = 2 req/sec, Burst Capacity = 3 (blocking=False)")
    denying_limiter = AsyncGCRA(rate=2.0, burst_capacity=3, blocking=False)
    start_time = time.monotonic()

    async def run_denying_request(req_id: int) -> None:
        try:
            async with denying_limiter:
                await simulate_api_call(req_id, start_time)
        except RateLimitExceeded as exc:
            elapsed = time.monotonic() - start_time
            print(f"[{elapsed:5.2f}s] [DENIED] Request #{req_id} rejected! ({exc})")

    # 3 burst immediately; remaining 3 rejected
    await asyncio.gather(*(run_denying_request(i + 1) for i in range(6)))

    print("\n=== Demo 2: Waiting/Blocking Mode (blocking=True) ===")
    print("Settings: Rate = 2 req/sec, Burst Capacity = 3 (blocking=True)")
    blocking_limiter = AsyncGCRA(rate=2.0, burst_capacity=3, blocking=True)
    start_time = time.monotonic()

    @blocking_limiter.limit
    async def run_blocking_request(req_id: int) -> None:
        await simulate_api_call(req_id, start_time)

    # 3 burst immediately; remaining 3 paced every 0.5s
    await asyncio.gather(*(run_blocking_request(i + 1) for i in range(6)))


if __name__ == "__main__":
    asyncio.run(main())
