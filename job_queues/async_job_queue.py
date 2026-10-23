"""Async In-Memory Job Queue with Exponential Backoff Retries & Dead-Letter Queue (DLQ).

Architecture:
- Ready Queue: asyncio.Queue for jobs ready to execute immediately.
- Delayed Heap: Priority queue for retrying jobs waiting for their backoff timer.
- Worker Pool: Multiple concurrent async workers pulling from the Ready Queue.
- Dead-Letter Queue (DLQ): Stores jobs that permanently exhausted their retry attempts.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from enum import Enum
import heapq
import random
import time
from typing import Any, Callable, Coroutine
import uuid


class JobStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    RETRYING = "RETRYING"
    FAILED = "FAILED"  # Dead-lettered


@dataclass(order=True)
class Job:
    scheduled_at: float
    id: str = field(compare=False)
    fn: Callable[..., Coroutine[Any, Any, Any]] = field(compare=False)
    args: tuple[Any, ...] = field(default_factory=tuple, compare=False)
    kwargs: dict[str, Any] = field(default_factory=dict, compare=False)
    max_retries: int = field(default=3, compare=False)
    attempts: int = field(default=0, compare=False)
    base_delay: float = field(default=0.5, compare=False)
    max_delay: float = field(default=30.0, compare=False)
    status: JobStatus = field(default=JobStatus.PENDING, compare=False)
    error: str | None = field(default=None, compare=False)
    result: Any = field(default=None, compare=False)


class AsyncJobQueue:
    """Async Job Queue with worker pool, exponential backoff, and DLQ."""

    def __init__(self, num_workers: int = 2) -> None:
        self.num_workers = num_workers
        self._ready_queue: asyncio.Queue[Job] = asyncio.Queue()
        self._delayed_heap: list[Job] = []
        self._delayed_lock = asyncio.Lock()
        self._dlq: list[Job] = []

        self._workers: list[asyncio.Task[None]] = []
        self._scheduler_task: asyncio.Task[None] | None = None
        self._running = False
        self._pending_count = 0
        self._all_done_event = asyncio.Event()
        self._all_done_event.set()

    def start(self) -> None:
        """Start worker pool and delayed-job scheduler."""
        if not self._running:
            self._running = True
            self._scheduler_task = asyncio.create_task(self._scheduler_loop())
            self._workers = [
                asyncio.create_task(self._worker_loop(i + 1))
                for i in range(self.num_workers)
            ]

    async def enqueue(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        max_retries: int = 3,
        base_delay: float = 0.5,
        max_delay: float = 30.0,
        **kwargs: Any,
    ) -> Job:
        """Add a job to the queue for immediate execution."""
        if not self._running:
            self.start()

        job = Job(
            scheduled_at=time.monotonic(),
            id=str(uuid.uuid4())[:8],
            fn=fn,
            args=args,
            kwargs=kwargs,
            max_retries=max_retries,
            base_delay=base_delay,
            max_delay=max_delay,
        )
        self._pending_count += 1
        self._all_done_event.clear()
        await self._ready_queue.put(job)
        return job

    async def _worker_loop(self, worker_id: int) -> None:
        """Pulls and executes ready jobs."""
        while self._running:
            try:
                job = await self._ready_queue.get()
            except asyncio.CancelledError:
                break

            job.status = JobStatus.RUNNING
            job.attempts += 1

            try:
                # Execute the job function
                job.result = await job.fn(*job.args, **job.kwargs)
                job.status = JobStatus.COMPLETED
                job.error = None
                self._decrement_pending()
            except Exception as exc:
                job.error = f"{type(exc).__name__}: {exc}"

                if job.attempts <= job.max_retries:
                    # Calculate exponential backoff with jitter
                    backoff = min(
                        job.max_delay,
                        job.base_delay * (2 ** (job.attempts - 1)),
                    )
                    # Add +/- 20% jitter
                    jitter = random.uniform(0.8, 1.2)
                    delay = backoff * jitter
                    job.scheduled_at = time.monotonic() + delay
                    job.status = JobStatus.RETRYING

                    print(
                        f"  [Worker-{worker_id}] Job {job.id} failed (attempt {job.attempts}/{job.max_retries}). "
                        f"Retrying in {delay:.2f}s... Reason: {job.error}"
                    )

                    async with self._delayed_lock:
                        heapq.heappush(self._delayed_heap, job)
                else:
                    # Exceeded max retries -> route to Dead-Letter Queue (DLQ)
                    job.status = JobStatus.FAILED
                    self._dlq.append(job)
                    print(
                        f"  [Worker-{worker_id}] Job {job.id} EXHAUSTED all {job.max_retries} retries! "
                        f"Moved to Dead-Letter Queue. Final error: {job.error}"
                    )
                    self._decrement_pending()

            self._ready_queue.task_done()

    async def _scheduler_loop(self) -> None:
        """Polls delayed heap and transfers ready jobs to the ready queue."""
        while self._running:
            now = time.monotonic()
            to_promote: list[Job] = []

            async with self._delayed_lock:
                while self._delayed_heap and self._delayed_heap[0].scheduled_at <= now:
                    to_promote.append(heapq.heappop(self._delayed_heap))

            for job in to_promote:
                job.status = JobStatus.PENDING
                await self._ready_queue.put(job)

            await asyncio.sleep(0.05)

    def _decrement_pending(self) -> None:
        self._pending_count -= 1
        if self._pending_count <= 0:
            self._pending_count = 0
            self._all_done_event.set()

    @property
    def dead_letter_queue(self) -> list[Job]:
        """Returns list of permanently failed jobs."""
        return list(self._dlq)

    async def join(self) -> None:
        """Wait until all pending and retrying jobs have reached a terminal state."""
        await self._all_done_event.wait()
        await self._ready_queue.join()

    async def stop(self) -> None:
        """Stop worker pool and scheduler."""
        self._running = False
        if self._scheduler_task:
            self._scheduler_task.cancel()
        for worker in self._workers:
            worker.cancel()
        await asyncio.gather(
            self._scheduler_task, *self._workers, return_exceptions=True
        )


# --- Demonstration ---
async def flaky_api_call(job_name: str, fail_times: int, tracker: dict[str, int]) -> str:
    """Simulates an API call that fails a given number of times before succeeding."""
    tracker[job_name] = tracker.get(job_name, 0) + 1
    current_attempt = tracker[job_name]

    if current_attempt <= fail_times:
        raise ConnectionResetError(
            f"Transient network drop on attempt {current_attempt}"
        )

    return f"Successfully processed {job_name} on attempt {current_attempt}"


async def poison_pill_job() -> None:
    """Simulates a permanently broken job."""
    raise ValueError("Invalid payload: schema mismatch")


async def main() -> None:
    print("=== Async Job Queue with Retries & DLQ Demo ===")
    print("Starting queue with 2 background workers...\n")

    queue = AsyncJobQueue(num_workers=2)
    queue.start()

    call_tracker: dict[str, int] = {}

    # Job 1: Succeeds immediately on first try
    j1 = await queue.enqueue(
        flaky_api_call, "Job-Clean", fail_times=0, tracker=call_tracker
    )

    # Job 2: Fails twice, then succeeds on attempt 3 (within max_retries=3)
    j2 = await queue.enqueue(
        flaky_api_call,
        "Job-Flaky",
        fail_times=2,
        tracker=call_tracker,
        max_retries=3,
        base_delay=0.3,
    )

    # Job 3: Always fails -> exhausts max_retries=2 -> moved to DLQ
    j3 = await queue.enqueue(
        poison_pill_job,
        max_retries=2,
        base_delay=0.2,
    )

    print(f"Enqueued: {j1.id} (clean), {j2.id} (flaky), {j3.id} (poison pill)")
    print("Awaiting completion of all jobs...\n")

    await queue.join()

    print("\n--- Summary ---")
    print(f"Job {j1.id} ({j1.status.value}): result = '{j1.result}'")
    print(f"Job {j2.id} ({j2.status.value}): attempts = {j2.attempts}, result = '{j2.result}'")
    print(f"Job {j3.id} ({j3.status.value}): attempts = {j3.attempts}, error = '{j3.error}'")

    print(f"\nDead-Letter Queue entries: {len(queue.dead_letter_queue)}")
    for dead_job in queue.dead_letter_queue:
        print(f"  DLQ Job [{dead_job.id}] permanently failed: {dead_job.error}")

    await queue.stop()


if __name__ == "__main__":
    asyncio.run(main())
