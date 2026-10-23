"""Interview-Ready Async Job Queue with Duplicate Job ID Rejection (~60 lines).

Key Interview Concepts:
1. In-Flight Deduplication: Uses a set (`in_flight`) protected by a lock to ensure
   only one active instance of a given job ID exists at any time.
2. Retry-Aware Lifecycle: An ID remains locked while queued, executing, OR retrying.
   It is only released once the job reaches a terminal state (Completed or DLQ).
3. Explicit Rejection: Raises `DuplicateJobError` (or returns False) to inform producers.
4. Sentinel Shutdown: Clean termination using `None`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine


class DuplicateJobError(Exception):
    """Raised when attempting to enqueue a job with an ID that is already in-flight."""
    pass


@dataclass
class Job:
    id: str
    fn: Callable[..., Coroutine[Any, Any, Any]]
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    retries_left: int = 3
    delay: float = 0.2


class DedupJobQueue:
    def __init__(self, num_workers: int = 2) -> None:
        self.queue: asyncio.Queue[Job | None] = asyncio.Queue()
        self.in_flight: set[str] = set()
        self.dlq: list[tuple[Job, Exception]] = []
        self._lock = asyncio.Lock()
        self.workers = [
            asyncio.create_task(self._worker(i + 1)) for i in range(num_workers)
        ]

    async def enqueue(
        self,
        job_id: str,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        retries: int = 3,
        delay: float = 0.2,
        **kwargs: Any,
    ) -> None:
        """Enqueue a job. Raises DuplicateJobError if the job_id is currently in-flight."""
        async with self._lock:
            if job_id in self.in_flight:
                raise DuplicateJobError(
                    f"Job '{job_id}' is already queued, running, or retrying."
                )
            self.in_flight.add(job_id)

        job = Job(id=job_id, fn=fn, args=args, kwargs=kwargs, retries_left=retries, delay=delay)
        await self.queue.put(job)

    async def _worker(self, worker_id: int) -> None:
        while True:
            job = await self.queue.get()
            if job is None:  # Sentinel value signaling worker shutdown
                self.queue.task_done()
                break

            try:
                await job.fn(*job.args, **job.kwargs)
                print(f"[Worker-{worker_id}] Completed job '{job.id}'")
                async with self._lock:
                    self.in_flight.remove(job.id)  # Terminal state: release ID
            except Exception as exc:
                if job.retries_left > 0:
                    job.retries_left -= 1
                    print(
                        f"[Worker-{worker_id}] Failed job '{job.id}': {exc}. "
                        f"Retrying in {job.delay:.2f}s ({job.retries_left} retries left)..."
                    )
                    # Notice: job.id remains in self.in_flight during retry backoff!
                    await asyncio.sleep(job.delay)
                    job.delay *= 2  # Exponential backoff
                    await self.queue.put(job)
                else:
                    print(f"[Worker-{worker_id}] Exhausted retries for job '{job.id}' -> DLQ!")
                    self.dlq.append((job, exc))
                    async with self._lock:
                        self.in_flight.remove(job.id)  # Terminal state: release ID
            finally:
                self.queue.task_done()

    async def close(self) -> None:
        await self.queue.join()
        for _ in self.workers:
            await self.queue.put(None)
        await asyncio.gather(*self.workers)


# --- Demonstration ---
async def flaky_task(duration: float, fail: bool = False) -> None:
    await asyncio.sleep(duration)
    if fail:
        raise RuntimeError("Simulated failure")


async def main() -> None:
    print("=== Deduplicated Job Queue Demo ===")
    jq = DedupJobQueue(num_workers=2)

    # 1. Enqueue job "email-user-42"
    print("Enqueuing job 'email-user-42'...")
    await jq.enqueue("email-user-42", flaky_task, 0.2)

    # 2. Try to enqueue the exact same ID immediately -> Should raise DuplicateJobError
    print("Attempting to enqueue duplicate 'email-user-42'...")
    try:
        await jq.enqueue("email-user-42", flaky_task, 0.2)
    except DuplicateJobError as e:
        print(f"  [REJECTED] {e}")

    # 3. Enqueue a failing task that will retry
    print("\nEnqueuing flaky job 'report-gen'...")
    await jq.enqueue("report-gen", flaky_task, 0.05, fail=True, retries=1, delay=0.2)

    # Allow worker to start processing and enter retry sleep
    await asyncio.sleep(0.1)

    # 4. Try to enqueue 'report-gen' while it is currently retrying -> Still rejected!
    print("Attempting to enqueue 'report-gen' while it is sleeping/retrying...")
    try:
        await jq.enqueue("report-gen", flaky_task, 0.05)
    except DuplicateJobError as e:
        print(f"  [REJECTED] {e}")

    await jq.close()
    print("\nQueue successfully drained and closed.")


if __name__ == "__main__":
    asyncio.run(main())
