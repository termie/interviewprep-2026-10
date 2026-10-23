"""Simple, interview-ready async job queue with retries (~50 lines).

Concepts demonstrated:
1. Producer-Consumer pattern using `asyncio.Queue`
2. Retry counting & exponential backoff
3. Dead-Letter Queue (DLQ) for poisoned jobs
4. Graceful shutdown using Sentinel / Poison-Pill pattern (`None`)
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Callable, Coroutine


@dataclass
class Job:
    fn: Callable[..., Coroutine[Any, Any, Any]]
    args: tuple[Any, ...] = ()
    retries_left: int = 3
    delay: float = 0.2


class SimpleJobQueue:
    def __init__(self, num_workers: int = 2) -> None:
        self.queue: asyncio.Queue[Job | None] = asyncio.Queue()
        self.dlq: list[tuple[Job, Exception]] = []
        self.workers = [
            asyncio.create_task(self._worker(i + 1)) for i in range(num_workers)
        ]

    async def enqueue(
        self,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        retries: int = 3,
        delay: float = 0.2,
    ) -> None:
        await self.queue.put(Job(fn=fn, args=args, retries_left=retries, delay=delay))

    async def _worker(self, worker_id: int) -> None:
        while True:
            job = await self.queue.get()
            if job is None:  # Sentinel value indicating worker shutdown
                self.queue.task_done()
                break

            try:
                await job.fn(*job.args)
                print(f"[Worker-{worker_id}] Finished {job.fn.__name__}{job.args}")
            except Exception as exc:
                if job.retries_left > 0:
                    job.retries_left -= 1
                    print(
                        f"[Worker-{worker_id}] Failed {job.fn.__name__}{job.args}: {exc}. "
                        f"Retrying in {job.delay:.2f}s ({job.retries_left} retries left)..."
                    )
                    await asyncio.sleep(job.delay)
                    job.delay *= 2  # Exponential backoff
                    await self.queue.put(job)
                else:
                    print(
                        f"[Worker-{worker_id}] Exhausted retries for {job.fn.__name__}{job.args}. "
                        f"Moved to DLQ! Error: {exc}"
                    )
                    self.dlq.append((job, exc))
            finally:
                self.queue.task_done()

    async def close(self) -> None:
        """Wait for in-flight tasks and gracefully terminate workers."""
        await self.queue.join()
        for _ in self.workers:
            await self.queue.put(None)  # Send sentinel shutdown signal to each worker
        await asyncio.gather(*self.workers)


# --- Quick Interview Demonstration ---
attempts: dict[str, int] = {}

async def task_a(name: str) -> None:
    # Fails once, then succeeds
    attempts[name] = attempts.get(name, 0) + 1
    if attempts[name] < 2:
        raise ConnectionError("Transient network glitch")


async def task_b(name: str) -> None:
    # Always fails
    raise ValueError("Invalid record")


async def main() -> None:
    jq = SimpleJobQueue(num_workers=2)

    await jq.enqueue(task_a, "job-1", retries=2, delay=0.1)
    await jq.enqueue(task_b, "job-2", retries=1, delay=0.1)

    await jq.close()

    print(f"\nCompleted! DLQ contains {len(jq.dlq)} permanently failed job(s).")


if __name__ == "__main__":
    asyncio.run(main())
