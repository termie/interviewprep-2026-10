"""Interview-Ready Async Priority Job Queue with Retries (~60 lines).

Key Interview Concepts:
1. `asyncio.PriorityQueue`: Min-heap where lower numbers indicate higher priority.
2. FIFO Tie-Breaking: Monotonic sequence counter (`seq`) prevents `TypeError` when
   priorities are equal and ensures stable FIFO order within the same priority tier.
3. Sentinel Shutdown: Uses `priority=float('inf')` so the shutdown sentinel is always
   processed after all real jobs are drained.
4. Exponential Backoff & Dead-Letter Queue (DLQ).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine


@dataclass(order=True)
class PrioritizedJob:
    priority: float  # Lower number = higher priority (e.g. 1 is P1/Urgent, 10 is P10/Background)
    seq: int  # Tie-breaker for stable FIFO within same priority
    fn: Callable[..., Coroutine[Any, Any, Any]] | None = field(compare=False)
    args: tuple[Any, ...] = field(default=(), compare=False)
    retries_left: int = field(default=3, compare=False)
    delay: float = field(default=0.2, compare=False)


class PriorityJobQueue:
    def __init__(self, num_workers: int = 2) -> None:
        self.queue: asyncio.PriorityQueue[PrioritizedJob] = asyncio.PriorityQueue()
        self.dlq: list[tuple[PrioritizedJob, Exception]] = []
        self._seq = 0
        self.workers = [
            asyncio.create_task(self._worker(i + 1)) for i in range(num_workers)
        ]

    async def enqueue(
        self,
        priority: int,
        fn: Callable[..., Coroutine[Any, Any, Any]],
        *args: Any,
        retries: int = 3,
        delay: float = 0.2,
    ) -> None:
        self._seq += 1
        job = PrioritizedJob(
            priority=priority,
            seq=self._seq,
            fn=fn,
            args=args,
            retries_left=retries,
            delay=delay,
        )
        await self.queue.put(job)

    async def _worker(self, worker_id: int) -> None:
        while True:
            job = await self.queue.get()
            if job.fn is None:  # Shutdown sentinel (priority=inf)
                self.queue.task_done()
                break

            try:
                await job.fn(*job.args)
                print(
                    f"[Worker-{worker_id}] (Priority {job.priority}) Completed {job.fn.__name__}{job.args}"
                )
            except Exception as exc:
                if job.retries_left > 0:
                    job.retries_left -= 1
                    print(
                        f"[Worker-{worker_id}] (Priority {job.priority}) Failed {job.fn.__name__}{job.args}: {exc}. "
                        f"Retrying in {job.delay:.2f}s ({job.retries_left} retries left)..."
                    )
                    await asyncio.sleep(job.delay)
                    job.delay *= 2  # Exponential backoff
                    await self.queue.put(job)
                else:
                    print(
                        f"[Worker-{worker_id}] (Priority {job.priority}) Exhausted retries for {job.fn.__name__}{job.args} -> DLQ!"
                    )
                    self.dlq.append((job, exc))
            finally:
                self.queue.task_done()

    async def close(self) -> None:
        await self.queue.join()
        for _ in self.workers:
            # Enqueue sentinel with infinite priority so it is processed last
            self._seq += 1
            await self.queue.put(
                PrioritizedJob(priority=float("inf"), seq=self._seq, fn=None)
            )
        await asyncio.gather(*self.workers)


# --- Demonstration ---
async def process_task(name: str) -> None:
    await asyncio.sleep(0.05)


async def main() -> None:
    print("=== Priority Job Queue Demo ===")
    print("Enqueuing jobs in reverse priority order...\n")

    # Use 1 worker to strictly demonstrate ordering
    jq = PriorityJobQueue(num_workers=1)

    # Enqueue low priority first, then medium, then urgent high priority
    await jq.enqueue(3, process_task, "Low-Priority Job 1")
    await jq.enqueue(3, process_task, "Low-Priority Job 2")
    await jq.enqueue(2, process_task, "Medium-Priority Job")
    await jq.enqueue(1, process_task, "HIGH-PRIORITY URGENT Job")

    await jq.close()


if __name__ == "__main__":
    asyncio.run(main())
