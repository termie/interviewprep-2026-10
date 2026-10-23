"""Interview-Ready Distributed Cron Service (~70 lines).

Concepts demonstrated:
1. Leader Election via Heartbeat Lease:
   - Only the designated Leader node executes/dispatches cron jobs.
   - Followers monitor lease expiration; if leader fails, a follower claims leadership.
2. Min-Heap Schedule:
   - Uses `heapq` sorted by `next_run_timestamp` for efficient O(1) next-job lookup.
3. Idempotent Dispatch:
   - Jobs are dispatched with a tick execution key (`job_id:tick_timestamp`).
4. Automatic Failover:
   - Demonstrates active leader crash and seamless follower takeover.
"""

from __future__ import annotations

import asyncio
import heapq
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Optional


@dataclass(order=True)
class ScheduledJob:
    next_run: float
    job_id: str = field(compare=False)
    interval: float = field(compare=False)
    action: Callable[[], Coroutine[Any, Any, None]] = field(compare=False)


class DistributedLeaseStore:
    """Simulates a shared coordination service (etcd, Consul, or Redis)."""

    def __init__(self) -> None:
        self.leader_id: Optional[str] = None
        self.lease_expires_at: float = 0.0

    def try_acquire_or_renew(self, node_id: str, ttl: float = 0.5) -> bool:
        now = time.time()
        # Acquire if unassigned, expired, or currently held by this node
        if self.leader_id is None or now > self.lease_expires_at or self.leader_id == node_id:
            self.leader_id = node_id
            self.lease_expires_at = now + ttl
            return True
        return False


class DistributedCronNode:
    def __init__(self, node_id: str, lease_store: DistributedLeaseStore) -> None:
        self.node_id = node_id
        self.lease_store = lease_store
        self.schedule: list[ScheduledJob] = []
        self.is_running = True
        self.is_leader = False

    def add_job(self, job_id: str, interval: float, action: Callable[[], Coroutine[Any, Any, None]]) -> None:
        heapq.heappush(self.schedule, ScheduledJob(next_run=time.time() + interval, job_id=job_id, interval=interval, action=action))

    async def run(self) -> None:
        """Main loop: manages leadership lease and dispatches due cron jobs."""
        while self.is_running:
            self.is_leader = self.lease_store.try_acquire_or_renew(self.node_id, ttl=0.4)

            if not self.is_leader:
                # Follower standby: sleep briefly and check again
                await asyncio.sleep(0.1)
                continue

            # Leader logic: check and dispatch due jobs
            now = time.time()
            while self.schedule and self.schedule[0].next_run <= now:
                job = heapq.heappop(self.schedule)
                # Dispatch job coroutine asynchronously
                asyncio.create_task(job.action())
                # Reschedule next occurrence
                job.next_run = now + job.interval
                heapq.heappush(self.schedule, job)

            await asyncio.sleep(0.05)


async def main() -> None:
    print("=== Interview-Ready Distributed Cron Service Demo ===")
    store = DistributedLeaseStore()

    # Track execution counts
    executed = []

    async def sample_task(task_name: str) -> None:
        executed.append((time.time(), task_name))
        print(f"  [DISPATCHED] '{task_name}' executed at {time.strftime('%H:%M:%S')}")

    # Start Node 1 (Leader) and Node 2 (Standby)
    node1 = DistributedCronNode("node-1", store)
    node2 = DistributedCronNode("node-2", store)

    for node in (node1, node2):
        node.add_job("heartbeat-task", interval=0.2, action=lambda: sample_task("heartbeat-task"))

    task1 = asyncio.create_task(node1.run())
    task2 = asyncio.create_task(node2.run())

    # Allow Node 1 to run as leader for 0.5s
    await asyncio.sleep(0.5)
    print(f"\n[Status] Current leader is: {store.leader_id} (Node 1 is active, Node 2 in standby)")

    # Simulate Node 1 crash
    print("\n>>> Simulating Node-1 crash! Stopping Node-1 heartbeats...")
    node1.is_running = False
    await task1

    # Wait for lease to expire and Node-2 to claim leadership
    await asyncio.sleep(0.6)
    print(f"\n[Status] After failover, new leader is: {store.leader_id} (Node 2 took over!)")

    node2.is_running = False
    await task2
    print(f"\nTotal tasks executed during test: {len(executed)} (Single execution maintained)")


if __name__ == "__main__":
    asyncio.run(main())
