"""Active-Passive Distributed Cron with Lease Election & Fencing Tokens.

Concepts demonstrated:
1. Distributed Lease Consensus (etcd / Consul / Redis Redlock pattern):
   - Active leader periodically renews an expiring lease with TTL.
   - Passive standby followers continuously watch the lease.
2. Fencing Tokens (Mitigating Split-Brain / Zombie Leaders):
   - Every leadership acquisition increments a monotonically increasing `fencing_token`.
   - Downstream message brokers / workers reject any dispatch with an outdated token.
   - Solves the classic distributed systems bug where a paused leader (e.g. GC pause)
     wakes up and tries to dispatch duplicate ticks.
3. Decoupled Worker Dispatch:
   - Schedulers do NOT execute the work; they dispatch an execution event containing
     an idempotency key (`{job_id}:{tick_timestamp}`) to an execution queue.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class LeaseRecord:
    leader_id: str
    fencing_token: int
    expires_at: float


class ConsensusCoordinationStore:
    """Simulates a strongly consistent key-value store (e.g. etcd Raft consensus)."""

    def __init__(self) -> None:
        self._current_lease: Optional[LeaseRecord] = None
        self._fencing_counter: int = 0
        self._lock = asyncio.Lock()

    async def acquire_or_renew(self, node_id: str, ttl: float) -> Tuple[bool, int]:
        """Tries to acquire or renew leadership. Returns (is_leader, fencing_token)."""
        async with self._lock:
            now = time.time()
            if self._current_lease is None or now > self._current_lease.expires_at:
                # Previous lease expired or none exists: new epoch
                self._fencing_counter += 1
                self._current_lease = LeaseRecord(
                    leader_id=node_id,
                    fencing_token=self._fencing_counter,
                    expires_at=now + ttl,
                )
                return True, self._fencing_counter

            if self._current_lease.leader_id == node_id:
                # Renew existing lease (retain current fencing token)
                self._current_lease.expires_at = now + ttl
                return True, self._current_lease.fencing_token

            # Held by another healthy node
            return False, self._current_lease.fencing_token


@dataclass
class CronJobSpec:
    job_id: str
    interval_seconds: float
    command: str
    next_tick: float = 0.0


class MessageQueueWorkerPool:
    """Downstream execution broker enforcing fencing token validity and deduplication."""

    def __init__(self) -> None:
        self.highest_fencing_token: int = 0
        self.executed_idempotency_keys: set[str] = set()
        self.execution_log: List[dict] = []

    def receive_dispatch(self, fencing_token: int, idempotency_key: str, command: str) -> bool:
        # 1. Fencing Token Check: Reject messages from zombie / superseded leaders
        if fencing_token < self.highest_fencing_token:
            print(
                f"  [DISPATCH REJECTED] Stale fencing token {fencing_token} "
                f"< current highest {self.highest_fencing_token} (Zombie leader rejected!)"
            )
            return False

        self.highest_fencing_token = max(self.highest_fencing_token, fencing_token)

        # 2. Idempotency Check: Prevent duplicate runs of the exact same cron tick
        if idempotency_key in self.executed_idempotency_keys:
            print(f"  [DISPATCH DEDUPED] Key '{idempotency_key}' already processed.")
            return False

        self.executed_idempotency_keys.add(idempotency_key)
        self.execution_log.append(
            {"key": idempotency_key, "token": fencing_token, "cmd": command, "time": time.time()}
        )
        print(f"  [WORKER EXECUTED] Token: {fencing_token} | Key: {idempotency_key} -> '{command}'")
        return True


class ActivePassiveCronScheduler:
    def __init__(
        self,
        node_id: str,
        coordinator: ConsensusCoordinationStore,
        queue: MessageQueueWorkerPool,
        jobs: List[CronJobSpec],
    ) -> None:
        self.node_id = node_id
        self.coordinator = coordinator
        self.queue = queue
        self.jobs = jobs
        self.is_running = True
        self.is_leader = False
        self.current_fencing_token = 0

    async def start(self) -> None:
        now = time.time()
        for job in self.jobs:
            job.next_tick = now + job.interval_seconds

        while self.is_running:
            # 1. Heartbeat renewal
            leader, token = await self.coordinator.acquire_or_renew(self.node_id, ttl=0.5)
            was_leader = self.is_leader
            self.is_leader = leader
            self.current_fencing_token = token

            if leader and not was_leader:
                print(f"[{self.node_id}] ACQUIRED LEADERSHIP (Fencing Token: {token})")
            elif not leader and was_leader:
                print(f"[{self.node_id}] LOST LEADERSHIP")

            if self.is_leader:
                # 2. Check due jobs
                current_time = time.time()
                for job in self.jobs:
                    if job.next_tick <= current_time:
                        tick_id = int(job.next_tick)
                        idempotency_key = f"{job.job_id}:{tick_id}"
                        # Dispatch to worker queue
                        self.queue.receive_dispatch(
                            fencing_token=self.current_fencing_token,
                            idempotency_key=idempotency_key,
                            command=job.command,
                        )
                        # Schedule next occurrence
                        job.next_tick = current_time + job.interval_seconds

            await asyncio.sleep(0.1)


async def main() -> None:
    print("=== Active-Passive Distributed Cron with Fencing Tokens Demo ===")
    coordinator = ConsensusCoordinationStore()
    worker_queue = MessageQueueWorkerPool()

    jobs_template = [
        CronJobSpec(job_id="db-backup", interval_seconds=0.25, command="pg_dump db_prod"),
        CronJobSpec(job_id="report-gen", interval_seconds=0.4, command="generate_daily_metrics"),
    ]

    node_east = ActivePassiveCronScheduler("node-us-east", coordinator, worker_queue, [
        CronJobSpec(j.job_id, j.interval_seconds, j.command) for j in jobs_template
    ])
    node_west = ActivePassiveCronScheduler("node-us-west", coordinator, worker_queue, [
        CronJobSpec(j.job_id, j.interval_seconds, j.command) for j in jobs_template
    ])

    task_east = asyncio.create_task(node_east.start())
    task_west = asyncio.create_task(node_west.start())

    # Run for 0.7s with Node-East as leader
    await asyncio.sleep(0.7)

    print("\n--- Simulating Network Split / Pause on Node-East ---")
    # Simulate node-east getting paused/frozen (stops renewing lease)
    node_east.is_running = False

    # Wait for lease to expire (0.5s TTL) and node-west to take over with a higher fencing token
    await asyncio.sleep(0.6)

    print(f"\n[Status] Current highest fencing token in broker: {worker_queue.highest_fencing_token}")

    print("\n--- Simulating Zombie Leader attempting dispatch with stale token ---")
    # Node East "wakes up" and attempts to dispatch with its old fencing token
    stale_token = node_east.current_fencing_token
    worker_queue.receive_dispatch(
        fencing_token=stale_token,
        idempotency_key="db-backup:stale_time",
        command="pg_dump db_prod (stale)",
    )

    node_west.is_running = False
    await asyncio.gather(task_east, task_west)

    print(f"\nTotal verified tasks executed by broker: {len(worker_queue.execution_log)}")
    print("All tasks executed with zero duplicate dispatches!")


if __name__ == "__main__":
    asyncio.run(main())
