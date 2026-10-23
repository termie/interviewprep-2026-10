"""Decentralized Partitioned Distributed Cron via Consistent Hashing Ring.

Concepts demonstrated:
1. Horizontal Scalability (Partitioning):
   - When managing millions of cron jobs, a single leader becomes a CPU/memory bottleneck.
   - Jobs are sharded across an arbitrary number of worker nodes using Consistent Hashing.
2. Virtual Nodes (Uniform Distribution):
   - Each physical node maps to multiple points (virtual nodes) on the ring to prevent
     hotspots and achieve balanced partition distribution.
3. Minimal Disruption on Membership Changes:
   - When a node joins or crashes, only K/N jobs migrate to neighboring nodes (where K = total jobs,
     N = number of nodes), rather than re-shuffling the entire cluster.
4. Independent Shard Schedulers:
   - Each node independently tracks and dispatches only its assigned slice of cron jobs.
"""

from __future__ import annotations

import bisect
import hashlib
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Set


def hash_key(key: str) -> int:
    """Computes MD5 hash as a 32-bit unsigned integer on the ring [0, 2^32 - 1]."""
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


class ConsistentHashRing:
    def __init__(self, replicas: int = 3) -> None:
        self.replicas = replicas
        self.ring: List[int] = []  # Sorted list of hash points
        self.ring_map: Dict[int, str] = {}  # hash point -> node_id
        self.nodes: Set[str] = set()

    def add_node(self, node_id: str) -> None:
        self.nodes.add(node_id)
        for i in range(self.replicas):
            point = hash_key(f"{node_id}:vnode_{i}")
            bisect.insort(self.ring, point)
            self.ring_map[point] = node_id

    def remove_node(self, node_id: str) -> None:
        if node_id not in self.nodes:
            return
        self.nodes.remove(node_id)
        for i in range(self.replicas):
            point = hash_key(f"{node_id}:vnode_{i}")
            idx = bisect.bisect_left(self.ring, point)
            if idx < len(self.ring) and self.ring[idx] == point:
                del self.ring[idx]
            self.ring_map.pop(point, None)

    def get_node(self, key: str) -> Optional[str]:
        """Finds the node responsible for key on the hash ring."""
        if not self.ring:
            return None
        val = hash_key(key)
        idx = bisect.bisect_right(self.ring, val)
        # Wrap around ring if beyond the highest hash point
        if idx == len(self.ring):
            idx = 0
        return self.ring_map[self.ring[idx]]


@dataclass
class JobDefinition:
    job_id: str
    interval_seconds: float
    command: str


class ShardedCronNode:
    def __init__(self, node_id: str, ring: ConsistentHashRing) -> None:
        self.node_id = node_id
        self.ring = ring
        self.local_jobs: Dict[str, JobDefinition] = {}
        self.executed_tasks: List[str] = []

    def rebalance(self, all_jobs: List[JobDefinition]) -> None:
        """Determines which jobs belong to this node based on the current ring topology."""
        new_subset: Dict[str, JobDefinition] = {}
        for job in all_jobs:
            owner = self.ring.get_node(job.job_id)
            if owner == self.node_id:
                new_subset[job.job_id] = job
        self.local_jobs = new_subset

    def tick(self) -> None:
        """Simulates one scheduler clock tick on this node's assigned shard."""
        for job_id, job in self.local_jobs.items():
            msg = f"[{self.node_id}] Executed job '{job_id}' -> '{job.command}'"
            self.executed_tasks.append(msg)
            print(f"  {msg}")


if __name__ == "__main__":
    print("=== Decentralized Consistent Hashing Sharded Cron Demo ===")

    # Setup Consistent Hash Ring with 3 virtual node replicas per physical node
    ring = ConsistentHashRing(replicas=3)
    nodes = ["node-alpha", "node-bravo", "node-charlie"]
    for n in nodes:
        ring.add_node(n)

    cluster = {n: ShardedCronNode(n, ring) for n in nodes}

    # 12 Sample Cron Jobs across the enterprise
    all_jobs = [
        JobDefinition(f"job-{i:02d}", 60.0, f"sync_service_{i}") for i in range(1, 13)
    ]

    print("\n1. Initial Partitioning across 3 nodes:")
    for node in cluster.values():
        node.rebalance(all_jobs)
        print(f"  {node.node_id}: Assigned {len(node.local_jobs)} jobs -> {sorted(node.local_jobs.keys())}")

    total_assigned = sum(len(n.local_jobs) for n in cluster.values())
    assert total_assigned == len(all_jobs), "Every job must be assigned to exactly one node!"
    print(f"Total jobs accounted for: {total_assigned}/{len(all_jobs)}")

    print("\n2. Executing one tick across all active shard nodes:")
    for node in cluster.values():
        node.tick()

    print("\n3. Simulating Node Crash: 'node-bravo' leaves the cluster:")
    ring.remove_node("node-bravo")
    del cluster["node-bravo"]

    print("Rebalancing remaining nodes on the ring:")
    for node in cluster.values():
        node.rebalance(all_jobs)
        print(f"  {node.node_id}: Newly assigned {len(node.local_jobs)} jobs -> {sorted(node.local_jobs.keys())}")

    rebalanced_total = sum(len(n.local_jobs) for n in cluster.values())
    assert rebalanced_total == len(all_jobs), "All jobs must be retained after failover!"
    print(f"Post-failover total jobs accounted for: {rebalanced_total}/{len(all_jobs)} (Seamless shard migration)")
