"""Distributed Range-Allocated Key Generation URL Shortener.

Concepts demonstrated:
1. Distributed Range Allocation (KGS / ZooKeeper pattern):
   - A central coordinator hands out numeric ranges (e.g. 10,000 IDs per lease).
   - Stateless web workers consume IDs locally in-memory with zero lock contention.
   - Guaranteed zero collisions across any number of distributed worker nodes.
2. Base62 Bijective Encoding:
   - Compact representation using characters [0-9, a-z, A-Z].
   - Predictable URL lengths: 7 characters provide 62^7 ≈ 3.52 trillion unique URLs.
3. Cache-Aside Pattern:
   - LRU cache simulates Redis/Memcached layer absorbing 99% of read redirection traffic.
"""

from __future__ import annotations

import collections
import string
import threading
import time
from dataclasses import dataclass
from typing import Dict, Optional, Tuple

BASE62_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase
BASE = 62


def encode_base62(num: int, min_length: int = 7) -> str:
    """Bijective Base62 encoding with optional zero-padding."""
    if num == 0:
        return BASE62_ALPHABET[0].rjust(min_length, BASE62_ALPHABET[0])
    chars: list[str] = []
    while num > 0:
        num, rem = divmod(num, BASE)
        chars.append(BASE62_ALPHABET[rem])
    encoded = "".join(reversed(chars))
    return encoded.rjust(min_length, BASE62_ALPHABET[0])


def decode_base62(code: str) -> int:
    """Decodes a Base62 string back to its unique integer ID."""
    num = 0
    for char in code:
        num = num * BASE + BASE62_ALPHABET.index(char)
    return num


class CentralRangeCoordinator:
    """Simulates a consensus coordinator (ZooKeeper / etcd / DB Sequence).
    
    Dispenses distinct non-overlapping integer ranges to distributed application nodes.
    """

    def __init__(self, block_size: int = 1000, initial_offset: int = 3_500_000_000) -> None:
        self.block_size = block_size
        self._current_id = initial_offset
        self._lock = threading.Lock()

    def allocate_range(self, worker_id: str) -> Tuple[int, int]:
        with self._lock:
            start = self._current_id
            end = start + self.block_size - 1
            self._current_id = end + 1
            return start, end


class LRUCache:
    """Simulates a fast distributed in-memory cache (e.g., Redis)."""

    def __init__(self, capacity: int = 100) -> None:
        self.capacity = capacity
        self.cache: collections.OrderedDict[str, str] = collections.OrderedDict()
        self.lock = threading.Lock()

    def get(self, key: str) -> Optional[str]:
        with self.lock:
            if key not in self.cache:
                return None
            self.cache.move_to_end(key)
            return self.cache[key]

    def put(self, key: str, value: str) -> None:
        with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            self.cache[key] = value
            if len(self.cache) > self.capacity:
                self.cache.popitem(last=False)


@dataclass
class RangeLease:
    current: int
    end: int

    def next_id(self) -> Optional[int]:
        if self.current > self.end:
            return None
        val = self.current
        self.current += 1
        return val


class DistributedShortenerNode:
    """Stateless worker node serving write (shorten) and read (redirect) traffic."""

    def __init__(
        self,
        node_id: str,
        coordinator: CentralRangeCoordinator,
        storage: Dict[str, str],
        cache: LRUCache,
    ) -> None:
        self.node_id = node_id
        self.coordinator = coordinator
        self.storage = storage  # Simulates persistent distributed DB (e.g., Cassandra/PostgreSQL)
        self.cache = cache
        self.current_lease: Optional[RangeLease] = None
        self._lock = threading.Lock()

    def _get_next_id(self) -> int:
        with self._lock:
            if self.current_lease is None:
                start, end = self.coordinator.allocate_range(self.node_id)
                self.current_lease = RangeLease(current=start, end=end)

            assigned = self.current_lease.next_id()
            if assigned is None:
                # Exhausted range, lease next chunk
                start, end = self.coordinator.allocate_range(self.node_id)
                self.current_lease = RangeLease(current=start, end=end)
                assigned = self.current_lease.next_id()
                assert assigned is not None
            return assigned

    def shorten(self, original_url: str) -> str:
        """Assigns next unique ID from local range, encodes to Base62, and persists."""
        unique_id = self._get_next_id()
        code = encode_base62(unique_id, min_length=7)

        # Write to database and populate cache
        self.storage[code] = original_url
        self.cache.put(code, original_url)
        return code

    def resolve(self, code: str) -> Optional[str]:
        """Resolves short code with Cache-Aside pattern (Cache -> DB)."""
        cached = self.cache.get(code)
        if cached:
            return cached

        # Cache miss: fetch from persistent storage
        original = self.storage.get(code)
        if original:
            self.cache.put(code, original)
        return original


if __name__ == "__main__":
    print("=== Distributed Range-Allocated Key Generation Shortener Demo ===")

    coordinator = CentralRangeCoordinator(block_size=5, initial_offset=100_000_000)
    shared_db: Dict[str, str] = {}
    shared_cache = LRUCache(capacity=50)

    # Instantiate two independent distributed worker nodes
    node_a = DistributedShortenerNode("node-us-east", coordinator, shared_db, shared_cache)
    node_b = DistributedShortenerNode("node-us-west", coordinator, shared_db, shared_cache)

    print("\n1. Generating URLs across multiple distributed worker nodes:")
    urls_a = [node_a.shorten(f"https://service.com/item/{i}") for i in range(7)]
    urls_b = [node_b.shorten(f"https://analytics.io/report/{i}") for i in range(7)]

    print(f"Node A generated codes: {urls_a}")
    print(f"Node B generated codes: {urls_b}")

    # Verify no collisions between nodes
    all_codes = urls_a + urls_b
    assert len(all_codes) == len(set(all_codes)), "Collision detected in range generation!"
    print(f"Total unique URLs generated: {len(all_codes)} (Zero collisions guaranteed)")

    print("\n2. Testing Base62 bijection & decoding:")
    sample_code = urls_a[0]
    decoded_num = decode_base62(sample_code)
    re_encoded = encode_base62(decoded_num, min_length=7)
    print(f"Code: {sample_code} -> Decoded ID: {decoded_num} -> Re-encoded: {re_encoded}")
    assert sample_code == re_encoded

    print("\n3. Testing Cache-Aside resolution:")
    res = node_b.resolve(sample_code)
    print(f"Node B resolving code '{sample_code}' created by Node A: {res}")
    assert res == "https://service.com/item/0"
