"""Hash-Based URL Shortener with Collision Resolution Strategy.

Concepts demonstrated:
1. Truncated Cryptographic Hash:
   - SHA-256 hash digest converted to Base62 representation.
   - Taking first N characters (e.g. 7 characters).
2. Collision Resolution via Salting / Linear Probing:
   - When different URLs produce identical truncated hash prefixes, detect the collision
     via database check.
   - Re-hash using an incremental salt (`url + ":" + attempt`) until a vacant slot is found.
3. Idempotency vs. Collision Trade-off:
   - The same URL generates the exact same hash (idempotent).
   - Demonstrates the performance cost of database roundtrips during collision spikes.
"""

from __future__ import annotations

import hashlib
import string
from dataclasses import dataclass
from typing import Dict, Optional

BASE62_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase
BASE = len(BASE62_ALPHABET)


def int_to_base62(num: int, length: int = 7) -> str:
    """Converts a positive integer to a fixed-length Base62 string."""
    chars: list[str] = []
    while num > 0:
        num, rem = divmod(num, BASE)
        chars.append(BASE62_ALPHABET[rem])
    code = "".join(reversed(chars))
    return code[-length:].rjust(length, BASE62_ALPHABET[0])


@dataclass
class StoredURL:
    short_code: str
    original_url: str
    collision_probes: int = 0


class HashCollisionURLShortener:
    def __init__(self, code_length: int = 7, max_probes: int = 10) -> None:
        self.code_length = code_length
        self.max_probes = max_probes
        self.db: Dict[str, StoredURL] = {}  # short_code -> StoredURL
        self.url_index: Dict[str, str] = {}  # original_url -> short_code (idempotency)

    def _hash_to_code(self, text: str) -> str:
        """Computes SHA-256 and maps the numeric digest to Base62 characters."""
        digest = hashlib.sha256(text.encode("utf-8")).digest()
        # Take the first 8 bytes (64 bits) as an integer
        int_val = int.from_bytes(digest[:8], byteorder="big")
        return int_to_base62(int_val, length=self.code_length)

    def shorten(self, original_url: str) -> str:
        """Shortens a URL, resolving any truncated hash collisions via incremental salting."""
        # 1. Idempotency check: if this exact URL was already shortened, reuse code
        if original_url in self.url_index:
            return self.url_index[original_url]

        # 2. Collision resolution loop
        for attempt in range(self.max_probes):
            # Salt the input on subsequent attempts: "url:1", "url:2", ...
            candidate_input = original_url if attempt == 0 else f"{original_url}:salt_{attempt}"
            code = self._hash_to_code(candidate_input)

            # Check if code is already occupied
            existing = self.db.get(code)
            if existing is None:
                # Vacant code found!
                record = StoredURL(short_code=code, original_url=original_url, collision_probes=attempt)
                self.db[code] = record
                self.url_index[original_url] = code
                return code

            if existing.original_url == original_url:
                # Match found (safety check)
                return code

            # Collision! Another URL took this short code. Try next salt.
            print(f"  [Collision Warning] Code '{code}' already taken by '{existing.original_url}'. Probing attempt {attempt + 1}...")

        raise RuntimeError(f"Exceeded max collision probes ({self.max_probes}) for URL: {original_url}")

    def resolve(self, code: str) -> Optional[str]:
        record = self.db.get(code)
        return record.original_url if record else None


if __name__ == "__main__":
    print("=== Hash Truncation & Collision Resolution Shortener Demo ===")

    # Use short code length of 4 characters to induce frequent collisions for testing
    shortener = HashCollisionURLShortener(code_length=4, max_probes=5)

    test_urls = [
        "https://example.com/alpha",
        "https://example.com/beta",
        "https://example.com/gamma",
        "https://example.com/delta",
        "https://example.com/epsilon",
        "https://example.com/zeta",
        "https://example.com/eta",
        "https://example.com/theta",
    ]

    print("\n1. Shortening URLs (length=4 induces collisions to demonstrate salting):")
    generated = {}
    for url in test_urls:
        code = shortener.shorten(url)
        record = shortener.db[code]
        generated[code] = url
        print(f"  Mapped: {url:<30} -> {code} (Probes needed: {record.collision_probes})")

    print("\n2. Idempotency verification:")
    same_code = shortener.shorten("https://example.com/alpha")
    print(f"  Re-shortening 'https://example.com/alpha': code={same_code} (Matches: {same_code == shortener.url_index['https://example.com/alpha']})")

    print("\n3. Resolving short codes:")
    for code, original in list(generated.items())[:3]:
        resolved = shortener.resolve(code)
        assert resolved == original
        print(f"  Code {code} -> {resolved}")
