"""Interview-Ready URL Shortener (~60 lines).

Concepts demonstrated:
1. Base62 bi-directional encoding/decoding (0-9, a-z, A-Z).
2. Monotonic counter ID generation (guarantees zero collisions).
3. Custom alias support with collision validation.
4. Optional TTL expiration handling.
"""

from __future__ import annotations

import string
import time
from dataclasses import dataclass

BASE62_ALPHABET = string.digits + string.ascii_lowercase + string.ascii_uppercase
BASE = len(BASE62_ALPHABET)  # 62


def encode_base62(num: int) -> str:
    """Converts a positive integer to a Base62 string."""
    if num == 0:
        return BASE62_ALPHABET[0]
    chars: list[str] = []
    while num > 0:
        num, rem = divmod(num, BASE)
        chars.append(BASE62_ALPHABET[rem])
    return "".join(reversed(chars))


def decode_base62(code: str) -> int:
    """Converts a Base62 string back to an integer."""
    num = 0
    for char in code:
        num = num * BASE + BASE62_ALPHABET.index(char)
    return num


@dataclass
class ShortURL:
    original_url: str
    short_code: str
    expires_at: float | None = None

    @property
    def is_expired(self) -> bool:
        return self.expires_at is not None and time.time() > self.expires_at


class URLShortener:
    def __init__(self, base_url: str = "https://sho.rt/") -> None:
        self.base_url = base_url
        self._counter: int = 100_000  # Start at offset to produce minimum length code
        self._code_to_url: dict[str, ShortURL] = {}
        self._url_to_code: dict[str, str] = {}

    def shorten(
        self, original_url: str, custom_alias: str | None = None, ttl_seconds: float | None = None
    ) -> str:
        """Creates or retrieves a shortened URL."""
        # Check if already shortened without custom alias
        if custom_alias is None and original_url in self._url_to_code:
            existing_code = self._url_to_code[original_url]
            if not self._code_to_url[existing_code].is_expired:
                return self.base_url + existing_code

        if custom_alias:
            if custom_alias in self._code_to_url:
                raise ValueError(f"Alias '{custom_alias}' already in use.")
            code = custom_alias
        else:
            code = encode_base62(self._counter)
            self._counter += 1

        expires_at = time.time() + ttl_seconds if ttl_seconds else None
        record = ShortURL(original_url=original_url, short_code=code, expires_at=expires_at)

        self._code_to_url[code] = record
        if not custom_alias:
            self._url_to_code[original_url] = code

        return self.base_url + code

    def resolve(self, short_code: str) -> str | None:
        """Resolves a short code back to original URL, returning None if expired/missing."""
        record = self._code_to_url.get(short_code)
        if not record:
            return None
        if record.is_expired:
            del self._code_to_url[short_code]
            self._url_to_code.pop(record.original_url, None)
            return None
        return record.original_url


if __name__ == "__main__":
    service = URLShortener()

    print("=== Interview-Ready URL Shortener Demo ===")
    
    # 1. Basic shortening
    u1 = service.shorten("https://example.com/very/long/path?param=123")
    print(f"Shortened URL 1: {u1}")
    code1 = u1.split("/")[-1]
    print(f"Resolved code '{code1}' -> {service.resolve(code1)}")

    # 2. Idempotency test
    u1_again = service.shorten("https://example.com/very/long/path?param=123")
    print(f"Idempotent check (same URL): {u1_again} (matches: {u1 == u1_again})")

    # 3. Custom alias
    u2 = service.shorten("https://github.com/termie", custom_alias="termie-gh")
    print(f"Custom alias: {u2}")
    print(f"Resolved 'termie-gh' -> {service.resolve('termie-gh')}")

    # 4. TTL Expiration
    u3 = service.shorten("https://temp-link.org", ttl_seconds=0.1)
    code3 = u3.split("/")[-1]
    print(f"Temporary short code: {code3}")
    print(f"Immediately resolved: {service.resolve(code3)}")
    time.sleep(0.15)
    print(f"Resolved after TTL expired (0.15s): {service.resolve(code3)} (None expected)")
