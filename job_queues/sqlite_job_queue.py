"""Persistent SQLite-backed Job Queue with Retries and DLQ.

Key Features:
- Zero external dependencies (uses standard library sqlite3).
- Crash resilient: jobs survive application restarts.
- ACID atomic claims: uses SQLite transactions to prevent duplicate worker execution.
- Delayed retries: scheduled execution with exponential backoff via `run_at` timestamps.
- Dead-Letter Queue (DLQ): jobs that exceed max retries are marked 'dead_letter'.
"""

from __future__ import annotations

import json
import sqlite3
import time
from typing import Any, Callable


class SQLiteJobQueue:
    """Persistent, thread-safe SQLite-backed job queue with retries."""

    def __init__(self, db_path: str = "jobs.db") -> None:
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        # Enable Write-Ahead Logging for high concurrent read/write throughput
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn

    def _init_db(self) -> None:
        with self._get_connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_name TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    max_retries INTEGER NOT NULL DEFAULT 3,
                    base_delay REAL NOT NULL DEFAULT 1.0,
                    run_at REAL NOT NULL,
                    last_error TEXT,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_ready ON jobs(status, run_at);"
            )

    def enqueue(
        self,
        task_name: str,
        payload: dict[str, Any],
        max_retries: int = 3,
        base_delay: float = 1.0,
        delay_seconds: float = 0.0,
    ) -> int:
        """Enqueue a job for execution."""
        now = time.time()
        run_at = now + delay_seconds
        payload_str = json.dumps(payload)

        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                INSERT INTO jobs (task_name, payload, status, max_retries, base_delay, run_at, created_at, updated_at)
                VALUES (?, ?, 'pending', ?, ?, ?, ?, ?)
                """,
                (task_name, payload_str, max_retries, base_delay, run_at, now, now),
            )
            return cursor.lastrowid  # type: ignore

    def claim_next_job(self) -> dict[str, Any] | None:
        """Atomically claim the next pending job whose `run_at` timestamp has passed."""
        now = time.time()
        conn = self._get_connection()
        try:
            # Use IMMEDIATE transaction to lock DB during claim
            conn.execute("BEGIN IMMEDIATE;")
            cursor = conn.execute(
                """
                SELECT id, task_name, payload, attempts, max_retries, base_delay
                FROM jobs
                WHERE status = 'pending' AND run_at <= ?
                ORDER BY run_at ASC
                LIMIT 1
                """,
                (now,),
            )
            row = cursor.fetchone()
            if not row:
                conn.commit()
                return None

            job_id = row["id"]
            conn.execute(
                """
                UPDATE jobs
                SET status = 'running', attempts = attempts + 1, updated_at = ?
                WHERE id = ?
                """,
                (now, job_id),
            )
            conn.commit()

            return {
                "id": job_id,
                "task_name": row["task_name"],
                "payload": json.loads(row["payload"]),
                "attempts": row["attempts"] + 1,
                "max_retries": row["max_retries"],
                "base_delay": row["base_delay"],
            }
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def mark_completed(self, job_id: int) -> None:
        """Mark a job as successfully finished."""
        now = time.time()
        with self._get_connection() as conn:
            conn.execute(
                "UPDATE jobs SET status = 'completed', updated_at = ? WHERE id = ?",
                (now, job_id),
            )

    def mark_failed_or_retry(self, job_id: int, error_msg: str) -> bool:
        """Handle failure: schedule next retry or move to dead-letter queue.

        Returns True if rescheduled for retry; False if dead-lettered.
        """
        now = time.time()
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT attempts, max_retries, base_delay FROM jobs WHERE id = ?",
                (job_id,),
            )
            row = cursor.fetchone()
            if not row:
                return False

            attempts = row["attempts"]
            max_retries = row["max_retries"]
            base_delay = row["base_delay"]

            if attempts <= max_retries:
                # Schedule exponential backoff retry: 2^(attempts-1) * base_delay
                delay = base_delay * (2 ** (attempts - 1))
                next_run_at = now + delay
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'pending', run_at = ?, last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (next_run_at, error_msg, now, job_id),
                )
                return True
            else:
                # Move to Dead-Letter Queue status
                conn.execute(
                    """
                    UPDATE jobs
                    SET status = 'dead_letter', last_error = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (error_msg, now, job_id),
                )
                return False

    def get_stats(self) -> dict[str, int]:
        """Return counts of jobs by status."""
        with self._get_connection() as conn:
            cursor = conn.execute(
                "SELECT status, COUNT(*) as cnt FROM jobs GROUP BY status"
            )
            return {row["status"]: row["cnt"] for row in cursor.fetchall()}


# --- Worker Loop Demonstration ---
def run_worker_cycle(
    queue: SQLiteJobQueue,
    handlers: dict[str, Callable[[dict[str, Any]], None]],
) -> bool:
    """Processes a single available job. Returns True if a job was found, False otherwise."""
    job = queue.claim_next_job()
    if not job:
        return False

    job_id = job["id"]
    task_name = job["task_name"]
    handler = handlers.get(task_name)

    print(f"[Worker] Claimed Job #{job_id} ('{task_name}', attempt {job['attempts']}/{job['max_retries']})")

    if not handler:
        queue.mark_failed_or_retry(job_id, f"No handler registered for '{task_name}'")
        return True

    try:
        handler(job["payload"])
        queue.mark_completed(job_id)
        print(f"[Worker] Job #{job_id} completed successfully!")
    except Exception as exc:
        retried = queue.mark_failed_or_retry(job_id, str(exc))
        if retried:
            print(f"[Worker] Job #{job_id} failed: {exc}. Scheduled for retry.")
        else:
            print(f"[Worker] Job #{job_id} EXHAUSTED retries. Sent to DLQ! Error: {exc}")

    return True


# --- Demonstration ---
simulated_network_failures = 0

def send_webhook(payload: dict[str, Any]) -> None:
    global simulated_network_failures
    simulated_network_failures += 1
    # Fail first 2 attempts, then succeed
    if simulated_network_failures <= 2:
        raise ConnectionError(f"HTTP 503 Service Unavailable (attempt #{simulated_network_failures})")
    print(f"  -> Successfully delivered webhook payload to {payload['url']}")


def broken_job(payload: dict[str, Any]) -> None:
    raise ValueError(f"Fatal error processing payload: {payload}")


def main() -> None:
    import os

    db_path = "demo_jobs.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    queue = SQLiteJobQueue(db_path=db_path)
    handlers = {
        "send_webhook": send_webhook,
        "broken_task": broken_job,
    }

    print("=== Persistent SQLite Job Queue Demo ===")
    print("Enqueuing jobs into SQLite database...")

    # 1. Flaky webhook job (will fail twice and succeed on 3rd attempt)
    id1 = queue.enqueue(
        "send_webhook",
        {"url": "https://api.example.com/webhook", "event": "order.created"},
        max_retries=3,
        base_delay=0.3,
    )

    # 2. Poison pill job (will fail all 2 retries and enter dead_letter)
    id2 = queue.enqueue(
        "broken_task",
        {"data": "corrupted"},
        max_retries=2,
        base_delay=0.2,
    )

    print(f"Enqueued Job #{id1} (send_webhook) and Job #{id2} (broken_task).\n")

    # Run polling loop until all jobs reach completed or dead_letter
    start = time.time()
    while time.time() - start < 4.0:
        processed = run_worker_cycle(queue, handlers)
        if not processed:
            # Check if all jobs are in terminal state
            stats = queue.get_stats()
            if stats.get("pending", 0) == 0 and stats.get("running", 0) == 0:
                break
            time.sleep(0.1)

    print("\n--- Final Database Status ---")
    stats = queue.get_stats()
    for status, count in stats.items():
        print(f"  Status '{status}': {count} job(s)")

    # Clean up demo database file
    if os.path.exists(db_path):
        os.remove(db_path)
    if os.path.exists(f"{db_path}-wal"):
        os.remove(f"{db_path}-wal")
    if os.path.exists(f"{db_path}-shm"):
        os.remove(f"{db_path}-shm")


if __name__ == "__main__":
    main()
