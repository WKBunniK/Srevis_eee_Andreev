import os
import time
from datetime import UTC, datetime
from uuid import uuid4

import httpx
import pytest

from app.common import ClickHouse


@pytest.fixture(scope="session")
def producer():
    with httpx.Client(base_url=os.getenv("PRODUCER_URL", "http://localhost:8005"), timeout=65) as client:
        yield client


@pytest.fixture(scope="session")
def analytics():
    with httpx.Client(base_url=os.getenv("AGGREGATION_URL", "http://localhost:8006"), timeout=120) as client:
        yield client


def event(user_id=None, timestamp=None, event_type="VIEW_STARTED", movie_id="film-a", progress=0, session_id=None):
    return {
        "event_id": str(uuid4()), "user_id": user_id or str(uuid4()), "movie_id": movie_id,
        "event_type": event_type, "timestamp": (timestamp or datetime.now(UTC)).isoformat(),
        "device_type": "TV", "session_id": session_id or str(uuid4()), "progress_seconds": progress,
    }


def wait_for(check, timeout=60):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = check()
        if last:
            return last
        time.sleep(0.5)
    raise AssertionError(f"Condition was not met in {timeout}s; last result: {last!r}")


def wait_events(events):
    identifiers = [item["event_id"] for item in events]
    return wait_for(lambda: int(ClickHouse().query(
        "SELECT uniqExact(event_id) AS n FROM cinema.events WHERE event_id IN {ids:Array(UUID)}",
        {"ids": str(identifiers)},
    )[0]["n"]) == len(identifiers))
