import random
from datetime import date

import pytest

from app.common import retry
from app.events import EventEncoder, EventType, local_schema
from app.generator import make_session


def test_generator_session_order_and_avro_validation():
    rng = random.Random(123)
    encoder = EventEncoder(local_schema(), 1)
    observed = set()
    for _ in range(100):
        events = make_session(rng, date(2020, 1, 10))
        kinds = [item.event_type for item in events]
        observed.update(kinds)
        assert kinds[:2] == [EventType.SEARCHED, EventType.VIEW_STARTED]
        assert len({item.user_id for item in events}) == 1
        assert len({item.session_id for item in events}) == 1
        assert len({item.device_type for item in events}) == 1
        assert len({item.movie_id for item in events if item.event_type != EventType.SEARCHED}) == 1
        assert len({item.event_id for item in events}) == len(events)
        assert [item.timestamp for item in events] == sorted(item.timestamp for item in events)
        assert [item.progress_seconds for item in events] == sorted(item.progress_seconds for item in events)
        if EventType.VIEW_FINISHED in kinds:
            assert kinds.index(EventType.VIEW_STARTED) < kinds.index(EventType.VIEW_FINISHED)
        if EventType.VIEW_RESUMED in kinds:
            assert kinds.index(EventType.VIEW_PAUSED) < kinds.index(EventType.VIEW_RESUMED)
        for item in events:
            assert encoder.encode(item)
    assert observed == set(EventType)


def test_retry_backoff_and_exhaustion(monkeypatch):
    delays = []
    monkeypatch.setattr("app.common.time.sleep", delays.append)
    calls = 0

    def broken():
        nonlocal calls
        calls += 1
        raise ConnectionError("offline")

    with pytest.raises(ConnectionError):
        retry(broken, (ConnectionError,))
    assert calls == 4
    assert delays == [0.5, 1, 2]


def test_retry_recovers(monkeypatch):
    monkeypatch.setattr("app.common.time.sleep", lambda _: None)
    calls = 0

    def temporary_failure():
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("offline")
        return "saved"

    assert retry(temporary_failure, (ConnectionError,)) == "saved"
    assert calls == 3
