import io
import json
import struct
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import httpx
import pytest
from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient
from fastavro import schemaless_reader

from app.bootstrap import initialize
from app.common import BROKERS, REGISTRY_URL, SUBJECT, TOPIC, ClickHouse
from app.events import local_schema
from conftest import event, wait_events, wait_for


def test_bootstrap_is_repeatable_and_schema_is_versioned():
    initialize()
    initialize()
    metadata = AdminClient({"bootstrap.servers": BROKERS}).list_topics(TOPIC, timeout=10)
    assert len(metadata.topics[TOPIC].partitions) == 3
    response = httpx.get(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions", timeout=10)
    response.raise_for_status()
    assert response.json() == [1]
    registered = httpx.get(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/1", timeout=10).json()
    assert json.loads(registered["schema"]) == local_schema()
    configuration = httpx.get(f"{REGISTRY_URL}/config/{SUBJECT}", timeout=10).json()
    assert configuration["compatibilityLevel"] == "BACKWARD"


def test_http_kafka_clickhouse_all_fields_and_user_key(producer):
    timestamp = datetime(2021, 2, 3, 12, 4, 5, 123000, tzinfo=timezone(timedelta(hours=3)))
    source = event(timestamp=timestamp, event_type="VIEW_PAUSED", progress=123)
    response = producer.post("/events", json=source)
    assert response.status_code == 201, response.text
    assert response.json() == {"event_id": source["event_id"]}
    wait_events([source])
    row = ClickHouse().query(
        "SELECT *, toUnixTimestamp64Milli(timestamp) AS timestamp_ms FROM cinema.events FINAL WHERE event_id = {id:UUID}",
        {"id": source["event_id"]},
    )[0]
    for field in ("event_id", "user_id", "movie_id", "event_type", "device_type", "session_id", "progress_seconds"):
        assert row[field] == source[field]
    assert int(row["timestamp_ms"]) == int(timestamp.timestamp() * 1000)

    consumer = Consumer({"bootstrap.servers": BROKERS, "group.id": str(uuid4()),
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    try:
        consumer.subscribe([TOPIC])

        def matching_message():
            message = consumer.poll(0.5)
            if message is not None and message.error() is None and message.key() == source["user_id"].encode():
                return message
            return None

        message = wait_for(matching_message)
        assert message.value()[0] == 0
    finally:
        consumer.close()


@pytest.mark.parametrize("patch", [
    {"event_id": "invalid"}, {"event_type": "INVALID"}, {"progress_seconds": -1},
])
def test_http_validation(producer, patch):
    response = producer.post("/events", json=event() | patch)
    assert response.status_code == 422, response.text


def test_missing_required_field(producer):
    source = event()
    del source["session_id"]
    assert producer.post("/events", json=source).status_code == 422


def test_same_user_events_keep_partition_and_order(producer):
    user = str(uuid4())
    session = str(uuid4())
    start = datetime(2010, 5, 7, tzinfo=UTC)
    events = [event(user, start + timedelta(seconds=index * 60), kind, progress=progress, session_id=session)
              for index, (kind, progress) in enumerate([
                  ("VIEW_STARTED", 0), ("VIEW_PAUSED", 60), ("VIEW_RESUMED", 60), ("VIEW_FINISHED", 120),
              ])]
    for source in events:
        response = producer.post("/events", json=source)
        assert response.status_code == 201, response.text
    wait_events(events)
    schema = httpx.get(f"{REGISTRY_URL}/subjects/{SUBJECT}/versions/1", timeout=10).json()
    consumer = Consumer({"bootstrap.servers": BROKERS, "group.id": str(uuid4()),
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    messages = []
    try:
        consumer.subscribe([TOPIC])

        def received_all():
            for message in consumer.consume(num_messages=100, timeout=0.5):
                assert message.error() is None, message.error()
                if message.key() == user.encode():
                    messages.append(message)
            return len(messages) >= len(events)

        wait_for(received_all)
    finally:
        consumer.close()
    assert len(messages) == len(events)
    assert len({message.partition() for message in messages}) == 1
    assert [message.offset() for message in messages] == sorted(message.offset() for message in messages)
    decoded_ids = []
    for message in messages:
        assert struct.unpack(">bI", message.value()[:5]) == (0, schema["id"])
        decoded = schemaless_reader(io.BytesIO(message.value()[5:]), json.loads(schema["schema"]))
        decoded_ids.append(str(decoded["event_id"]))
    assert decoded_ids == [source["event_id"] for source in events]
