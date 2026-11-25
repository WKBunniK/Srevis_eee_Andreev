import json
import logging
import os
import threading
from contextlib import asynccontextmanager

import httpx
from confluent_kafka import KafkaException, Producer
from fastapi import FastAPI, HTTPException
from fastavro.validation import ValidationError

from app.common import BROKERS, REGISTRY_URL, SUBJECT, TOPIC, retry
from app.events import EventEncoder, MovieEvent, local_schema
from app.generator import generate

logger = logging.getLogger(__name__)


class EventPublisher:
    def __init__(self):
        response = httpx.post(
            f"{REGISTRY_URL}/subjects/{SUBJECT}",
            json={"schemaType": "AVRO", "schema": json.dumps(local_schema())}, timeout=15,
        )
        response.raise_for_status()
        registered = response.json()
        self.version = registered["version"]
        self.encoder = EventEncoder(json.loads(registered["schema"]), registered["id"])
        self.producer = Producer({
            "bootstrap.servers": BROKERS,
            "client.id": "movie-service",
            "enable.idempotence": True,
            "acks": "all",
            "retries": 2147483647,
            "retry.backoff.ms": 250,
            "retry.backoff.max.ms": 2000,
            "delivery.timeout.ms": 15000,
            "request.timeout.ms": 5000,
        })
        self.lock = threading.Lock()

    def publish(self, event):
        payload = self.encoder.encode(event)

        def send():
            delivered = threading.Event()
            errors = []

            def callback(error, message):
                if error is not None:
                    errors.append(error)
                delivered.set()

            self.producer.produce(TOPIC, key=event.user_id.encode(), value=payload, on_delivery=callback)
            while not delivered.is_set():
                self.producer.poll(0.1)
            if errors:
                raise KafkaException(errors[0])

        with self.lock:
            retry(send, (KafkaException, BufferError), attempts=3)
        logger.info("event_published event_id=%s event_type=%s timestamp=%s",
                    event.event_id, event.event_type, event.timestamp.isoformat())


@asynccontextmanager
async def lifespan(app):
    publisher = retry(EventPublisher, (httpx.HTTPError, KafkaException))
    app.state.publisher = publisher
    stop = threading.Event()
    thread = None
    if os.getenv("GENERATOR_ENABLED", "true").lower() == "true":
        interval = float(os.getenv("GENERATOR_INTERVAL_SECONDS", "2"))
        if interval <= 0:
            raise ValueError("GENERATOR_INTERVAL_SECONDS must be positive")
        thread = threading.Thread(target=generate, args=(publisher, stop, interval), daemon=True)
        thread.start()
    yield
    stop.set()
    if thread:
        thread.join(timeout=55)
    publisher.producer.flush(5)


app = FastAPI(title="Movie Service", version="1.0", lifespan=lifespan)


@app.post("/events", status_code=201)
def publish(event: MovieEvent):
    try:
        app.state.publisher.publish(event)
    except ValidationError as exc:
        raise HTTPException(422, "Событие не соответствует зарегистрированной Avro-схеме") from exc
    except (KafkaException, BufferError) as exc:
        logger.exception("event_publish_failed event_id=%s", event.event_id)
        raise HTTPException(503, "Kafka недоступна; повторите запрос с тем же event_id") from exc
    return {"event_id": str(event.event_id)}


@app.get("/health")
def health():
    try:
        metadata = app.state.publisher.producer.list_topics(TOPIC, timeout=3)
        if metadata.topics[TOPIC].error:
            raise RuntimeError(str(metadata.topics[TOPIC].error))
    except Exception as exc:
        raise HTTPException(503, "Kafka недоступна") from exc
    return {"status": "ok", "schema_version": app.state.publisher.version}
