import json
import logging

import httpx
import psycopg
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from app.common import BROKERS, POSTGRES_DSN, REGISTRY_URL, ROOT, SUBJECT, TOPIC, ClickHouse, retry

logger = logging.getLogger(__name__)


def initialize():
    admin = AdminClient({"bootstrap.servers": BROKERS})
    topic = NewTopic(TOPIC, num_partitions=3, replication_factor=1, config={"min.insync.replicas": "1"})
    try:
        admin.create_topics([topic])[TOPIC].result(timeout=30)
    except KafkaException as exc:
        if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
            raise
    metadata = admin.list_topics(TOPIC, timeout=15)
    if len(metadata.topics[TOPIC].partitions) < 3:
        raise RuntimeError("movie-events requires at least 3 partitions")

    schema = json.loads((ROOT / "schemas/movie_event_v1.avsc").read_text())
    with httpx.Client(base_url=REGISTRY_URL, timeout=30) as client:
        client.put(f"/config/{SUBJECT}", json={"compatibility": "BACKWARD"}).raise_for_status()
        payload = {"schemaType": "AVRO", "schema": json.dumps(schema)}
        client.post(f"/subjects/{SUBJECT}/versions", json=payload).raise_for_status()
        response = client.post(f"/subjects/{SUBJECT}", json=payload)
        response.raise_for_status()
        registered = response.json()
        logger.info("schema_registered subject=%s version=%s id=%s", SUBJECT, registered["version"], registered["id"])

    clickhouse = ClickHouse()
    for statement in (ROOT / "sql/clickhouse.sql").read_text().split(";"):
        if statement.strip():
            clickhouse.execute(statement)
    with psycopg.connect(POSTGRES_DSN, connect_timeout=5) as connection:
        connection.execute((ROOT / "sql/postgres.sql").read_text())
    logger.info("bootstrap_completed topic=%s partitions=3", TOPIC)


if __name__ == "__main__":
    retry(initialize, (httpx.HTTPError, psycopg.Error, KafkaException, RuntimeError, TimeoutError))
