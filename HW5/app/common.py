import json
import logging
import os
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
TOPIC = "movie-events"
SUBJECT = f"{TOPIC}-value"
BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://cinema:cinema@postgres:5432/cinema")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def retry(operation, exceptions, attempts=4, delay=0.5):
    for attempt in range(attempts):
        try:
            return operation()
        except exceptions:
            if attempt == attempts - 1:
                logger.exception("operation_failed attempts=%s", attempts)
                raise
            wait = delay * 2**attempt
            logger.warning("operation_retry attempt=%s delay_seconds=%s", attempt + 1, wait, exc_info=True)
            time.sleep(wait)


class ClickHouse:
    def __init__(self):
        self.url = os.getenv("CLICKHOUSE_URL", "http://clickhouse:8123")
        self.auth = (os.getenv("CLICKHOUSE_USER", "cinema"), os.getenv("CLICKHOUSE_PASSWORD", "cinema"))

    def execute(self, sql, parameters=None, data=None):
        params = {f"param_{key}": str(value) for key, value in (parameters or {}).items()}
        if data is None:
            content = sql
        else:
            params["query"] = sql
            content = data
        response = httpx.post(self.url, params=params, content=content, auth=self.auth, timeout=60)
        if response.is_error:
            raise RuntimeError(f"ClickHouse: {response.text}")
        return response.text

    def query(self, sql, parameters=None):
        return json.loads(self.execute(sql + " FORMAT JSON", parameters))["data"]

    def insert_metrics(self, rows):
        data = "\n".join(json.dumps(row, allow_nan=False) for row in rows)
        self.execute("INSERT INTO cinema.daily_metrics FORMAT JSONEachRow", data=data)
