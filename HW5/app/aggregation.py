import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta

import psycopg
from fastapi import FastAPI, HTTPException

from app.common import POSTGRES_DSN, ROOT, ClickHouse, retry

logger = logging.getLogger(__name__)
calculation_lock = threading.Lock()


def calculate(day):
    clickhouse = ClickHouse()
    parameters = {"day": day.isoformat()}
    daily = clickhouse.query((ROOT / "sql/daily.sql").read_text(), parameters)[0]
    top = clickhouse.query((ROOT / "sql/top_movies.sql").read_text(), parameters)
    cohort = clickhouse.query((ROOT / "sql/retention.sql").read_text(), parameters)[0]
    computed_at = datetime.now(UTC)
    rows = []

    def add(name, value, dimension=""):
        rows.append({"date": day.isoformat(), "metric_name": name, "dimension": dimension,
                     "value": float(value) if value is not None else None,
                     "computed_at": computed_at.strftime("%Y-%m-%d %H:%M:%S.%f")})

    for metric in ("dau", "avg_watch_seconds", "view_conversion"):
        add(metric, daily[metric])
    cohort_size = int(cohort["cohort_size"])
    for offset in (1, 7):
        mature = day + timedelta(days=offset) < computed_at.date()
        value = int(cohort[f"returned_d{offset}"]) / cohort_size if cohort_size and mature else None
        add(f"retention_d{offset}", value)
    for movie in top:
        add("movie_views", movie["views"], movie["movie_id"])
        add("movie_rank", movie["rank"], movie["movie_id"])
    return rows, int(daily["processed_events"]), computed_at


def save_postgres(rows, computed_at):
    def write():
        with psycopg.connect(POSTGRES_DSN, connect_timeout=5) as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """INSERT INTO daily_metrics (date, metric_name, dimension, value, computed_at)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (date, metric_name, dimension)
                       DO UPDATE SET value = EXCLUDED.value, computed_at = EXCLUDED.computed_at""",
                    [(row["date"], row["metric_name"], row["dimension"], row["value"], computed_at) for row in rows],
                )
    retry(write, (psycopg.Error,))


def recalculate(day):
    with calculation_lock:
        started = time.monotonic()
        logger.info("aggregation_started date=%s", day)
        try:
            rows, processed, computed_at = calculate(day)
            ClickHouse().insert_metrics(rows)
            save_postgres(rows, computed_at)
        except Exception:
            logger.exception("aggregation_failed date=%s duration_seconds=%.3f", day, time.monotonic() - started)
            raise
        logger.info("aggregation_completed date=%s processed_events=%s metrics=%s duration_seconds=%.3f",
                    day, processed, len(rows), time.monotonic() - started)
        return {"date": day, "processed_events": processed, "metrics_written": len(rows), "computed_at": computed_at}


def run_schedule(stop, interval):
    while not stop.is_set():
        started = time.monotonic()
        processed = 0
        failures = 0
        logger.info("aggregation_cycle_started")
        try:
            days = ClickHouse().query("SELECT DISTINCT toDate(timestamp) AS date FROM cinema.events ORDER BY date")
            for row in days:
                if stop.is_set():
                    return
                try:
                    processed += recalculate(date.fromisoformat(row["date"]))["processed_events"]
                except Exception:
                    failures += 1
            logger.info("aggregation_cycle_completed days=%s processed_events=%s failures=%s duration_seconds=%.3f",
                        len(days), processed, failures, time.monotonic() - started)
        except Exception:
            logger.exception("aggregation_cycle_failed")
        stop.wait(interval)


@asynccontextmanager
async def lifespan(app):
    stop = threading.Event()
    interval = float(os.getenv("AGGREGATION_INTERVAL_SECONDS", "60"))
    if interval <= 0:
        raise ValueError("AGGREGATION_INTERVAL_SECONDS must be positive")
    thread = threading.Thread(target=run_schedule, args=(stop, interval), daemon=True)
    thread.start()
    yield
    stop.set()
    thread.join(timeout=30)


app = FastAPI(title="Cinema Analytics", version="1.0", lifespan=lifespan)


@app.post("/recalculate/{day}")
def manual_recalculate(day: date):
    if not date(1970, 1, 1) <= day <= datetime.now(UTC).date():
        raise HTTPException(422, "Укажите дату от 1970-01-01 до сегодняшней в UTC")
    try:
        return recalculate(day)
    except Exception as exc:
        raise HTTPException(503, "Пересчёт не завершён; повторите запрос позже") from exc


@app.get("/health")
def health():
    try:
        ClickHouse().execute("SELECT 1")
        with psycopg.connect(POSTGRES_DSN, connect_timeout=3) as connection:
            connection.execute("SELECT 1")
    except Exception as exc:
        raise HTTPException(503, "Хранилище недоступно") from exc
    return {"status": "ok"}
