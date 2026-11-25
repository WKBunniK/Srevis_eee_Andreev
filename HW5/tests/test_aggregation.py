from datetime import UTC, date, datetime, timedelta
from uuid import uuid4

import psycopg
import pytest
from psycopg.rows import dict_row

from app.common import POSTGRES_DSN, ClickHouse
from conftest import event, wait_events, wait_for


def postgres_metrics(day):
    with psycopg.connect(POSTGRES_DSN, row_factory=dict_row) as connection:
        return connection.execute(
            "SELECT * FROM daily_metrics WHERE date = %s ORDER BY metric_name, dimension", (day,),
        ).fetchall()


def metric_map(rows):
    return {(row["metric_name"], row["dimension"]): row["value"] for row in rows}


@pytest.fixture(scope="module")
def dataset(producer):
    start = datetime(2020, 1, 10, 12, tzinfo=UTC)
    prefix = str(uuid4())
    users = [f"{prefix}-{i}" for i in range(5)]
    events = []

    def add(index, offset, kind="VIEW_STARTED", movie="film-a", progress=0):
        events.append(event(users[index], start + timedelta(days=offset, seconds=progress), kind, movie, progress,
                            session_id=f"{prefix}-{index}-{offset}"))

    add(4, -1)
    for index in range(5):
        add(index, 0, movie="film-a" if index < 3 else "film-b")
    add(0, 0, "VIEW_FINISHED", progress=100)
    add(1, 0, "VIEW_FINISHED", progress=300)
    add(0, 0, "LIKED", progress=100)
    add(0, 1)
    add(1, 1)
    add(1, 1, "VIEW_FINISHED", progress=200)
    add(0, 2)
    add(0, 7)
    add(0, 7, "VIEW_FINISHED", progress=150)
    for source in events:
        response = producer.post("/events", json=source)
        assert response.status_code == 201, response.text
    duplicate = events[6]
    assert duplicate["event_type"] == "VIEW_FINISHED"
    assert producer.post("/events", json=duplicate).status_code == 201
    wait_events(events)
    return start.date(), users, events


def test_all_business_metrics_materialized_and_postgres_upsert(analytics, dataset):
    day, _, _ = dataset
    expected = {
        ("dau", ""): 5, ("avg_watch_seconds", ""): 200,
        ("view_conversion", ""): 0.4,
        ("retention_d1", ""): 0.5, ("retention_d7", ""): 0.25,
        ("movie_views", "film-a"): 3, ("movie_views", "film-b"): 2,
        ("movie_rank", "film-a"): 1, ("movie_rank", "film-b"): 2,
    }
    for _ in range(2):
        response = analytics.post(f"/recalculate/{day}")
        assert response.status_code == 200, response.text
        assert response.json()["processed_events"] == 8
        assert response.json()["metrics_written"] == len(expected)
        rows = postgres_metrics(day)
        assert len(rows) == len(expected)
        assert metric_map(rows) == pytest.approx(expected)
        assert all(row["date"] == day and row["computed_at"] for row in rows)
        clickhouse_rows = ClickHouse().query("SELECT * FROM cinema.daily_metrics FINAL WHERE date = {day:Date}", {"day": day})
        assert len(clickhouse_rows) == len(expected)
        assert metric_map(clickhouse_rows) == pytest.approx(expected)
    with psycopg.connect(POSTGRES_DSN) as connection:
        assert connection.execute("SELECT count(*) FROM daily_metrics WHERE date = %s", (day,)).fetchone()[0] == len(expected)


def test_late_event_updates_metrics_and_scheduler_runs(producer, analytics, dataset):
    day, users, _ = dataset
    baseline = analytics.post(f"/recalculate/{day}")
    assert baseline.status_code == 200
    earlier = event(users[3], datetime.combine(day - timedelta(days=2), datetime.min.time(), UTC))
    late_return = event(users[2], datetime.combine(day + timedelta(days=7), datetime.min.time(), UTC))
    for source in (earlier, late_return):
        assert producer.post("/events", json=source).status_code == 201
    wait_events([earlier, late_return])

    def updated():
        metrics = metric_map(postgres_metrics(day))
        return (metrics.get(("retention_d1", "")) == pytest.approx(2 / 3)
                and metrics.get(("retention_d7", "")) == pytest.approx(2 / 3))

    wait_for(updated, timeout=90)


def test_empty_day_and_division_by_zero(analytics):
    day = date(1999, 1, 1)
    response = analytics.post(f"/recalculate/{day}")
    assert response.status_code == 200, response.text
    values = metric_map(postgres_metrics(day))
    assert values[("dau", "")] == 0
    for name in ("avg_watch_seconds", "view_conversion", "retention_d1", "retention_d7"):
        assert values[(name, "")] is None
