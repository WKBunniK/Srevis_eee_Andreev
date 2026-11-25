import logging
import random
from datetime import UTC, datetime, time, timedelta
from uuid import uuid4

from app.events import DeviceType, EventType, MovieEvent

logger = logging.getLogger(__name__)


def make_session(rng, day=None):
    day = day or (datetime.now(UTC).date() - timedelta(days=rng.randint(0, 8)))
    duration = rng.randint(600, 7200)
    start = datetime.combine(day, time(10), UTC) + timedelta(seconds=rng.randint(0, 18000))
    start = min(start, datetime.now(UTC) - timedelta(seconds=duration + 120))
    user_id = f"demo-user-{rng.randint(1, 20)}"
    movie_id = f"movie-{rng.randint(1, 10)}"
    device = rng.choice(list(DeviceType))
    session_id = str(uuid4())
    sequence = [(EventType.SEARCHED, 0, -10), (EventType.VIEW_STARTED, 0, 0)]
    pause = rng.random() < 0.6
    if pause:
        progress = duration // 3
        sequence.extend([(EventType.VIEW_PAUSED, progress, progress),
                         (EventType.VIEW_RESUMED, progress, progress + 60)])
    final_type = EventType.VIEW_FINISHED if rng.random() < 0.8 else EventType.VIEW_PAUSED
    sequence.append((final_type, duration, duration + (60 if pause else 0)))
    if final_type == EventType.VIEW_FINISHED and rng.random() < 0.4:
        sequence.append((EventType.LIKED, duration, duration + 90))
    return [
        MovieEvent(
            event_id=uuid4(), user_id=user_id,
            movie_id="" if kind == EventType.SEARCHED else movie_id,
            event_type=kind, timestamp=start + timedelta(seconds=offset),
            device_type=device, session_id=session_id, progress_seconds=progress,
        )
        for kind, progress, offset in sequence
    ]


def generate(publisher, stop, interval):
    rng = random.Random()
    while not stop.is_set():
        try:
            for event in make_session(rng):
                if stop.is_set():
                    return
                publisher.publish(event)
        except Exception:
            logger.exception("generator_session_failed")
        stop.wait(interval)
