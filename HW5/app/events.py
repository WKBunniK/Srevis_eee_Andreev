import io
import json
import struct
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import UUID

from fastavro import parse_schema, schemaless_writer
from fastavro.validation import validate
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

from app.common import ROOT


class EventType(StrEnum):
    VIEW_STARTED = "VIEW_STARTED"
    VIEW_FINISHED = "VIEW_FINISHED"
    VIEW_PAUSED = "VIEW_PAUSED"
    VIEW_RESUMED = "VIEW_RESUMED"
    LIKED = "LIKED"
    SEARCHED = "SEARCHED"


class DeviceType(StrEnum):
    MOBILE = "MOBILE"
    DESKTOP = "DESKTOP"
    TV = "TV"
    TABLET = "TABLET"


class MovieEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: UUID
    user_id: str = Field(min_length=1, max_length=200)
    movie_id: str = Field(max_length=200)
    event_type: EventType
    timestamp: AwareDatetime
    device_type: DeviceType
    session_id: str = Field(min_length=1, max_length=200)
    progress_seconds: int = Field(ge=0, le=2147483647, strict=True)

    @field_validator("timestamp")
    @classmethod
    def utc_timestamp(cls, value):
        value = value.astimezone(UTC)
        if not datetime(1970, 1, 1, tzinfo=UTC) <= value < datetime(2100, 1, 1, tzinfo=UTC):
            raise ValueError("timestamp должен быть в диапазоне 1970–2099")
        return value.replace(microsecond=value.microsecond // 1000 * 1000)

    @model_validator(mode="after")
    def validate_event(self):
        if self.event_type != EventType.SEARCHED and not self.movie_id.strip():
            raise ValueError("movie_id обязателен для события фильма")
        if self.event_type in (EventType.VIEW_STARTED, EventType.SEARCHED) and self.progress_seconds != 0:
            raise ValueError("Для VIEW_STARTED и SEARCHED progress_seconds должен быть равен 0")
        return self


def local_schema():
    return json.loads((ROOT / "schemas/movie_event_v1.avsc").read_text())


class EventEncoder:
    def __init__(self, schema, schema_id):
        self.schema = parse_schema(schema)
        self.schema_id = schema_id

    def encode(self, event):
        record = event.model_dump(mode="json")
        record["timestamp"] = (event.timestamp - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(milliseconds=1)
        validate(record, self.schema, strict=True)
        buffer = io.BytesIO()
        buffer.write(struct.pack(">bI", 0, self.schema_id))
        schemaless_writer(buffer, self.schema, record)
        return buffer.getvalue()
