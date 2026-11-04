import logging
import os
from contextlib import asynccontextmanager
from datetime import date as Date, datetime, time, timezone
from uuid import UUID, uuid4

import grpc
import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, EmailStr, Field
from google.protobuf.timestamp_pb2 import Timestamp
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import connect
from booking.flight_client import invoke

log = logging.getLogger(__name__)
TEXT_PATTERN = r"^[^\x00-\x1f\x7f]+$"


@asynccontextmanager
async def lifespan(app):
    key = os.environ["FLIGHT_API_KEY"]
    if not key:
        raise ValueError("FLIGHT_API_KEY must not be empty")
    app.state.metadata = (("x-api-key", key),)
    app.state.timeout = float(os.getenv("GRPC_TIMEOUT_SECONDS", "5"))
    app.state.channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight:50051"),
                                              options=(("grpc.enable_retries", 0),))
    app.state.flight = rpc.FlightServiceStub(app.state.channel)
    yield
    app.state.channel.close()


app = FastAPI(title="Flight Booking", lifespan=lifespan)

GRPC_TO_HTTP = {
    grpc.StatusCode.INVALID_ARGUMENT: 422,
    grpc.StatusCode.NOT_FOUND: 404,
    grpc.StatusCode.RESOURCE_EXHAUSTED: 409,
    grpc.StatusCode.FAILED_PRECONDITION: 409,
    grpc.StatusCode.ALREADY_EXISTS: 409,
    grpc.StatusCode.UNAUTHENTICATED: 502,
    grpc.StatusCode.PERMISSION_DENIED: 502,
    grpc.StatusCode.UNAVAILABLE: 503,
    grpc.StatusCode.DEADLINE_EXCEEDED: 504,
}


def call(method, request):
    try:
        return invoke(app.state.flight, method, request, app.state.metadata, app.state.timeout)
    except grpc.RpcError as exc:
        raise HTTPException(GRPC_TO_HTTP.get(exc.code(), 502), exc.details()) from exc


@app.exception_handler(psycopg.Error)
async def database_error(request, exc):
    log.error("Booking database error", exc_info=exc)
    return JSONResponse(status_code=503, content={"detail": "Booking database operation failed"})


def flight_json(flight):
    return {
        "id": flight.id, "flight_number": flight.flight_number, "airline": flight.airline,
        "origin": flight.origin, "destination": flight.destination,
        "departure_at": flight.departure_at.ToJsonString(), "arrival_at": flight.arrival_at.ToJsonString(),
        "total_seats": flight.total_seats, "available_seats": flight.available_seats,
        "price_minor": flight.price_minor, "currency": "RUB", "status": pb.FlightStatus.Name(flight.status),
    }


class BookingInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")
    user_id: str = Field(min_length=1, max_length=100, pattern=TEXT_PATTERN)
    flight_id: int = Field(gt=0, le=9223372036854775807, strict=True)
    passenger_name: str = Field(min_length=1, max_length=200, pattern=TEXT_PATTERN)
    passenger_email: EmailStr = Field(max_length=254)
    seat_count: int = Field(gt=0, le=2147483647, strict=True)


@app.get("/health")
def health():
    with connect() as conn:
        conn.execute("SELECT 1 FROM bookings LIMIT 1")
    return {"status": "ok"}


@app.get("/flights")
def search_flights(origin: str = Query(pattern="^[A-Z]{3}$"),
                   destination: str = Query(pattern="^[A-Z]{3}$"), date: Date | None = None):
    request = pb.SearchFlightsRequest(origin=origin, destination=destination)
    if date is not None:
        ts = Timestamp()
        ts.FromDatetime(datetime.combine(date, time(), timezone.utc))
        request.departure_date.CopyFrom(ts)
    return [flight_json(f) for f in call("SearchFlights", request).flights]


@app.get("/flights/{flight_id}")
def get_flight(flight_id: int):
    if not 0 < flight_id <= 9223372036854775807:
        raise HTTPException(422, "flight_id must be a positive int64")
    return flight_json(call("GetFlight", pb.GetFlightRequest(id=flight_id)))


def compensate(booking_id):
    try:
        call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
    except HTTPException:
        log.exception("Compensation failed; reconcile booking_id=%s manually", booking_id)


@app.post("/bookings", status_code=201)
def create_booking(data: BookingInput):
    flight = call("GetFlight", pb.GetFlightRequest(id=data.flight_id))
    total = data.seat_count * flight.price_minor
    if not 0 < total <= 9223372036854775807:
        raise HTTPException(422, "Total price exceeds supported range")
    booking_id = uuid4()

    with connect() as conn:
        try:
            call("ReserveSeats", pb.ReserveSeatsRequest(flight_id=data.flight_id,
                 seat_count=data.seat_count, booking_id=str(booking_id)))
        except HTTPException as exc:
            if exc.status_code in {502, 503, 504}:
                log.error("Uncertain reservation outcome; booking_id=%s", booking_id)
                compensate(booking_id)
            raise
        try:
            row = conn.execute("""INSERT INTO bookings
                (id,user_id,flight_id,passenger_name,passenger_email,seat_count,total_price_minor,status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,'CONFIRMED') RETURNING *""",
                (booking_id, data.user_id, data.flight_id, data.passenger_name,
                 str(data.passenger_email), data.seat_count, total)).fetchone()
        except Exception:
            compensate(booking_id)
            raise
        try:
            conn.commit()
        except Exception:
            log.exception("Uncertain booking COMMIT; reconcile booking_id=%s manually", booking_id)
            raise
    return row


@app.get("/bookings")
def list_bookings(user_id: str = Query(min_length=1, max_length=100, pattern=TEXT_PATTERN)):
    if not user_id.strip():
        raise HTTPException(422, "user_id must not be blank")
    with connect() as conn:
        return conn.execute("SELECT * FROM bookings WHERE user_id=%s ORDER BY created_at, id", (user_id,)).fetchall()


@app.get("/bookings/{booking_id}")
def get_booking(booking_id: UUID):
    with connect() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id=%s", (booking_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Booking not found")
        return row


@app.post("/bookings/{booking_id}/cancel")
def cancel_booking(booking_id: UUID):
    with connect() as conn:
        row = conn.execute("SELECT * FROM bookings WHERE id=%s FOR UPDATE", (booking_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "Booking not found")
        if row["status"] != "CONFIRMED":
            raise HTTPException(409, "Booking is already cancelled")
        call("ReleaseReservation", pb.ReleaseReservationRequest(booking_id=str(booking_id)))
        row = conn.execute("UPDATE bookings SET status='CANCELLED' WHERE id=%s RETURNING *", (booking_id,)).fetchone()
    return row
