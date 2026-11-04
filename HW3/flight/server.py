import functools
import hmac
import logging
import os
import re
import signal
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta, timezone
from uuid import UUID

import grpc
import psycopg
from google.protobuf.timestamp_pb2 import Timestamp
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import connect
from flight.cache import FlightCache, cache_version, advance_version

log = logging.getLogger(__name__)


class ApiKeyInterceptor(grpc.ServerInterceptor):

    def __init__(self, key):
        if not key:
            raise ValueError("FLIGHT_API_KEY must not be empty")
        self.key = key.encode()

    def intercept_service(self, continuation, details):
        handler = continuation(details)
        if handler is None:
            return None
        values = [v for k, v in details.invocation_metadata if k == "x-api-key"]
        if len(values) == 1 and hmac.compare_digest(values[0].encode(), self.key):
            return handler

        def reject(request, context):
            context.abort(grpc.StatusCode.UNAUTHENTICATED, "Missing or invalid API key")
        return grpc.unary_unary_rpc_method_handler(
            reject, request_deserializer=handler.request_deserializer,
            response_serializer=handler.response_serializer)


def db_errors(method):
    @functools.wraps(method)
    def wrapper(self, request, context):
        try:
            return method(self, request, context)
        except psycopg.errors.UniqueViolation:
            context.abort(grpc.StatusCode.ALREADY_EXISTS, "Booking already has a reservation")
        except (psycopg.OperationalError, psycopg.InterfaceError):
            log.exception("Database unavailable")
            context.abort(grpc.StatusCode.UNAVAILABLE, "Flight database unavailable")
        except psycopg.Error:
            log.exception("Database operation failed")
            context.abort(grpc.StatusCode.INTERNAL, "Flight database operation failed")
    return wrapper


def timestamp(value):
    result = Timestamp()
    result.FromDatetime(value)
    return result


def flight_message(row):
    values = dict(row)
    values["status"] = pb.FlightStatus.Value(values["status"])
    values["departure_at"] = timestamp(values["departure_at"])
    values["arrival_at"] = timestamp(values["arrival_at"])
    return pb.Flight(**values)


def reservation_message(row):
    return pb.SeatReservation(booking_id=str(row["booking_id"]), flight_id=row["flight_id"],
                              seat_count=row["seat_count"], status=pb.ReservationStatus.Value(row["status"]),
                              created_at=timestamp(row["created_at"]))


def booking_uuid(value, context):
    try:
        return UUID(value)
    except ValueError:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, "booking_id must be a UUID")


def positive(value, name, context):
    if value <= 0:
        context.abort(grpc.StatusCode.INVALID_ARGUMENT, f"{name} must be positive")


class FlightService(rpc.FlightServiceServicer):
    def __init__(self, cache=None):
        self.cache = cache if cache is not None else FlightCache()

    @db_errors
    def SearchFlights(self, request, context):
        if not all(re.fullmatch(r"[A-Z]{3}", value) for value in (request.origin, request.destination)):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Use three uppercase IATA letters")
        if request.origin == request.destination:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Airports must differ")
        sql = "SELECT * FROM flights WHERE origin=%s AND destination=%s AND status='SCHEDULED'"
        params = [request.origin, request.destination]
        if request.HasField("departure_date"):
            try:
                day = request.departure_date.ToDatetime(tzinfo=timezone.utc)
                if (day.hour, day.minute, day.second, request.departure_date.nanos) != (0, 0, 0, 0):
                    raise ValueError("Expected midnight")
                end = day + timedelta(days=1)
            except (ValueError, OverflowError):
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "departure_date must be midnight UTC")
            sql += " AND departure_at >= %s AND departure_at < %s"
            params.extend([day, end])
        date_key = day.date().isoformat() if request.HasField("departure_date") else "all"
        key = f"search:{request.origin}:{request.destination}:{date_key}"
        with connect() as conn:
            version = cache_version(conn)
            cached = self.cache.get(key, version, pb.SearchFlightsResponse)
            if cached is not None:
                return cached
            rows = conn.execute(sql + " ORDER BY departure_at, id", params).fetchall()
            response = pb.SearchFlightsResponse(flights=[flight_message(row) for row in rows])
            self.cache.set(key, version, response)
        return response

    @db_errors
    def GetFlight(self, request, context):
        positive(request.id, "id", context)
        key = f"flight:{request.id}"
        with connect() as conn:
            version = cache_version(conn)
            cached = self.cache.get(key, version, pb.Flight)
            if cached is not None:
                return cached
            row = conn.execute("SELECT * FROM flights WHERE id=%s", (request.id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            response = flight_message(row)
            self.cache.set(key, version, response)
        return response

    @db_errors
    def UpdateFlight(self, request, context):
        positive(request.id, "id", context)
        if not request.HasField("price_minor") and not request.HasField("status"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "No fields to update")
        if request.HasField("price_minor"):
            positive(request.price_minor, "price_minor", context)
        if request.HasField("status") and request.status not in {pb.SCHEDULED, pb.DEPARTED, pb.CANCELLED, pb.COMPLETED}:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Invalid flight status")
        with connect() as conn:
            cache_version(conn, write=True)
            row = conn.execute("SELECT * FROM flights WHERE id=%s FOR UPDATE", (request.id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            price = request.price_minor if request.HasField("price_minor") else row["price_minor"]
            status = pb.FlightStatus.Name(request.status) if request.HasField("status") else row["status"]
            row = conn.execute("UPDATE flights SET price_minor=%s, status=%s WHERE id=%s RETURNING *",
                               (price, status, request.id)).fetchone()
            advance_version(conn)
            self.cache.invalidate()
        return flight_message(row)

    @db_errors
    def ReserveSeats(self, request, context):
        positive(request.flight_id, "flight_id", context)
        positive(request.seat_count, "seat_count", context)
        booking_id = booking_uuid(request.booking_id, context)
        with connect() as conn:
            cache_version(conn, write=True)
            existing = conn.execute("SELECT * FROM seat_reservations WHERE booking_id=%s FOR UPDATE",
                                    (booking_id,)).fetchone()
            if existing is not None:
                if existing["flight_id"] != request.flight_id or existing["seat_count"] != request.seat_count:
                    context.abort(grpc.StatusCode.ALREADY_EXISTS, "booking_id belongs to different reservation parameters")
                if existing["status"] != "ACTIVE":
                    context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation is no longer active")
                return reservation_message(existing)
            flight = conn.execute("SELECT * FROM flights WHERE id=%s FOR UPDATE", (request.flight_id,)).fetchone()
            if flight is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Flight not found")
            if flight["status"] != "SCHEDULED":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Flight is not scheduled")
            if flight["available_seats"] < request.seat_count:
                context.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "Not enough seats")
            conn.execute("UPDATE flights SET available_seats=available_seats-%s WHERE id=%s",
                         (request.seat_count, request.flight_id))
            row = conn.execute("""INSERT INTO seat_reservations (booking_id, flight_id, seat_count, status)
                                  VALUES (%s,%s,%s,'ACTIVE') RETURNING *""",
                               (booking_id, request.flight_id, request.seat_count)).fetchone()
            advance_version(conn)
            self.cache.invalidate()

        return reservation_message(row)

    @db_errors
    def ReleaseReservation(self, request, context):
        booking_id = booking_uuid(request.booking_id, context)
        with connect() as conn:
            cache_version(conn, write=True)
            row = conn.execute("SELECT * FROM seat_reservations WHERE booking_id=%s FOR UPDATE",
                               (booking_id,)).fetchone()
            if row is None:
                context.abort(grpc.StatusCode.NOT_FOUND, "Reservation not found")
            if row["status"] == "RELEASED":
                return reservation_message(row)
            if row["status"] != "ACTIVE":
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "Reservation is not active")

            conn.execute("UPDATE flights SET available_seats=available_seats+%s WHERE id=%s",
                         (row["seat_count"], row["flight_id"]))
            row = conn.execute("""UPDATE seat_reservations SET status='RELEASED'
                                  WHERE booking_id=%s RETURNING *""", (booking_id,)).fetchone()
            advance_version(conn)
            self.cache.invalidate()
        return reservation_message(row)


def main():
    logging.basicConfig(level=logging.INFO)
    server = grpc.server(ThreadPoolExecutor(max_workers=16),
                         interceptors=[ApiKeyInterceptor(os.environ["FLIGHT_API_KEY"])])
    rpc.add_FlightServiceServicer_to_server(FlightService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    log.info("Flight gRPC server listening on 50051")
    signal.signal(signal.SIGTERM, lambda *_: server.stop(5))
    server.wait_for_termination()


if __name__ == "__main__":
    main()
