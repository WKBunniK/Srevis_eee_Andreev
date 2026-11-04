import json
import os
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import grpc
from redis import ConnectionError
from unittest.mock import patch

import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import connect
from flight.cache import FlightCache, advance_version, cache_version, redis_client
from flight.server import flight_message


class CacheTests(unittest.TestCase):
    def setUp(self):
        self.channel = grpc.insecure_channel("localhost:50051", options=(("grpc.enable_retries", 0),))
        self.stub = rpc.FlightServiceStub(self.channel)
        self.auth = (("x-api-key", os.environ["FLIGHT_API_KEY"]),)
        self.redis = redis_client()
        with connect() as conn:
            cache_version(conn, write=True)
            self.flight_id = conn.execute("""INSERT INTO flights
                (flight_number,airline,origin,destination,departure_at,arrival_at,total_seats,available_seats,price_minor,status)
                VALUES (%s,'Test Air','AMS','CDG','2030-01-01 10:00+00','2030-01-01 12:00+00',3,3,12345,'SCHEDULED')
                RETURNING id""", ("T" + uuid4().hex[:10],)).fetchone()["id"]
            advance_version(conn)
            FlightCache(self.redis).invalidate()
        self.reservations = []

    def tearDown(self):
        for booking_id in self.reservations:
            self.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=self.auth)
        with connect() as conn:
            cache_version(conn, write=True)
            conn.execute("DELETE FROM seat_reservations WHERE flight_id=%s", (self.flight_id,))
            conn.execute("DELETE FROM flights WHERE id=%s", (self.flight_id,))
            advance_version(conn)
            FlightCache(self.redis).invalidate()
        self.channel.close()

    def get(self):
        return self.stub.GetFlight(pb.GetFlightRequest(id=self.flight_id), metadata=self.auth, timeout=10)

    def search(self):
        return self.stub.SearchFlights(pb.SearchFlightsRequest(origin="AMS", destination="CDG"), metadata=self.auth, timeout=10)

    def reserve(self, booking_id=None, count=1):
        booking_id = booking_id or str(uuid4())
        self.reservations.append(booking_id)
        return self.stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=self.flight_id, seat_count=count,
                                                            booking_id=booking_id), metadata=self.auth, timeout=10)

    def test_cache_aside_ttl_and_all_mutations(self):
        key = f"flight:{self.flight_id}"
        search_key = "search:AMS:CDG:all"
        self.assertEqual(self.get().available_seats, 3)
        self.assertEqual(len(self.search().flights), 1)
        for name in (key, search_key):
            self.assertTrue(self.redis.exists(name))
            self.assertTrue(0 < self.redis.ttl(name) <= 300)
        old = self.redis.get(key)
        self.assertEqual(self.get().available_seats, 3)
        self.assertEqual(self.redis.get(key), old)
        reservation = self.reserve()
        self.assertIsNone(self.redis.get(key))
        self.assertIsNone(self.redis.get(search_key))
        self.assertEqual(self.get().available_seats, 2)
        self.assertEqual(self.search().flights[0].available_seats, 2)
        self.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=reservation.booking_id), metadata=self.auth)
        self.assertIsNone(self.redis.get(key))
        self.assertIsNone(self.redis.get(search_key))
        self.assertEqual(self.get().available_seats, 3)
        self.search()
        self.stub.UpdateFlight(pb.UpdateFlightRequest(id=self.flight_id, price_minor=22222, status=pb.CANCELLED), metadata=self.auth)
        self.assertIsNone(self.redis.get(key))
        self.assertIsNone(self.redis.get(search_key))
        self.assertEqual(self.get().price_minor, 22222)
        self.assertEqual(list(self.search().flights), [])
        for name in self.redis.scan_iter(match="flight:*"):
            self.assertGreater(self.redis.ttl(name), 0)
        for name in self.redis.scan_iter(match="search:*"):
            self.assertGreater(self.redis.ttl(name), 0)

    def test_cache_hit_does_not_read_flights_table(self):
        self.get()
        self.search()
        with connect() as conn:
            conn.execute("LOCK TABLE flights IN ACCESS EXCLUSIVE MODE")
            flight = self.stub.GetFlight(pb.GetFlightRequest(id=self.flight_id), metadata=self.auth, timeout=2)
            search = self.stub.SearchFlights(pb.SearchFlightsRequest(origin="AMS", destination="CDG"),
                                             metadata=self.auth, timeout=2)
        self.assertEqual(flight.id, self.flight_id)
        self.assertEqual([item.id for item in search.flights], [self.flight_id])

    def test_stale_replica_data_is_not_served(self):
        key = f"flight:{self.flight_id}"
        self.get()
        self.search()
        stale_flight = self.redis.get(key)
        stale_search = self.redis.get("search:AMS:CDG:all")
        self.reserve()
        self.redis.set(key, stale_flight, ex=300)
        self.redis.set("search:AMS:CDG:all", stale_search, ex=300)
        self.assertEqual(self.get().available_seats, 2)
        self.assertEqual(self.search().flights[0].available_seats, 2)
        self.assertNotEqual(json.loads(self.redis.get(key))["version"], json.loads(stale_flight)["version"])

    def test_cache_fill_cannot_race_past_mutation(self):
        started = threading.Event()
        with ThreadPoolExecutor(max_workers=1) as pool:
            with connect() as conn:
                version = cache_version(conn)
                row = conn.execute("SELECT * FROM flights WHERE id=%s", (self.flight_id,)).fetchone()
                def reserve():
                    started.set()
                    return self.reserve()
                pending = pool.submit(reserve)
                self.assertTrue(started.wait(5))
                deadline = time.monotonic() + 3
                waiting = False
                while time.monotonic() < deadline:
                    conn.execute("SELECT pg_stat_clear_snapshot()")
                    waiting = conn.execute("""SELECT EXISTS (
                        SELECT 1 FROM pg_stat_activity WHERE datname=current_database()
                        AND pid <> pg_backend_pid() AND wait_event_type='Lock'
                        AND query LIKE 'SELECT version FROM cache_state%'
                    ) AS waiting""").fetchone()["waiting"]
                    if waiting:
                        break
                    time.sleep(0.02)
                self.assertTrue(waiting, "Reservation did not wait for the cache version lock")
                self.assertFalse(pending.done())
                FlightCache(self.redis).set(f"flight:{self.flight_id}", version, flight_message(row))
            pending.result(timeout=10)
        self.assertEqual(self.get().available_seats, 2)

    def test_cache_unavailable_is_a_miss(self):
        cache = FlightCache(self.redis)
        with patch.object(self.redis, "get", side_effect=ConnectionError("offline")):
            self.assertIsNone(cache.get("flight:1", 0, pb.Flight))
        with patch.object(self.redis, "set", side_effect=ConnectionError("offline")):
            cache.set("flight:1", 0, pb.Flight(id=1))
        with patch.object(self.redis, "scan_iter", side_effect=ConnectionError("offline")):
            cache.invalidate()

    def test_corrupt_cache_entry_is_replaced(self):
        key = f"flight:{self.flight_id}"
        self.redis.set(key, "not-json", ex=300)
        self.assertEqual(self.get().id, self.flight_id)
        self.assertIn("version", json.loads(self.redis.get(key)))

    def test_lost_reservation_response_is_retried_safely(self):
        from booking.flight_client import invoke
        for code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            calls = []
            def reserve(request, context):
                result = self.stub.ReserveSeats(request, metadata=self.auth, timeout=5)
                calls.append(request.booking_id)
                if len(calls) == 1:
                    context.abort(code, "Response lost after commit")
                return result
            server = grpc.server(ThreadPoolExecutor(max_workers=2))
            server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler(
                "flight.v1.FlightService", {"ReserveSeats": grpc.unary_unary_rpc_method_handler(
                    reserve, request_deserializer=pb.ReserveSeatsRequest.FromString,
                    response_serializer=pb.SeatReservation.SerializeToString)}),))
            port = server.add_insecure_port("127.0.0.1:0")
            server.start()
            booking_id = str(uuid4())
            self.reservations.append(booking_id)
            try:
                with grpc.insecure_channel(f"127.0.0.1:{port}", options=(("grpc.enable_retries", 0),)) as channel:
                    result = invoke(rpc.FlightServiceStub(channel), "ReserveSeats",
                                    pb.ReserveSeatsRequest(flight_id=self.flight_id, seat_count=1, booking_id=booking_id),
                                    self.auth, 5)
                self.assertEqual(result.booking_id, booking_id)
                self.assertEqual(calls, [booking_id, booking_id])
                self.assertEqual(self.get().available_seats, 2)
            finally:
                server.stop(0).wait()
                self.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=self.auth)

    def test_concurrent_identical_reservations(self):
        booking_id = str(uuid4())
        barrier = threading.Barrier(8)
        def reserve(_):
            barrier.wait(timeout=10)
            return self.reserve(booking_id)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(reserve, range(8)))
        self.assertTrue(all(result == results[0] for result in results))
        self.assertEqual(self.get().available_seats, 2)
        with connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM seat_reservations WHERE booking_id=%s",
                                          (booking_id,)).fetchone()["n"], 1)
        with self.assertRaises(grpc.RpcError) as error:
            self.stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=self.flight_id, seat_count=2,
                                                        booking_id=booking_id), metadata=self.auth)
        self.assertEqual(error.exception.code(), grpc.StatusCode.ALREADY_EXISTS)
        self.stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=self.auth)
        with self.assertRaises(grpc.RpcError) as error:
            self.stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=self.flight_id, seat_count=1,
                                                        booking_id=booking_id), metadata=self.auth)
        self.assertEqual(error.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
        self.assertEqual(self.get().available_seats, 3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
