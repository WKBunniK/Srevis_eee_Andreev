import json
import os
import threading
import unittest
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from psycopg import sql
from uuid import uuid4

import grpc
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import connect

BASE = os.getenv("TEST_BASE_URL", "http://localhost:8000")


def http(method, path, data=None):
    request = urllib.request.Request(BASE + path, method=method,
        data=json.dumps(data).encode() if data is not None else None,
        headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return response.status, json.load(response)
    except urllib.error.HTTPError as error:
        return error.code, json.load(error)


class Integration(unittest.TestCase):
    def setUp(self):
        self.user = "test-" + str(uuid4())
        self.created = []
        self.channel = grpc.insecure_channel(os.getenv("FLIGHT_GRPC_TARGET", "flight:50051"))
        self.stub = rpc.FlightServiceStub(self.channel)
        self.auth = (("x-api-key", os.environ["FLIGHT_API_KEY"]),)

    def tearDown(self):
        for booking_id in self.created:
            http("POST", f"/bookings/{booking_id}/cancel")
        self.channel.close()

    def payload(self, **overrides):
        return {"user_id": self.user, "flight_id": 1, "passenger_name": "Test Passenger",
                "passenger_email": "test@example.com", "seat_count": 1, **overrides}

    def create(self, **overrides):
        status, body = http("POST", "/bookings", self.payload(**overrides))
        if status == 201:
            self.created.append(body["id"])
        return status, body

    def assert_rpc(self, code, method, request, metadata=None):
        with self.assertRaises(grpc.RpcError) as caught:
            method(request, metadata=self.auth if metadata is None else metadata, timeout=5)
        self.assertEqual(caught.exception.code(), code)

    @contextmanager
    def reject_booking_write(self, event):
        name = "test_booking_" + uuid4().hex
        with connect() as conn:
            conn.execute(sql.SQL("""CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN IF NEW.user_id = {user} THEN RAISE EXCEPTION 'Injected booking failure'; END IF;
                RETURN NEW; END $$;
                CREATE TRIGGER {name} BEFORE {event} ON bookings
                FOR EACH ROW EXECUTE FUNCTION {name}();""").format(
                    name=sql.Identifier(name), user=sql.Literal(self.user), event=sql.SQL(event)))
        try:
            yield
        finally:
            with connect() as conn:
                conn.execute(sql.SQL("DROP TRIGGER {name} ON bookings; DROP FUNCTION {name}();").format(
                    name=sql.Identifier(name)))

    def test_booking_insert_failure_compensates_reservation(self):
        before = http("GET", "/flights/1")[1]["available_seats"]
        with self.reject_booking_write("INSERT"):
            status, body = self.create()
            self.assertEqual(status, 503, body)
        self.assertEqual(http("GET", f"/bookings?user_id={self.user}")[1], [])
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before)

    def test_cancel_recovers_after_local_update_failure(self):
        before = http("GET", "/flights/1")[1]["available_seats"]
        status, booking = self.create()
        self.assertEqual(status, 201, booking)
        path = f'/bookings/{booking["id"]}'
        with self.reject_booking_write("UPDATE"):
            self.assertEqual(http("POST", path + "/cancel")[0], 503)
        self.assertEqual(http("GET", path)[1]["status"], "CONFIRMED")
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before)
        status, cancelled = http("POST", path + "/cancel")
        self.assertEqual(status, 200, cancelled)
        self.assertEqual(cancelled["status"], "CANCELLED")
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before)

    def test_price_snapshot_survives_flight_update(self):
        original = http("GET", "/flights/1")[1]["price_minor"]
        status, booking = self.create(seat_count=2)
        self.assertEqual(status, 201, booking)
        try:
            self.stub.UpdateFlight(pb.UpdateFlightRequest(id=1, price_minor=original + 10000), metadata=self.auth)
            self.assertEqual(http("GET", "/flights/1")[1]["price_minor"], original + 10000)
            self.assertEqual(http("GET", f'/bookings/{booking["id"]}')[1]["total_price_minor"], 2 * original)
        finally:
            self.stub.UpdateFlight(pb.UpdateFlightRequest(id=1, price_minor=original), metadata=self.auth)

    def test_update_flight_rejects_invalid_arguments(self):
        for request in (pb.UpdateFlightRequest(id=0, price_minor=1), pb.UpdateFlightRequest(id=1),
                        pb.UpdateFlightRequest(id=1, price_minor=0), pb.UpdateFlightRequest(id=1, status=0),
                        pb.UpdateFlightRequest(id=1, status=99)):
            self.assert_rpc(grpc.StatusCode.INVALID_ARGUMENT, self.stub.UpdateFlight, request)
        self.assert_rpc(grpc.StatusCode.NOT_FOUND, self.stub.UpdateFlight,
                        pb.UpdateFlightRequest(id=999999999, price_minor=1))

    def test_search_get_and_validation(self):
        status, flights = http("GET", "/flights?origin=SVO&destination=LED&date=2026-12-01")
        self.assertEqual(status, 200)
        self.assertEqual([f["id"] for f in flights], [1, 2])
        self.assertTrue(all(f["status"] == "SCHEDULED" for f in flights))
        self.assertEqual(len(http("GET", "/flights?origin=SVO&destination=LED")[1]), 3)
        self.assertEqual(http("GET", "/flights?origin=LED&destination=SVO")[1], [])
        self.assertEqual(http("GET", "/flights/1")[1]["price_minor"], 750000)
        for path in ["/flights?origin=SVO", "/flights?origin=xx&destination=LED",
                     "/flights?origin=SVO&destination=SVO", "/flights/0",
                     "/flights?origin=SVO&destination=LED&date=not-a-date"]:
            self.assertEqual(http("GET", path)[0], 422, path)
        self.assertEqual(http("GET", "/flights/999999999")[0], 404)

    def test_booking_lifecycle(self):
        before = http("GET", "/flights/1")[1]["available_seats"]
        status, booking = self.create(seat_count=2)
        self.assertEqual(status, 201, booking)
        self.assertEqual(booking["total_price_minor"], 1500000)
        self.assertEqual(booking["status"], "CONFIRMED")
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before - 2)
        self.assertEqual(http("GET", f'/bookings/{booking["id"]}')[1], booking)
        self.assertEqual(http("GET", f"/bookings?user_id={self.user}")[1], [booking])
        status, cancelled = http("POST", f'/bookings/{booking["id"]}/cancel')
        self.assertEqual(status, 200)
        self.assertEqual(cancelled["status"], "CANCELLED")
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before)
        self.assertEqual(http("POST", f'/bookings/{booking["id"]}/cancel')[0], 409)

    def test_errors_leave_no_booking(self):
        before = http("GET", "/flights/1")[1]["available_seats"]
        for overrides, expected in [({"seat_count": 1000}, 409), ({"flight_id": 999999999}, 404),
            ({"flight_id": 4}, 409), ({"seat_count": 0}, 422), ({"seat_count": -1}, 422),
            ({"seat_count": True}, 422), ({"passenger_email": "invalid"}, 422),
            ({"passenger_name": "   "}, 422), ({"user_id": ""}, 422),
            ({"passenger_name": "Ivan\x00Petrov"}, 422), ({"user_id": "user\x00id"}, 422)]:
            self.assertEqual(self.create(**overrides)[0], expected, overrides)
        self.assertEqual(http("GET", f"/bookings?user_id={self.user}")[1], [])
        with connect() as conn:
            self.assertEqual(conn.execute("SELECT count(*) AS n FROM bookings WHERE user_id=%s",
                                         (self.user,)).fetchone()["n"], 0)
        self.assertEqual(http("GET", "/flights/1")[1]["available_seats"], before)
        missing = str(uuid4())
        self.assertEqual(http("GET", f"/bookings/{missing}")[0], 404)
        self.assertEqual(http("POST", f"/bookings/{missing}/cancel")[0], 404)
        self.assertEqual(http("GET", "/bookings/not-a-uuid")[0], 422)
        self.assertEqual(http("GET", "/bookings")[0], 422)
        self.assertEqual(http("GET", "/bookings?user_id=bad%00id")[0], 422)

    def test_concurrent_last_seat_and_cancellation(self):
        self.assertEqual(http("GET", "/flights/2")[1]["available_seats"], 1,
                         "Demo flight 2 must be free before the test")
        barrier = threading.Barrier(12)
        def reserve(_):
            barrier.wait(timeout=10)
            return http("POST", "/bookings", self.payload(flight_id=2))
        with ThreadPoolExecutor(max_workers=12) as pool:
            results = list(pool.map(reserve, range(12)))
        self.created.extend(body["id"] for status, body in results if status == 201)
        self.assertEqual(sorted(status for status, _ in results), [201] + [409] * 11)
        self.assertEqual(http("GET", "/flights/2")[1]["available_seats"], 0)
        self.assertEqual(len(http("GET", f"/bookings?user_id={self.user}")[1]), 1)
        booking_id = self.created[0]
        with ThreadPoolExecutor(max_workers=2) as pool:
            cancelled = list(pool.map(lambda _: http("POST", f"/bookings/{booking_id}/cancel")[0], range(2)))
        self.assertEqual(sorted(cancelled), [200, 409])
        self.assertEqual(http("GET", "/flights/2")[1]["available_seats"], 1)

    def test_all_grpc_methods_require_authentication(self):
        calls = [(self.stub.SearchFlights, pb.SearchFlightsRequest(origin="SVO", destination="LED")),
                 (self.stub.GetFlight, pb.GetFlightRequest(id=1)),
                 (self.stub.UpdateFlight, pb.UpdateFlightRequest(id=1, price_minor=750000)),
                 (self.stub.ReserveSeats, pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id=str(uuid4()))),
                 (self.stub.ReleaseReservation, pb.ReleaseReservationRequest(booking_id=str(uuid4())))]
        for method, request in calls:
            for metadata in [(), (("x-api-key", "wrong"),), self.auth + self.auth]:
                self.assert_rpc(grpc.StatusCode.UNAUTHENTICATED, method, request, metadata)

    def test_grpc_business_errors(self):
        self.assert_rpc(grpc.StatusCode.NOT_FOUND, self.stub.GetFlight, pb.GetFlightRequest(id=999999999))
        self.assert_rpc(grpc.StatusCode.INVALID_ARGUMENT, self.stub.GetFlight, pb.GetFlightRequest(id=0))
        self.assert_rpc(grpc.StatusCode.INVALID_ARGUMENT, self.stub.ReserveSeats,
                        pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id="bad"))
        self.assert_rpc(grpc.StatusCode.NOT_FOUND, self.stub.ReleaseReservation,
                        pb.ReleaseReservationRequest(booking_id=str(uuid4())))
        self.assert_rpc(grpc.StatusCode.RESOURCE_EXHAUSTED, self.stub.ReserveSeats,
                        pb.ReserveSeatsRequest(flight_id=1, seat_count=1000, booking_id=str(uuid4())))
        self.assert_rpc(grpc.StatusCode.FAILED_PRECONDITION, self.stub.ReserveSeats,
                        pb.ReserveSeatsRequest(flight_id=4, seat_count=1, booking_id=str(uuid4())))


if __name__ == "__main__":
    unittest.main(verbosity=2)
