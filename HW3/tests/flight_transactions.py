import os
import unittest
from uuid import uuid4
import grpc
import psycopg
from psycopg import sql
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from common.db import connect


class Transactions(unittest.TestCase):
    def test_atomic_reserve_release_and_constraints(self):
        booking_id = str(uuid4())
        auth = (("x-api-key", os.environ["FLIGHT_API_KEY"]),)
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = rpc.FlightServiceStub(channel)
            def seats():
                return stub.GetFlight(pb.GetFlightRequest(id=1), metadata=auth).available_seats
            before = seats()

            with connect() as conn:
                conn.execute(sql.SQL("""CREATE FUNCTION hw3_test_failure() RETURNS trigger LANGUAGE plpgsql AS $$
                    BEGIN IF NEW.booking_id = {}::uuid THEN RAISE EXCEPTION 'Injected test failure'; END IF;
                    RETURN NEW; END $$;
                    CREATE TRIGGER hw3_test_trigger BEFORE INSERT OR UPDATE ON seat_reservations
                    FOR EACH ROW EXECUTE FUNCTION hw3_test_failure();""").format(sql.Literal(booking_id)))
            try:
                with self.assertRaises(grpc.RpcError) as caught:
                    stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id=booking_id), metadata=auth)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.INTERNAL)
                self.assertEqual(seats(), before)
                with connect() as conn:
                    self.assertIsNone(conn.execute("SELECT * FROM seat_reservations WHERE booking_id=%s", (booking_id,)).fetchone())
                    conn.execute("ALTER TABLE seat_reservations DISABLE TRIGGER hw3_test_trigger")
                stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id=booking_id), metadata=auth)
                self.assertEqual(seats(), before - 1)
                repeated = stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id=booking_id), metadata=auth)
                self.assertEqual(repeated.status, pb.ACTIVE)
                with self.assertRaises(grpc.RpcError) as duplicate:
                    stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=1, seat_count=2, booking_id=booking_id), metadata=auth)
                self.assertEqual(duplicate.exception.code(), grpc.StatusCode.ALREADY_EXISTS)
                self.assertEqual(seats(), before - 1)
                with connect() as conn:
                    conn.execute("ALTER TABLE seat_reservations ENABLE TRIGGER hw3_test_trigger")
                with self.assertRaises(grpc.RpcError) as caught:
                    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=auth)
                self.assertEqual(caught.exception.code(), grpc.StatusCode.INTERNAL)
                self.assertEqual(seats(), before - 1)
                with connect() as conn:
                    self.assertEqual(conn.execute("SELECT status FROM seat_reservations WHERE booking_id=%s",
                                                  (booking_id,)).fetchone()["status"], "ACTIVE")
            finally:
                with connect() as conn:
                    conn.execute("DROP TRIGGER hw3_test_trigger ON seat_reservations; DROP FUNCTION hw3_test_failure();")
                try:
                    stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=auth)
                except grpc.RpcError as error:
                    if error.code() != grpc.StatusCode.NOT_FOUND:
                        raise

            stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=auth)
            self.assertEqual(seats(), before)
            for query in ["UPDATE flights SET available_seats=-1 WHERE id=1",
                          "UPDATE flights SET available_seats=total_seats+1 WHERE id=1",
                          "UPDATE flights SET price_minor=0 WHERE id=1",
                          "UPDATE flights SET total_seats=0 WHERE id=1"]:
                with self.assertRaises(psycopg.errors.CheckViolation):
                    with connect() as conn:
                        conn.execute(query)
            with self.assertRaises(psycopg.errors.UniqueViolation):
                with connect() as conn:
                    conn.execute("""INSERT INTO flights (flight_number, airline, origin, destination,
                        departure_at, arrival_at, total_seats, available_seats, price_minor, status)
                        SELECT flight_number, airline, origin, destination, departure_at + interval '1 hour',
                        arrival_at + interval '1 hour', total_seats, available_seats, price_minor, status
                        FROM flights WHERE id=1""")
            with connect() as conn:
                conn.execute("DELETE FROM seat_reservations WHERE booking_id=%s", (booking_id,))


if __name__ == "__main__":
    unittest.main(verbosity=2)
