import os
import sys
import time
from uuid import uuid4

import grpc
from redis import Redis
from redis.exceptions import RedisError
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from flight.cache import redis_client


def direct(host):
    return Redis(host=host, socket_timeout=1, socket_connect_timeout=1)


def wait_for(check, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if check():
                return
        except (RedisError, OSError):
            pass
        time.sleep(0.5)
    raise AssertionError("Redis topology did not reach the expected state")


def main():
    mode = sys.argv[1]
    client = redis_client()
    if mode == "master":
        wait_for(lambda: client.info("replication")["connected_slaves"] == 1)
        run_id = client.info("server")["run_id"]
        for host in ("redis-master", "redis-replica"):
            if direct(host).info("server")["run_id"] == run_id:
                print(host)
                return
        raise AssertionError("Unknown Redis master")
    if mode == "promoted":
        expected = "redis-replica" if sys.argv[2] == "redis-master" else "redis-master"
        def promoted():
            return (direct(expected).info("replication")["role"] == "master"
                    and client.info("server")["run_id"] == direct(expected).info("server")["run_id"])
        wait_for(promoted)
        with grpc.insecure_channel("localhost:50051") as channel:
            stub = rpc.FlightServiceStub(channel)
            auth = (("x-api-key", os.environ["FLIGHT_API_KEY"]),)
            flight = stub.GetFlight(pb.GetFlightRequest(id=1), metadata=auth, timeout=10)
            booking_id = str(uuid4())
            try:
                stub.ReserveSeats(pb.ReserveSeatsRequest(flight_id=1, seat_count=1, booking_id=booking_id), metadata=auth, timeout=10)
                updated = stub.GetFlight(pb.GetFlightRequest(id=1), metadata=auth, timeout=10)
                assert updated.available_seats == flight.available_seats - 1
                assert client.get("flight:1") is not None
                assert 0 < client.ttl("flight:1") <= 300
            finally:
                stub.ReleaseReservation(pb.ReleaseReservationRequest(booking_id=booking_id), metadata=auth, timeout=10)
            assert stub.GetFlight(pb.GetFlightRequest(id=1), metadata=auth, timeout=10).available_seats == flight.available_seats
        print(f"Failover passed: {expected} is master; gRPC and cache remain operational")
    elif mode == "recovered":
        hosts = ("redis-master", "redis-replica")
        def ready():
            states = [direct(host).info("replication") for host in hosts]
            return (sorted(state["role"] for state in states) == ["master", "slave"]
                    and all(state.get("master_link_status") == "up" for state in states if state["role"] == "slave")
                    and client.info("replication")["connected_slaves"] == 1)
        wait_for(ready)
        replica_host = next(host for host in hosts if direct(host).info("replication")["role"] == "slave")
        key = "failover:check:" + uuid4().hex
        value = uuid4().hex.encode()
        try:
            client.set(key, value, ex=300)
            wait_for(lambda: direct(replica_host).get(key) == value)
        finally:
            client.delete(key)
        print("Replication recovered: one master, one replica")
    else:
        raise ValueError(mode)


if __name__ == "__main__":
    main()
