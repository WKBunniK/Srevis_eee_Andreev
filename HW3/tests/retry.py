import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import grpc
import flight_pb2 as pb
import flight_pb2_grpc as rpc
from booking.flight_client import invoke


class FaultyService(rpc.FlightServiceServicer):
    def __init__(self, code, failures):
        self.code = code
        self.failures = failures
        self.calls = 0
        self.requests = []

    def GetFlight(self, request, context):
        self.calls += 1
        self.requests.append((request.SerializeToString(), tuple(context.invocation_metadata())))
        if self.calls <= self.failures:
            context.abort(self.code, "Injected RPC failure")
        return pb.Flight(id=request.id)


class RetryTests(unittest.TestCase):
    def run_case(self, code, failures):
        service = FaultyService(code, failures)
        server = grpc.server(ThreadPoolExecutor(max_workers=2))
        rpc.add_FlightServiceServicer_to_server(service, server)
        port = server.add_insecure_port("127.0.0.1:0")
        server.start()
        try:
            with grpc.insecure_channel(f"127.0.0.1:{port}", options=(("grpc.enable_retries", 0),)) as channel:
                grpc.channel_ready_future(channel).result(timeout=5)
                with patch("booking.flight_client.time.sleep") as sleep:
                    try:
                        result = invoke(rpc.FlightServiceStub(channel), "GetFlight", pb.GetFlightRequest(id=5),
                                        (("x-api-key", "test"),), 1)
                        error = None
                    except grpc.RpcError as exc:
                        result, error = None, exc.code()
            return service, sleep, result, error
        finally:
            server.stop(0).wait()

    def test_transient_failure_retries_with_backoff(self):
        for code in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
            with self.subTest(code=code):
                service, sleep, result, error = self.run_case(code, 2)
                self.assertIsNone(error)
                self.assertEqual(result.id, 5)
                self.assertEqual(service.calls, 3)
                self.assertEqual([args.args[0] for args in sleep.call_args_list], [0.1, 0.2])
                self.assertTrue(all(request == service.requests[0] for request in service.requests))

    def test_three_attempt_limit(self):
        service, sleep, result, error = self.run_case(grpc.StatusCode.UNAVAILABLE, 10)
        self.assertIsNone(result)
        self.assertEqual(error, grpc.StatusCode.UNAVAILABLE)
        self.assertEqual(service.calls, 3)
        self.assertEqual(sleep.call_count, 2)

    def test_business_and_auth_errors_are_not_retried(self):
        for code in (grpc.StatusCode.INVALID_ARGUMENT, grpc.StatusCode.NOT_FOUND,
                     grpc.StatusCode.RESOURCE_EXHAUSTED, grpc.StatusCode.UNAUTHENTICATED,
                     grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.ALREADY_EXISTS,
                     grpc.StatusCode.INTERNAL):
            with self.subTest(code=code):
                service, sleep, result, error = self.run_case(code, 10)
                self.assertEqual(error, code)
                self.assertEqual(service.calls, 1)
                sleep.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
