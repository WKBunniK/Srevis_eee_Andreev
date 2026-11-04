import os
import sys
import urllib.request
import grpc
import flight_pb2 as pb
import flight_pb2_grpc as rpc

if sys.argv[1] == "flight":
    with grpc.insecure_channel("localhost:50051") as channel:
        rpc.FlightServiceStub(channel).SearchFlights(
            pb.SearchFlightsRequest(origin="SVO", destination="LED"),
            metadata=(("x-api-key", os.environ["FLIGHT_API_KEY"]),), timeout=4)
else:
    with urllib.request.urlopen("http://localhost:8000/health", timeout=4) as response:
        assert response.status == 200
