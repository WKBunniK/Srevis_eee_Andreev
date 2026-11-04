import logging
import time

import grpc

log = logging.getLogger(__name__)
RETRYABLE = {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}


def invoke(stub, method, request, metadata, timeout):
    for attempt in range(3):
        try:
            return getattr(stub, method)(request, metadata=metadata, timeout=timeout)
        except grpc.RpcError as error:
            if error.code() not in RETRYABLE or attempt == 2:
                raise
            delay = 0.1 * (2 ** attempt)
            log.warning("grpc retry method=%s attempt=%s code=%s delay=%s",
                        method, attempt + 2, error.code().name, delay)
            time.sleep(delay)
