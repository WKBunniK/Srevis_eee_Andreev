import base64
import json
import logging
import os

from google.protobuf.message import DecodeError
from redis import RedisError
from redis.sentinel import Sentinel

log = logging.getLogger(__name__)


def redis_client():
    addresses = [address.rsplit(":", 1) for address in os.environ["REDIS_SENTINELS"].split(",")]
    sentinel = Sentinel([(host, int(port)) for host, port in addresses],
                        socket_timeout=0.3, socket_connect_timeout=0.3)
    return sentinel.master_for(os.getenv("REDIS_MASTER_NAME", "flights"),
                               socket_timeout=0.3, socket_connect_timeout=0.3)


class FlightCache:
    def __init__(self, client=None):
        self.client = client if client is not None else redis_client()
        self.ttl = int(os.getenv("CACHE_TTL_SECONDS", "300"))
        if not 300 <= self.ttl <= 600:
            raise ValueError("CACHE_TTL_SECONDS must be between 300 and 600")

    def get(self, key, version, message_type):
        try:
            raw = self.client.get(key)
            if raw is not None:
                entry = json.loads(raw)
                if entry["version"] == version:
                    message = message_type.FromString(base64.b64decode(entry["data"], validate=True))
                    log.info("cache hit key=%s", key)
                    return message
                self.client.delete(key)
        except RedisError:
            log.warning("cache unavailable during get key=%s", key)
        except (ValueError, KeyError, TypeError, DecodeError):
            log.warning("invalid cache entry key=%s", key)
        log.info("cache miss key=%s", key)
        return None

    def set(self, key, version, message):
        entry = json.dumps({"version": version,
                            "data": base64.b64encode(message.SerializeToString()).decode()})
        try:
            self.client.set(key, entry, ex=self.ttl)
        except RedisError:
            log.warning("cache unavailable during set key=%s", key)

    def invalidate(self):
        try:
            for pattern in ("flight:*", "search:*"):
                for key in self.client.scan_iter(match=pattern, count=100):
                    self.client.delete(key)
            log.info("cache invalidated")
        except RedisError:
            log.warning("cache invalidation unavailable; PostgreSQL version invalidates old entries")


def cache_version(conn, write=False):
    lock = "FOR UPDATE" if write else "FOR SHARE"
    return conn.execute(f"SELECT version FROM cache_state WHERE id=1 {lock}").fetchone()["version"]


def advance_version(conn):
    conn.execute("UPDATE cache_state SET version=version+1 WHERE id=1")
