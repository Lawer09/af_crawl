"""
RedisClient: Thread-safe Redis client with connection pooling and essential operations.

Configuration sources:
- Environment variables (preferred):
    REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD,
    REDIS_POOL_MAXSIZE, REDIS_SSL, REDIS_DECODE_RESPONSES,
    REDIS_SOCKET_TIMEOUT, REDIS_CLIENT_NAME
- Optional config dict `REDIS` in config.settings if present.

This class follows Python best practices: type hints, docstrings, and clear error handling.
"""
from __future__ import annotations

import os
import logging
import threading
from typing import Optional, Any

try:
    import redis
    from redis.exceptions import RedisError
except Exception as e:  # pragma: no cover
    # Provide a clear guidance if dependency is missing
    raise RuntimeError(
        "Missing dependency 'redis'. Please install it (e.g., pip install redis) or add it to requirements.txt"
    ) from e

logger = logging.getLogger(__name__)


def _load_config_from_sources() -> dict:
    """Load Redis configuration from config.settings.REDIS if present, otherwise from environment variables.

    Fallback defaults:
    - host: localhost
    - port: 6379
    - db: 1
    - password: None
    - ssl: False
    - pool_maxsize: 10
    - decode_responses: True
    - socket_timeout: 5 (seconds)
    - client_name: 'af_crawl'
    """
    settings_cfg: Optional[dict] = None
    try:
        import config.settings as settings  # type: ignore
        settings_cfg = getattr(settings, "REDIS", None)
    except Exception:
        settings_cfg = None

    def _env(name: str, default: Any) -> Any:
        val = os.getenv(name)
        return default if val is None or val == "" else val

    host = (settings_cfg or {}).get("host", _env("REDIS_HOST", "localhost"))
    port = int((settings_cfg or {}).get("port", _env("REDIS_PORT", "6379")))
    db = int((settings_cfg or {}).get("db", _env("REDIS_DB", "1")))
    password = (settings_cfg or {}).get("password", _env("REDIS_PASSWORD", "")) or None
    ssl_raw = (settings_cfg or {}).get("ssl", _env("REDIS_SSL", "false"))
    ssl = bool(str(ssl_raw).lower() in ("true", "1", "yes"))
    pool_maxsize = int((settings_cfg or {}).get("pool_maxsize", _env("REDIS_POOL_MAXSIZE", "10")))
    decode_responses = bool(str((settings_cfg or {}).get("decode_responses", _env("REDIS_DECODE_RESPONSES", "true"))).lower() in ("true", "1", "yes"))
    socket_timeout = float((settings_cfg or {}).get("socket_timeout", _env("REDIS_SOCKET_TIMEOUT", "5")))
    client_name = (settings_cfg or {}).get("client_name", _env("REDIS_CLIENT_NAME", "af_crawl"))

    return {
        "host": host,
        "port": port,
        "db": db,
        "password": password,
        "ssl": ssl,
        "pool_maxsize": pool_maxsize,
        "decode_responses": decode_responses,
        "socket_timeout": socket_timeout,
        "client_name": client_name,
    }


class RedisClient:
    """Thread-safe Redis client with connection pooling.

    This wrapper constructs a `redis.ConnectionPool` and a `redis.Redis` client.
    The underlying redis-py client is thread-safe; we additionally guard stateful operations
    like closing the pool with a re-entrant lock to avoid races.

    Typical usage:
        r = redis_client  # module-level singleton
        r.set("k", "v", ex=60)
        v = r.get("k")
        r.delete("k")

    Args:
        config: Optional dict with keys: host, port, db, password, ssl, pool_maxsize,
                decode_responses, socket_timeout, client_name. If None, uses env/config.
    """

    def __init__(self, config: Optional[dict] = None) -> None:
        self._lock = threading.RLock()
        self._config = config or _load_config_from_sources()
        self._pool: redis.ConnectionPool = self._create_pool(self._config)
        self._client: redis.Redis = self._create_client(self._pool, self._config)
        logger.info(
            "Redis client initialized: host=%s port=%s db=%s pool=%s ssl=%s",
            self._config["host"], self._config["port"], self._config["db"], self._config["pool_maxsize"], self._config["ssl"]
        )

    @staticmethod
    def _create_pool(cfg: dict) -> redis.ConnectionPool:
        return redis.ConnectionPool(
            host=cfg["host"],
            port=cfg["port"],
            db=cfg["db"],
            password=cfg["password"],
            max_connections=cfg["pool_maxsize"],
            socket_timeout=cfg["socket_timeout"],
            ssl=cfg["ssl"],
        )

    @staticmethod
    def _create_client(pool: redis.ConnectionPool, cfg: dict) -> redis.Redis:
        return redis.Redis(
            connection_pool=pool,
            decode_responses=cfg["decode_responses"],
            client_name=cfg.get("client_name"),
        )

    # ------------------ connection handling ------------------
    def ping(self) -> bool:
        """Ping Redis server to test connectivity."""
        try:
            return bool(self._client.ping())
        except RedisError as e:
            logger.error("Redis ping failed: %s", e)
            return False

    def close(self) -> None:
        """Close client connections and disconnect pool.

        Safe to call multiple times; guarded by lock to avoid concurrent pool disconnect.
        """
        with self._lock:
            try:
                try:
                    self._client.close()
                except Exception:
                    # older redis-py may not implement close(); ignore
                    pass
                self._pool.disconnect()
                logger.info("Redis client closed and pool disconnected")
            except Exception as e:  # pragma: no cover
                logger.warning("Redis close encountered error: %s", e)

    # ------------------ key-value operations ------------------
    def set(self, key: str, value: str, *, ex: Optional[int] = None, nx: bool = False, xx: bool = False) -> bool:
        """Set a key to a value.

        Args:
            key: Redis key
            value: String value (use json.dumps for structured data)
            ex: Expire time in seconds; if provided, sets TTL
            nx: Only set if key does not exist
            xx: Only set if key already exists
        Returns:
            True if set succeeded; False otherwise.
        """
        try:
            res = self._client.set(name=key, value=value, ex=ex, nx=nx, xx=xx)
            return bool(res)
        except RedisError as e:
            logger.error("Redis set failed key=%s: %s", key, e)
            return False

    def setex(self, key: str, seconds: int, value: str) -> bool:
        """Set key to hold the value and set key to expire after `seconds`."""
        try:
            res = self._client.setex(name=key, time=seconds, value=value)
            return bool(res)
        except RedisError as e:
            logger.error("Redis setex failed key=%s: %s", key, e)
            return False

    def get(self, key: str) -> Optional[str]:
        """Get value of key (returns None if not found or on error)."""
        try:
            val = self._client.get(name=key)
            return val if val is not None else None
        except RedisError as e:
            logger.error("Redis get failed key=%s: %s", key, e)
            return None

    def delete(self, *keys: str) -> int:
        """Delete one or more keys. Returns number of keys removed."""
        try:
            return int(self._client.delete(*keys))
        except RedisError as e:
            logger.error("Redis delete failed keys=%s: %s", keys, e)
            return 0

    # ------------------ expiration management ------------------
    def expire(self, key: str, seconds: int) -> bool:
        """Set a timeout on key. Returns True if timeout set."""
        try:
            return bool(self._client.expire(name=key, time=seconds))
        except RedisError as e:
            logger.error("Redis expire failed key=%s: %s", key, e)
            return False

    def ttl(self, key: str) -> int:
        """Get the time to live for key in seconds. Returns -2 if key does not exist, -1 if key exists but has no associated expire."""
        try:
            return int(self._client.ttl(name=key))
        except RedisError as e:
            logger.error("Redis ttl failed key=%s: %s", key, e)
            return -2

    def exists(self, key: str) -> bool:
        """Check if key exists."""
        try:
            return int(self._client.exists(key)) > 0
        except RedisError as e:
            logger.error("Redis exists failed key=%s: %s", key, e)
            return False

    # ------------------ helpers ------------------
    @property
    def client(self) -> redis.Redis:
        """Access underlying redis.Redis client (thread-safe)."""
        return self._client


# Module-level singleton, mimic db/mysql_pool style
try:
    redis_client = RedisClient()
except Exception as _e:  # pragma: no cover
    logger.warning("Redis client initialization failed: %s", _e)
    redis_client = None