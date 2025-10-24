from __future__ import annotations

import logging
from typing import Any, Dict, List, Tuple, Optional

import mysql.connector
from mysql.connector.pooling import MySQLConnectionPool, PooledMySQLConnection

from config.settings import MYSQL, REPORT_MYSQL
import time
import os

logger = logging.getLogger(__name__)

_SLOW_SEC = float(os.getenv("MYSQL_SLOW_QUERY_SECONDS", "5"))
class MySQLPool:

    def __init__(self, config: dict):
        try:
            self.pool: MySQLConnectionPool = MySQLConnectionPool(
                pool_name=config["pool_name"],
                pool_size=config["pool_size"],
                **{k: v for k, v in config.items() if k not in {"pool_name", "pool_size"}}
            )
            self._initialized = True
            logger.info(
                "MySQL connection pool created: host=%s db=%s size=%s",
                config["host"], config["database"], config["pool_size"],
            )
        except mysql.connector.Error as e:
            logger.exception("[MySQL] create pool failed: %s", e)
            raise

    def get_conn(self) -> PooledMySQLConnection:
        return self.pool.get_connection()

    def select(self, sql: str, params: Tuple | Dict | None = None) -> List[Dict[str, Any]]:
        conn = self.get_conn()
        try:
            cursor = conn.cursor(dictionary=True)
            t0 = time.perf_counter()
            cursor.execute(sql, params or ())
            rows = cursor.fetchall()
            elapsed = time.perf_counter() - t0
            if elapsed > _SLOW_SEC:
                snippet = (sql[:300] + "...") if len(sql) > 300 else sql
                try:
                    pcount = len(params) if isinstance(params, (list, tuple, dict)) else (1 if params else 0)
                except Exception:
                    pcount = 0
                logger.warning("[MySQL] slow select: %.2fs params=%d sql=%s", elapsed, pcount, snippet)
            return rows
        finally:
            cursor.close()
            conn.close()

    def fetch_one(self, sql: str, params: Tuple | Dict | None = None) -> Optional[Dict[str, Any]]:
        rows = self.select(sql, params)
        return rows[0] if rows else None

    def execute(self, sql: str, params: Tuple | Dict | None = None) -> int:
        """单条写入 / 更新 / 删除"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            t0 = time.perf_counter()
            cursor.execute(sql, params or ())
            affected_rows = cursor.rowcount
            conn.commit()
            elapsed = time.perf_counter() - t0
            if elapsed > _SLOW_SEC:
                snippet = (sql[:300] + "...") if len(sql) > 300 else sql
                try:
                    pcount = len(params) if isinstance(params, (list, tuple, dict)) else (1 if params else 0)
                except Exception:
                    pcount = 0
                logger.warning("[MySQL] slow execute: %.2fs affected=%d params=%d sql=%s", elapsed, affected_rows, pcount, snippet)
            return affected_rows
        except Exception as e:
            conn.rollback()
            logger.exception("[MySQL] execute failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

    def executemany(self, sql: str, param_list: List[Tuple | Dict]):
        if not param_list:
            return
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            t0 = time.perf_counter()
            cursor.executemany(sql, param_list)
            conn.commit()
            elapsed = time.perf_counter() - t0
            snippet = (sql[:300] + "...") if len(sql) > 300 else sql
            try:
                pcount = len(param_list)
            except Exception:
                pcount = 0
            if elapsed > _SLOW_SEC:
                logger.warning("[MySQL] slow executemany: %.2fs batch=%d sql=%s", elapsed, pcount, snippet)
        except Exception as e:
            conn.rollback()
            logger.exception("[MySQL] executemany failed: %s", e)
            raise
        finally:
            cursor.close()
            conn.close()

# 单例
mysql_pool = MySQLPool(MYSQL)
report_mysql_pool = MySQLPool(REPORT_MYSQL)