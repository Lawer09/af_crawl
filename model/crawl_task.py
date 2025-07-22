from __future__ import annotations

import logging
from typing import List, Dict, Optional
from datetime import datetime

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class CrawlTaskDAO:
    """任务表: 记录爬虫执行进度，支持失败延迟、重启续跑"""

    TABLE = "af_crawl_tasks"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        task_type VARCHAR(32) NOT NULL,
        username VARCHAR(255) NOT NULL,
        app_id VARCHAR(128) DEFAULT NULL,
        start_date DATE DEFAULT NULL,
        end_date DATE DEFAULT NULL,
        retry INT DEFAULT 0,
        status ENUM('pending','running','failed','done') DEFAULT 'pending',
        next_run_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        KEY idx_status_next (status, next_run_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls):
        mysql_pool.execute(cls.CREATE_SQL)

    # ---------- CURD ----------
    @classmethod
    def add_tasks(cls, tasks: List[Dict]):
        if not tasks:
            return
        sql = f"""
        INSERT INTO {cls.TABLE}
            (task_type, username, app_id, start_date, end_date, next_run_at)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE status='pending'
        """
        params = [(
            t['task_type'], t['username'], t.get('app_id'), t.get('start_date'), t.get('end_date'), t.get('next_run_at')
        ) for t in tasks]
        mysql_pool.executemany(sql, params)

    @classmethod
    def fetch_pending(cls, task_type: str, limit: int = 100) -> List[Dict]:
        sql = f"""SELECT * FROM {cls.TABLE}
                 WHERE task_type=%s AND status='pending' AND next_run_at<=NOW()
                 ORDER BY next_run_at LIMIT %s"""
        return mysql_pool.select(sql, (task_type, limit))

    @classmethod
    def mark_running(cls, task_id: int):
        mysql_pool.execute(f"UPDATE {cls.TABLE} SET status='running' WHERE id=%s", (task_id,))

    @classmethod
    def mark_done(cls, task_id: int):
        mysql_pool.execute(f"UPDATE {cls.TABLE} SET status='done', updated_at=NOW() WHERE id=%s", (task_id,))

    @classmethod
    def fail_task(cls, task_id: int, retry_delay_sec: int):
        mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status='failed', retry=retry+1, next_run_at=NOW()+INTERVAL %s SECOND WHERE id=%s",
            (retry_delay_sec, task_id),
        )

    @classmethod
    def reset_all(cls):
        mysql_pool.execute(f"DELETE FROM {cls.TABLE}")

    @classmethod
    def reset_failed(cls):
        mysql_pool.execute(f"UPDATE {cls.TABLE} SET status='pending' WHERE status='failed'") 