from __future__ import annotations

import logging
from typing import List, Dict

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class UserAppDataDAO:
    TABLE = "af_user_app_data"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL,
        app_id VARCHAR(128) NOT NULL,
        offer_id VARCHAR(128) NOT NULL,
        af_clicks INT DEFAULT 0,
        af_installs INT DEFAULT 0,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        days INT DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY uk_offer (username, app_id, offer_id, start_date, end_date)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls):
        mysql_pool.execute(cls.CREATE_SQL)

    @classmethod
    def save_data_bulk(cls, datas: List[Dict]):
        if not datas:
            return
        cls.init_table()
        sql = f"""
        INSERT INTO {cls.TABLE}
            (username, app_id, offer_id, af_clicks, af_installs, start_date, end_date, days, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        ON DUPLICATE KEY UPDATE
            af_clicks=VALUES(af_clicks), af_installs=VALUES(af_installs), days=VALUES(days)
        """
        params = [
            (
                d["username"], d["app_id"], d["offer_id"], d["af_clicks"], d["af_installs"],
                d["start_date"], d["end_date"], d["days"],
            )
            for d in datas
        ]
        mysql_pool.executemany(sql, params)

    # -------- 活跃度 ---------
    @classmethod
    def get_recent_activity(cls, days: int = 7) -> Dict[tuple, int]:
        """返回 {(username, app_id): installs_sum} 最近 N 天安装量"""
        sql = f"""
        SELECT username, app_id, SUM(af_installs) AS s
        FROM {cls.TABLE}
        WHERE end_date >= CURDATE() - INTERVAL %s DAY
        GROUP BY username, app_id
        """
        rows = mysql_pool.select(sql, (days,))
        return {(r['username'], r['app_id']): r['s'] for r in rows}

    @classmethod
    def get_last_data_date(cls) -> Dict[tuple, str]:
        """返回 {(username, app_id): max_end_date_str} 用于判断长期无数据的应用"""
        sql = f"SELECT username, app_id, MAX(end_date) AS d FROM {cls.TABLE} GROUP BY username, app_id"
        rows = mysql_pool.select(sql)
        return {(r['username'], r['app_id']): str(r['d']) for r in rows if r['d']} 