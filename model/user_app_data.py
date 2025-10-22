from __future__ import annotations

import logging
from typing import List, Dict, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class UserAppDataDAO:
    TABLE = "af_user_app_data"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NULL,
        pid VARCHAR(50) NOT NULL,
        app_id VARCHAR(128) NOT NULL,
        offer_id VARCHAR(128) NOT NULL,
        aff_id VARCHAR(128) NULL,
        af_clicks INT DEFAULT 0,
        af_installs INT DEFAULT 0,
        start_date DATE NOT NULL,
        end_date DATE NOT NULL,
        days INT DEFAULT 1,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
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
            (username, pid, app_id, offer_id, aff_id, af_clicks, af_installs, start_date, end_date, days, created_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """
        params = [
            (
                d["username"], d.get("pid"), d["app_id"], d["offer_id"], d.get("aff_id"), d["af_clicks"], d["af_installs"], 
                d["start_date"], d["end_date"], d["days"],
            )
            for d in datas
        ]
        mysql_pool.executemany(sql, params)

    @classmethod
    def get_recent_by_pid(cls, pid: str, date: str, within_minutes: int = 60) -> List[Dict]: 
        """查询在最近 within_minutes 分钟内生成的缓存数据。
        精确匹配 pid、date。
        """
        cls.init_table()
        sql = f"""
        SELECT username, pid, app_id, offer_id, aff_id, af_clicks, af_installs, start_date, end_date, days, created_at
        FROM {cls.TABLE}
        WHERE pid = %s AND start_date = %s AND end_date = %s
          AND created_at >= NOW() - INTERVAL %s MINUTE
        """
        rows = mysql_pool.select(sql, (pid, date, date, within_minutes))
        return rows or []

    @classmethod
    def get_recent_rows(cls, pid: str, app_id: str, start_date: str, end_date: str, aff_id: str | None = None, within_minutes: int = 60) -> List[Dict]: 
        """查询在最近 within_minutes 分钟内生成的缓存数据。
        精确匹配 pid、app_id、start_date、end_date。
        """
        cls.init_table()
        sql = f"""
        SELECT username, pid, app_id, offer_id, aff_id, af_clicks, af_installs, start_date, end_date, days, created_at
        FROM {cls.TABLE}
        WHERE pid = %s AND app_id = %s AND start_date = %s AND end_date = %s
          AND aff_id = %s
          AND created_at >= NOW() - INTERVAL %s MINUTE
        """
        rows = mysql_pool.select(sql, (pid, app_id, start_date, end_date, aff_id, within_minutes))
        return rows or []

    @classmethod
    def get_rows_by_date(cls, pid: str, app_id: str, start_date: str, end_date: str, aff_id: Optional[str] = None) -> List[Dict]:
        """返回指定日期范围（通常为同一天）的缓存数据，按创建时间降序。
        - 当 aff_id 提供时进行过滤；未提供则不限制渠道。
        - 不进行 "最近N分钟" 限制，适用于前天仅取缓存的场景。
        """
        cls.init_table()
        base_sql = [
            f"SELECT username, pid, app_id, offer_id, aff_id, af_clicks, af_installs, start_date, end_date, days, created_at",
            f"FROM {cls.TABLE}",
            "WHERE pid = %s AND app_id = %s AND start_date = %s AND end_date = %s",
        ]
        params: List = [pid, app_id, start_date, end_date]
        if aff_id is not None:
            base_sql.append("AND aff_id = %s")
            params.append(aff_id)
        base_sql.append("ORDER BY created_at DESC")
        sql = "\n".join(base_sql)
        rows = mysql_pool.select(sql, tuple(params))
        return rows or []

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