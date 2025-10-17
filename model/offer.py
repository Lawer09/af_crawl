from __future__ import annotations

import logging
from typing import List, Dict

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class OfferDAO:
    """_tb_campaign 表操作封装。

    字段：pid、prt、live、id、package_name
    """

    TABLE = "_tb_campaign"

    @classmethod
    def get_list_by_pid(cls, pid: str) -> List[Dict]:
        """查询指定 pid 下 live=1 的记录列表"""
        sql = (
            f"SELECT id, pid, prt, live, package_name AS app_id FROM {cls.TABLE} "
            f"WHERE live = 1 AND pid = %s"
        )
        try:
            return mysql_pool.select(sql, (pid,))
        except Exception as e:
            logger.error("Error fetching offer list by pid=%s: %s", pid, e)
            return []

    @classmethod
    def get_list_by_pids(cls, pids: List[str]) -> List[Dict]:
        """批量查询多个 pid 下 live=1 的记录列表"""
        if not pids:
            return []
        placeholders = ",".join(["%s"] * len(pids))
        sql = (
            f"SELECT id, pid, prt, live, package_name AS app_id FROM {cls.TABLE} "
            f"WHERE live = 1 AND pid IN ({placeholders})"
        )
        try:
            return mysql_pool.select(sql, tuple(pids))
        except Exception as e:
            logger.error("Error fetching offer list by pids=%s: %s", pids, e)
            return []

    @classmethod
    def get_list_by_pids_group_pid(cls, pids: List[str]) -> Dict[str, List[Dict]]:
        """批量查询并按 pid 分组返回：{pid: [rows]}"""
        rows = cls.get_list_by_pids(pids)
        grouped: Dict[str, List[Dict]] = {}
        for r in rows:
            key = str(r.get("pid")) if r.get("pid") is not None else ""
            grouped.setdefault(key, []).append(r)
        return grouped