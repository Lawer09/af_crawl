from __future__ import annotations

import logging
from typing import List, Dict, Union

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class AffDAO:
    """_tb_realation 表操作封装。

    字段：id、campaign、customer、live
    需求：按 offer_ids（对应 campaign）查询 live=1 的数据列表，并映射：
         campaign -> offer_id, customer -> aff_id
    """

    TABLE = "_tb_realation"

    @classmethod
    def get_list_by_offer_ids(cls, offer_ids: List[int]) -> List[Dict]:
        """批量按 offer_id（即 campaign）查询 live=1 的关系列表。

        返回字段：id, offer_id, aff_id, live, need_proxy
        """
        if not offer_ids:
            return []

        placeholders = ",".join(["%s"] * len(offer_ids))
        sql = (
            f"SELECT id, campaign AS offer_id, customer AS aff_id, live, need_proxy"
            f"FROM {cls.TABLE} WHERE live = 1 AND need_proxy = 1 AND campaign IN ({placeholders})"
        )
        try:
            return mysql_pool.select(sql, tuple(offer_ids))
        except Exception as e:
            logger.error("Error fetching aff relations by offer_ids=%s: %s", offer_ids, e)
            return []

    @classmethod
    def get_list_by_offer_ids_group(cls, offer_ids: List[int]) -> Dict[str, List[Dict]]:
        """批量查询并按 offer_id 分组返回：{offer_id: [rows]}"""
        rows = cls.get_list_by_offer_ids(offer_ids)
        grouped: Dict[str, List[Dict]] = {}
        for r in rows:
            key = str(r.get("offer_id")) if r.get("offer_id") is not None else ""
            grouped.setdefault(key, []).append(r)
        return grouped