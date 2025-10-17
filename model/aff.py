from __future__ import annotations

import logging
from typing import List, Dict

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class AffDAO:
    """_tb_relation 表操作封装。

    字段：id、campaign、customer、live
    需求：按 offer_ids（对应 campaign）查询 live=1 的数据列表，并映射：
         campaign -> offer_id, customer -> aff_id
    """

    TABLE = "_tb_relation"

    @classmethod
    def get_list_by_offer_ids(cls, offer_ids: List[int]) -> List[Dict]:
        """批量按 offer_id（即 campaign）查询 live=1 的关系列表。

        返回字段：id, offer_id, aff_id, live
        """
        if not offer_ids:
            return []

        placeholders = ",".join(["%s"] * len(offer_ids))
        sql = (
            f"SELECT id, campaign AS offer_id, customer AS aff_id, live "
            f"FROM {cls.TABLE} WHERE live = 1 AND campaign IN ({placeholders})"
        )
        try:
            return mysql_pool.select(sql, tuple(offer_ids))
        except Exception as e:
            logger.error("Error fetching aff relations by offer_ids=%s: %s", offer_ids, e)
            return []

    @classmethod
    def get_list_by_offer_ids_group_offer_id(cls, offer_ids: List[int]) -> Dict[int, List[Dict]]:
        """按 offer_id 分组返回关系列表（支持大列表分批查询）。"""
        grouped: Dict[int, List[Dict]] = {}
        if not offer_ids:
            return grouped
        CHUNK = 500
        for i in range(0, len(offer_ids), CHUNK):
            chunk = offer_ids[i:i+CHUNK]
            rows = cls.get_list_by_offer_ids(chunk)
            for r in rows:
                key = r.get("offer_id")
                if key is None:
                    continue
                if isinstance(key, str) and key.isdigit():
                    key_int = int(key)
                else:
                    key_int = key
                grouped.setdefault(key_int, []).append(r)
        return grouped

    @classmethod
    def get_list_by_offer_ids_group(cls, offer_ids: List[int]) -> Dict[int, List[Dict]]:
        """别名：按 offer_id 分组（与调用处保持兼容）"""
        return cls.get_list_by_offer_ids_group_offer_id(offer_ids)