from __future__ import annotations

import logging
from typing import List, Dict, Optional

from core.db import mysql_pool

logger = logging.getLogger(__name__)


class AfHandshakeDAO:
    """_tb_af_handshake_relation 表操作封装（位于 adbink/report 数据库）。

    字段：
      - af_user_id: 本系统内 af_user.id
      - prt: 代理/机构标识
      - status: 0 未处理，1 成功，2 失败，3 失效

    说明：
      - 支持“一个用户(pid)绑定多个 prt”。
      - 如需唯一绑定，可使用 ensure_only_prt（不推荐在多绑定场景）。
    """

    TABLE = "_tb_af_handshake_relation"

    @classmethod
    def get_prts_by_user(cls, af_user_id: int) -> List[str]:
        sql = f"SELECT prt FROM {cls.TABLE} WHERE af_user_id = %s"
        rows = mysql_pool.select(sql, (af_user_id,))
        return [r["prt"] for r in rows] if rows else []

    @classmethod
    def ensure_only_prt(cls, af_user_id: int, prt: str, status: int = 1) -> Dict[str, int]:
        """确保指定用户仅与给定 prt 建立关系：缺少则增加，多余则删除。

        返回统计：{"added": n1, "deleted": n2, "updated": n3}
        """
        # 查询现有 PRT
        existing = cls.get_prts_by_user(af_user_id)
        to_delete = [x for x in existing if x != prt]

        deleted = 0
        if to_delete:
            placeholders = ",".join(["%s"] * len(to_delete))
            sql = (
                f"DELETE FROM {cls.TABLE} WHERE af_user_id = %s AND prt IN ({placeholders})"
            )
            deleted = mysql_pool.execute(sql, (af_user_id, *to_delete))

        # 先尝试更新（若已存在则更新状态为成功）
        updated = mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status = %s WHERE af_user_id = %s AND prt = %s",
            (status, af_user_id, prt),
        )

        added = 0
        if updated == 0:
            # 不存在则插入
            added = mysql_pool.execute(
                f"INSERT INTO {cls.TABLE} (af_user_id, prt, status) VALUES (%s, %s, %s)",
                (af_user_id, prt, status),
            )

        return {"added": added, "deleted": deleted, "updated": updated}

    @classmethod
    def upsert_prt(cls, af_user_id: int, prt: str, status: int = 1) -> Dict[str, int]:
        """确保指定 (af_user_id, prt) 记录存在：存在则更新状态，否则插入；不删除其他 prt。

        返回统计：{"added": n1, "updated": n2}
        """
        updated = mysql_pool.execute(
            f"UPDATE {cls.TABLE} SET status = %s WHERE af_user_id = %s AND prt = %s",
            (status, af_user_id, prt),
        )
        added = 0
        if updated == 0:
            added = mysql_pool.execute(
                f"INSERT INTO {cls.TABLE} (af_user_id, prt, status) VALUES (%s, %s, %s)",
                (af_user_id, prt, status),
            )
        return {"added": added, "updated": updated}

    @classmethod
    def sync_user_prts(cls, af_user_id: int, prts: List[str], status: int = 1) -> Dict[str, int]:
        """将指定用户的 prt 列表同步为给定集合：缺少则插入，冗余则删除，存在则更新状态。

        返回统计：{"added": n1, "deleted": n2, "updated": n3}
        """
        target_set = set([p for p in prts if p])
        existing_list = cls.get_prts_by_user(af_user_id)
        existing_set = set(existing_list)

        to_delete = list(existing_set - target_set)
        to_update = list(existing_set & target_set)
        to_add = list(target_set - existing_set)

        deleted = 0
        if to_delete:
            placeholders = ",".join(["%s"] * len(to_delete))
            sql = f"DELETE FROM {cls.TABLE} WHERE af_user_id = %s AND prt IN ({placeholders})"
            deleted = mysql_pool.execute(sql, (af_user_id, *to_delete))

        updated = 0
        if to_update:
            placeholders = ",".join(["%s"] * len(to_update))
            sql = f"UPDATE {cls.TABLE} SET status = %s WHERE af_user_id = %s AND prt IN ({placeholders})"
            updated = mysql_pool.execute(sql, (status, af_user_id, *to_update))

        added = 0
        for p in to_add:
            added += mysql_pool.execute(
                f"INSERT INTO {cls.TABLE} (af_user_id, prt, status) VALUES (%s, %s, %s)",
                (af_user_id, p, status),
            )

        return {"added": added, "deleted": deleted, "updated": updated}