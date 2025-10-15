from __future__ import annotations

from typing import Dict, Optional
import logging

from core.db import report_mysql_pool

logger = logging.getLogger(__name__)


class OverallReportCountDAO:
    TABLE = "overall_report_count"

    @classmethod
    def get_counts(cls, pid: str, offer_id: int, aff_id: Optional[int] = None) -> Dict[str, int]:
        """返回指定 pid/offer_id/aff_id 的 clicks 与 installation 汇总。
        当 aff_id 为 None 时，按 pid+offer_id 聚合所有渠道。
        """
        try:
            if aff_id is None:
                sql = (
                    f"SELECT COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(installation),0) AS installation "
                    f"FROM {cls.TABLE} WHERE pid=%s AND offer_id=%s"
                )
                params = (pid, offer_id)
            else:
                sql = (
                    f"SELECT COALESCE(SUM(clicks),0) AS clicks, COALESCE(SUM(installation),0) AS installation "
                    f"FROM {cls.TABLE} WHERE pid=%s AND offer_id=%s AND aff_id=%s"
                )
                params = (pid, offer_id, aff_id)

            result = report_mysql_pool.fetch_one(sql, params)
            if not result:
                return {"clicks": 0, "installation": 0}
            return {"clicks": int(result.get("clicks", 0) or 0), "installation": int(result.get("installation", 0) or 0)}
        except Exception as e:
            logger.exception(
                "Failed to fetch counts from %s for pid=%s offer_id=%s aff_id=%s: %s",
                cls.TABLE,
                pid,
                offer_id,
                aff_id,
                e,
            )
            return {"clicks": 0, "installation": 0}