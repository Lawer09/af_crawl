from typing import List, Dict
from core.db import report_mysql_pool

import logging
logger = logging.getLogger(__name__)

class AfDataDAO:

    @staticmethod
    def exists_prev_day_data() -> bool:
        from datetime import datetime, timedelta
        prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        sql = "SELECT COUNT(1) as count FROM af_data WHERE `date` = %s"
        try:
            result = report_mysql_pool.fetch_one(sql, (prev_date,))
            return result['count'] > 0 if result else False
        except Exception as e:
            logger.exception("检查前一天数据存在性失败")
            return False

    @staticmethod
    def save_data_bulk(data_list: List[Dict]) -> None:
        if not data_list:
            return

        sql = """
        INSERT INTO af_data (
            offer_id, aff_id, clicks, installs, app_id, timezone, 
            created_at, pid, prt, `date`
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        params = [
            (
                item['offer_id'], item['aff_id'], item['clicks'], item['installs'],
                item['app_id'], item['timezone'], item['created_at'], item['pid'], item['prt'], item['date']
            )
            for item in data_list
        ]

        report_mysql_pool.executemany(sql, params)


class AfAppDataDAO:
    """af_data 表应用数据 DAO。

    字段：offer_id、aff_id、id、clicks、installs、app_id、pid、date、updated_at
    需求：当 (app_id, pid, offer_id, aff_id, date) 相同时更新对应记录，否则插入新记录。
    """

    TABLE = "af_data"

    @staticmethod
    def upsert_bulk(items: List[Dict]) -> int:
        """使用 INSERT ... ON DUPLICATE KEY UPDATE 进行批量 upsert。
        需要唯一索引：(app_id, pid, offer_id, aff_id, date)
        更新策略：覆盖 clicks、installs，更新时间戳。
        返回成功执行的条数（若底层不返回受影响行数则返回尝试条数）。
        """
        if not items:
            return 0
        required = {"app_id", "pid", "offer_id", "aff_id", "date"}
        valid: List[Dict] = []
        for it in items:
            if not required.issubset(it.keys()):
                logger.warning("upsert_bulk skip invalid item: missing keys from %s", it)
                continue
            valid.append(it)
        if not valid:
            return 0
        sql = (
            f"INSERT INTO {AfAppDataDAO.TABLE} (offer_id, aff_id, clicks, installs, app_id, pid, `date`, updated_at) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s, NOW()) "
            f"ON DUPLICATE KEY UPDATE clicks=VALUES(clicks), installs=VALUES(installs), updated_at=NOW()"
        )
        params = [
            (
                it["offer_id"], it["aff_id"], int(it.get("af_clicks", 0) or 0), int(it.get("af_installs", 0) or 0),
                it["app_id"], it["pid"], it["date"],
            )
            for it in valid
        ]
        try:
            affected = report_mysql_pool.executemany(sql, params)
            return affected if isinstance(affected, int) else len(valid)
        except Exception as e:
            logger.exception("AfAppDataDAO.upsert_bulk_odku failed: err=%s", e)
            return 0