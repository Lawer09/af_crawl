from typing import List, Dict
from core.db import report_mysql_pool

import logging
logger = logging.getLogger(__name__)

class AfDataDAO:

    @staticmethod
    def exists_prev_day_data() -> bool:
        from datetime import datetime, timedelta
        prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        sql = "SELECT COUNT(1) as count FROM af_data WHERE date = %s"
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
            created_at, pid, prt, date
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