from typing import List, Dict
from core.db import report_mysql_pool

import logging
import time
import os
logger = logging.getLogger(__name__)
_SLOW_SEC = float(os.getenv("MYSQL_SLOW_QUERY_SECONDS", "5"))

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

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id BIGINT AUTO_INCREMENT PRIMARY KEY,
        offer_id BIGINT NOT NULL,
        aff_id VARCHAR(64) NOT NULL,
        clicks INT NOT NULL DEFAULT 0,
        installs INT NOT NULL DEFAULT 0,
        app_id VARCHAR(128) NOT NULL,
        pid VARCHAR(64) NOT NULL,
        `date` DATE NOT NULL,
        timezone VARCHAR(64) DEFAULT NULL,
        created_at DATETIME DEFAULT NULL,
        prt VARCHAR(64) DEFAULT NULL,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        UNIQUE KEY uniq_app_pid_offer_aff_date (app_id, pid, offer_id, aff_id, `date`),
        KEY idx_date (`date`),
        KEY idx_app_pid_date (app_id, pid, `date`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    @classmethod
    def init_table(cls) -> None:
        """初始化 af_data 表（若不存在则创建）"""
        try:
            report_mysql_pool.execute(cls.CREATE_SQL)
            logger.info("Table %s initialized.", cls.TABLE)
        except Exception as e:
            logger.exception("Init table %s failed: %s", cls.TABLE, e)
            raise

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

    @staticmethod
    def upsert_one_safe(item: Dict, lock_timeout: int = 5) -> bool:
        """在无法建立唯一索引时的安全 upsert：仅更新不删除；若不存在则插入。单事务减少往返。
        - 需要键：app_id, pid, offer_id, aff_id, date
        - 并发安全：对每个键使用建议锁，避免并发写入冲突
        - 语义：覆盖更新（更新 clicks/installs/updated_at），不存在则插入一条
        """
        required = {"app_id", "pid", "offer_id", "aff_id", "date"}
        if not required.issubset(item.keys()):
            logger.warning("upsert_one_safe skip invalid item: missing keys from %s", item)
            return False
        lock_key = f"af_data|{item['app_id']}|{item['pid']}|{item['offer_id']}|{item['aff_id']}|{item['date']}"
        conn = report_mysql_pool.get_conn()
        cursor = conn.cursor()
        try:
            try:
                cursor.execute("SELECT GET_LOCK(%s, %s)", (lock_key, lock_timeout))
            except Exception:
                logger.debug("GET_LOCK unsupported; proceed without advisory lock key=%s", lock_key)
            clicks = int(item.get("af_clicks", 0) or 0)
            installs = int(item.get("af_installs", 0) or 0)
            # 先尝试更新（仅更新，不删除）
            update_sql = (
                f"UPDATE {AfAppDataDAO.TABLE} SET clicks=%s, installs=%s, updated_at=NOW() "
                f"WHERE app_id=%s AND pid=%s AND offer_id=%s AND aff_id=%s AND `date`=%s"
            )
            cursor.execute(
                update_sql,
                (clicks, installs, item["app_id"], item["pid"], item["offer_id"], item["aff_id"], item["date"]),
            )
            if cursor.rowcount == 0:
                # 不存在则插入
                insert_sql = (
                    f"INSERT INTO {AfAppDataDAO.TABLE} (offer_id, aff_id, clicks, installs, app_id, pid, `date`, updated_at) "
                    f"VALUES (%s,%s,%s,%s,%s,%s,%s,NOW())"
                )
                cursor.execute(
                    insert_sql,
                    (
                        item["offer_id"], item["aff_id"], clicks, installs,
                        item["app_id"], item["pid"], item["date"],
                    ),
                )
            conn.commit()
            return True
        except Exception as e:
            conn.rollback()
            logger.exception("AfAppDataDAO.upsert_one_safe failed: err=%s item=%s", e, item)
            return False
        finally:
            try:
                cursor.execute("SELECT RELEASE_LOCK(%s)", (lock_key,))
            except Exception:
                pass
            cursor.close()
            conn.close()

    @staticmethod
    def upsert_bulk_safe(items: List[Dict], lock_timeout: int = 5) -> int:
        """批量安全 upsert：仅更新不删除；对不存在的键再插入。单事务批量 UPDATE + 条件 INSERT。
        - 不依赖唯一索引；保证并发下同键做覆盖更新
        - 构造衍生数据表（UNION ALL）进行 JOIN 更新与插入，减少往返
        """
        if not items:
            return 0
        required = {"offer_id", "aff_id", "date", "app_id", "pid"}
        # 本地去重：同键只保留最后一条（覆盖语义）
        dedup_map: Dict[tuple, Dict] = {}
        for it in items:
            if not required.issubset(it.keys()):
                logger.warning("upsert_bulk_safe skip invalid item: missing keys from %s", it)
                continue
            key = (it["offer_id"], it["aff_id"], it["date"])
            dedup_map[key] = it
        if not dedup_map:
            return 0

        values = list(dedup_map.values())
        # 构造衍生数据表（UNION ALL），包含所有键与目标值
        rows_sql_parts: List[str] = []
        params: List = []
        for it in values:
            rows_sql_parts.append(
                "SELECT %s AS app_id, %s AS pid, %s AS offer_id, %s AS aff_id, %s AS `date`, %s AS clicks, %s AS installs"
            )
            params.extend([
                it["app_id"], it["pid"], it["offer_id"], it["aff_id"], it["date"],
                int(it.get("af_clicks", 0) or 0), int(it.get("af_installs", 0) or 0),
            ])
        derived_sql = " UNION ALL ".join(rows_sql_parts)

        # 批量 UPDATE（仅更新不删除），将 af_data 与衍生表按复合键 JOIN
        update_sql = (
            f"UPDATE {AfAppDataDAO.TABLE} AS a "
            f"JOIN ({derived_sql}) AS v "
            f"ON a.offer_id=v.offer_id AND a.aff_id=v.aff_id AND a.`date`=v.`date` "
            f"SET a.clicks=v.clicks, a.installs=v.installs, a.app_id=v.app_id, a.pid=v.pid, a.updated_at=NOW()"
        )

        # 对不存在的键进行 INSERT（条件插入），避免删除
        insert_sql = (
            f"INSERT INTO {AfAppDataDAO.TABLE} (offer_id, aff_id, clicks, installs, app_id, pid, `date`, updated_at) "
            f"SELECT v.offer_id, v.aff_id, v.clicks, v.installs, v.app_id, v.pid, v.`date`, NOW() "
            f"FROM ({derived_sql}) AS v "
            f"LEFT JOIN {AfAppDataDAO.TABLE} AS a "
            f"ON a.offer_id=v.offer_id AND a.aff_id=v.aff_id AND a.`date`=v.`date` "
            f"WHERE a.offer_id IS NULL"
        )

        conn = report_mysql_pool.get_conn()
        cursor = conn.cursor()
        try:
            # 设置行锁等待阈值，避免长时间等待
            try:
                lock_wait = int(os.getenv("MYSQL_LOCK_WAIT_SECONDS", "12"))
            except Exception:
                lock_wait = 12
            try:
                cursor.execute(f"SET SESSION innodb_lock_wait_timeout = {lock_wait}")
            except Exception:
                logger.debug("SET innodb_lock_wait_timeout unsupported; skip setting")

            # 批次级建议锁，减少并发批次之间的冲突
            t_lock = time.perf_counter()
            try:
                cursor.execute("SELECT GET_LOCK(%s, %s)", ("af_data_bulk", lock_timeout))
                try:
                    ret = cursor.fetchone()
                    got = int(ret[0]) if ret and len(ret) > 0 else -1
                except Exception:
                    got = -1
                lock_elapsed = time.perf_counter() - t_lock
                logger.info("AfAppDataDAO.upsert_bulk_safe GET_LOCK elapsed=%.2fs result=%s", lock_elapsed, got)
                if lock_elapsed > _SLOW_SEC:
                    logger.warning("AfAppDataDAO.upsert_bulk_safe GET_LOCK slow: %.2fs", lock_elapsed)
            except Exception:
                logger.debug("GET_LOCK unsupported; proceed without bulk advisory lock")

            # 单事务：先 UPDATE，再条件 INSERT
            logger.info("AfAppDataDAO.upsert_bulk_safe UPDATE start rows=%d", len(values))
            t_upd = time.perf_counter()
            cursor.execute(update_sql, tuple(params))
            updated_count = cursor.rowcount
            upd_elapsed = time.perf_counter() - t_upd
            logger.info("AfAppDataDAO.upsert_bulk_safe UPDATE done in %.2fs updated=%d", upd_elapsed, int(updated_count or 0))
            if upd_elapsed > _SLOW_SEC:
                logger.warning("AfAppDataDAO.upsert_bulk_safe UPDATE slow: %.2fs updated=%d", upd_elapsed, int(updated_count or 0))

            logger.info("AfAppDataDAO.upsert_bulk_safe INSERT start rows=%d", len(values))
            t_ins = time.perf_counter()
            cursor.execute(insert_sql, tuple(params))
            inserted_count = cursor.rowcount
            ins_elapsed = time.perf_counter() - t_ins
            logger.info("AfAppDataDAO.upsert_bulk_safe INSERT done in %.2fs inserted=%d", ins_elapsed, int(inserted_count or 0))
            if ins_elapsed > _SLOW_SEC:
                logger.warning("AfAppDataDAO.upsert_bulk_safe INSERT slow: %.2fs inserted=%d", ins_elapsed, int(inserted_count or 0))

            logger.info("AfAppDataDAO.upsert_bulk_safe COMMIT start")
            t_commit = time.perf_counter()
            conn.commit()
            commit_elapsed = time.perf_counter() - t_commit
            logger.info("AfAppDataDAO.upsert_bulk_safe COMMIT done in %.2fs", commit_elapsed)
            if commit_elapsed > _SLOW_SEC:
                logger.warning("AfAppDataDAO.upsert_bulk_safe COMMIT slow: %.2fs", commit_elapsed)
            return int(updated_count or 0) + int(inserted_count or 0)
        except Exception as e:
                conn.rollback()
                logger.exception("AfAppDataDAO.upsert_bulk_safe failed: err=%s", e)
                return 0
        finally:
            try:
                cursor.execute("SELECT RELEASE_LOCK(%s)", ("af_data_bulk",))
            except Exception:
                pass
            cursor.close()
            conn.close()