from __future__ import annotations

import logging
import time
import threading

from services.app_service import update_user_apps
from services.data_service import update_daily_data

logger = logging.getLogger(__name__)


def start_update_apps_scheduler(interval_minutes: int = 60):
    """启动后台定时任务，每 interval_minutes 分钟执行一次 update_apps。"""
    def _runner():
        while True:
            try:
                update_user_apps()
            except Exception:
                logger.exception("update_daily_apps scheduled run failed")
            time.sleep(interval_minutes * 60)

    t = threading.Thread(target=_runner, name="update_apps_scheduler", daemon=True)
    t.start()
    return t


def run_update_apps_cron(interval_minutes: int = 60):
    """以前台阻塞形式运行定时任务，适合命令行或服务容器启动。"""
    logger.info("=== sync_apps_cron start interval=%d min ===", interval_minutes)
    start_update_apps_scheduler(interval_minutes=interval_minutes)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("sync_apps_cron stopped by user")


def _seconds_until_next_midnight() -> int:
    """计算距离下一次0点的秒数。"""
    import datetime as _dt
    now = _dt.datetime.now()
    tomorrow = now.date() + _dt.timedelta(days=1)
    next_midnight = _dt.datetime.combine(tomorrow, _dt.time())
    return int((next_midnight - now).total_seconds())


def start_update_apps_midnight_scheduler():
    """启动后台定时任务，每天0点执行一次 update_user_apps。"""
    def _runner():
        while True:
            try:
                sleep_secs = _seconds_until_next_midnight()
                logger.info("update_apps_midnight_scheduler sleeping %d seconds until next midnight", sleep_secs)
                time.sleep(max(1, sleep_secs))
                update_user_apps()
            except Exception:
                logger.exception("update_user_apps midnight scheduled run failed")
                # 失败后稍等一分钟，避免快速重试
                time.sleep(60)

    t = threading.Thread(target=_runner, name="update_apps_midnight_scheduler", daemon=True)
    t.start()
    return t


def run_update_apps_midnight_cron():
    """以前台阻塞形式运行每天0点执行的应用列表更新任务。"""
    logger.info("=== sync_apps_midnight_cron start (daily at 00:00) ===")
    start_update_apps_midnight_scheduler()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("sync_apps_midnight_cron stopped by user")


def start_update_data_scheduler(interval_hours: int = 24):
    """启动后台定时任务。"""
    def _runner():
        try:
            update_daily_data()
        except Exception:
            logger.exception("update_daily_data scheduled run failed")
        time.sleep(interval_hours * 3600)

    t = threading.Thread(target=_runner, name="update_data_scheduler", daemon=True)
    t.start()
    return t

def run_update_data_cron(interval_hours: int = 24):
    """以前台阻塞形式运行数据定时任务。"""
    logger.info("=== sync_data_cron start interval=%d hours ===", interval_hours)
    start_update_data_scheduler(interval_hours=interval_hours)
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logger.info("sync_data_cron stopped by user")

def run_data():
    """运行一次"""
    logger.info("run_data start")
    try:
        update_daily_data()
    except Exception:
        logger.exception("update_daily_data scheduled run failed")