from __future__ import annotations

import logging
import time
import threading

from services.app_service import update_daily_apps
from services.data_service import update_daily_data

logger = logging.getLogger(__name__)


def start_update_apps_scheduler(interval_minutes: int = 60):
    """启动后台定时任务，每 interval_minutes 分钟执行一次 update_apps。"""
    def _runner():
        while True:
            try:
                update_daily_apps()
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