from __future__ import annotations

"""同步所有启用用户的 App 列表"""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
import random
from datetime import date
from typing import Dict, Any

from model.user import AfUserDAO
from services.app_service import fetch_and_save_apps
from setting.settings import CRAWLER
from core.logger import setup_logging  # noqa: F401  # 触发日志初始化
from model.task import TaskDAO

logger = logging.getLogger(__name__)


def sync_user_apps(username: str) -> Dict[str, Any]:
    """同步单个用户的应用列表 - 用于分布式任务执行器
    
    Args:
        username: 用户邮箱/用户名
        
    Returns:
        Dict containing sync results
    """
    start_time = time.time()
    
    try:
        logger.info(f"Starting app sync for user: {username}")
        
        # 获取用户信息
        user = AfUserDAO.get_user_by_email(username)
        if not user:
            raise ValueError(f"User not found: {username}")
        
        # 同步应用
        apps = fetch_and_save_apps(user)
        
        execution_time = time.time() - start_time
        
        logger.info(f"App sync completed for user {username}: {len(apps)} apps synced in {execution_time:.2f}s")
        
        return {
            "status": "success",
            "username": username,
            "synced_apps": len(apps),
            "execution_time": execution_time
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Failed to sync apps for user {username}: {e}"
        logger.exception(error_msg)
        
        return {
            "status": "error",
            "username": username,
            "synced_apps": 0,
            "execution_time": execution_time,
            "error_message": str(e),
            "error_type": type(e).__name__
        }


def _worker(params):
    task_id, user = params
    try:
        apps = fetch_and_save_apps(user)
        logger.info("sync apps ok -> %s , count=%d", user["email"], len(apps))
        TaskDAO.mark_done(task_id)
    except Exception as e:
        logger.exception("sync apps fail -> %s : %s", user["email"], e)
        delay = 300 + random.randint(180, 360)
        TaskDAO.fail_task(task_id, delay)


def run():
    TaskDAO.init_table()
    
    if not TaskDAO.fetch_pending('user_apps', 1):
        users = AfUserDAO.get_enabled_users()
        init_tasks = [{
            'task_type': 'user_apps',
            'username': u['email'],
            'next_run_at': date.today().isoformat(),
        } for u in users]
        TaskDAO.add_tasks(init_tasks)

    pending = TaskDAO.fetch_pending('user_apps', limit=500)
    logger.info("pending user_apps=%d", len(pending))

    usernames = [t['username'] for t in pending]
    users_map = AfUserDAO.get_users_by_emails(usernames)

    tasks = [ (t['id'], users_map[t['username']]) for t in pending if t['username'] in users_map ]

    with ThreadPoolExecutor(max_workers=CRAWLER["threads_per_process"]) as pool:
        pool.map(_worker, tasks)
    logger.info("batch user_apps done")


if __name__ == "__main__":
    run()