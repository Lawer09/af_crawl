from __future__ import annotations

"""同步所有启用用户的 App 列表"""

import logging
from concurrent.futures import ThreadPoolExecutor
import random
from datetime import date

from model.user import UserDAO
from services.app_service import fetch_and_save_apps
from config.settings import CRAWLER
from core.logger import setup_logging  # noqa: F401  # 触发日志初始化
from model.crawl_task import CrawlTaskDAO

logger = logging.getLogger(__name__)


def _worker(params):
    task_id, user = params
    try:
        apps = fetch_and_save_apps(user)
        logger.info("sync apps ok -> %s , count=%d", user["email"], len(apps))
        CrawlTaskDAO.mark_done(task_id)
    except Exception as e:
        logger.exception("sync apps fail -> %s : %s", user["email"], e)
        delay = 300 + random.randint(180, 360)
        CrawlTaskDAO.fail_task(task_id, delay)


def run():
    CrawlTaskDAO.init_table()
    
    if not CrawlTaskDAO.fetch_pending('user_apps', 1):
        users = UserDAO.get_enabled_users()
        init_tasks = [{
            'task_type': 'user_apps',
            'username': u['email'],
            'next_run_at': date.today().isoformat(),
        } for u in users]
        CrawlTaskDAO.add_tasks(init_tasks)

    pending = CrawlTaskDAO.fetch_pending('user_apps', limit=500)
    logger.info("pending user_apps=%d", len(pending))

    usernames = [t['username'] for t in pending]
    users_map = UserDAO.get_users_by_emails(usernames)

    tasks = [ (t['id'], users_map[t['username']]) for t in pending if t['username'] in users_map ]

    with ThreadPoolExecutor(max_workers=CRAWLER["threads_per_process"]) as pool:
        pool.map(_worker, tasks)
    logger.info("batch user_apps done")


if __name__ == "__main__":
    run() 