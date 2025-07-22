from __future__ import annotations

"""按日期区间同步全部用户全部 App 的统计数据"""

import logging
from datetime import date, timedelta
import random
from concurrent.futures import ThreadPoolExecutor

from model.user import UserDAO
from model.user_app import UserAppDAO
from model.crawl_task import CrawlTaskDAO
from services.data_service import fetch_and_save_table_data
from config.settings import CRAWLER
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def _daterange(days: int):
    today = date.today()
    for i in range(days):
        d = today - timedelta(days=i + 1)  # 昨天、前天...
        yield d.isoformat(), d.isoformat()


def _worker(param):
    task_id, user, app_id, start_date, end_date = param
    try:
        print(user, app_id, start_date, end_date)
        rows = fetch_and_save_table_data(user, app_id, start_date, end_date)
        logger.info("sync data ok -> %s %s [%s,%s] rows=%d", user["email"], app_id, start_date, end_date, len(rows))
        CrawlTaskDAO.mark_done(task_id)
    except Exception as e:
        logger.exception("sync data fail -> %s %s : %s", user["email"], app_id, e)
        # 延迟任务
        delay = 300 + random.randint(180, 360)
        CrawlTaskDAO.fail_task(task_id, delay)


def run(days: int = 1):
    CrawlTaskDAO.init_table()

    # 若无 pending 任务则初始化
    if not CrawlTaskDAO.fetch_pending('app_data', 1):
        users = UserDAO.get_enabled_users()
        init_tasks = []
        for user in users:
            apps = UserAppDAO.get_user_apps(user["email"])
            for app in apps:
                for start_date, end_date in _daterange(days):
                    init_tasks.append({
                        'task_type': 'app_data',
                        'username': user['email'],
                        'app_id': app['app_id'],
                        'start_date': start_date,
                        'end_date': end_date,
                        'next_run_at': date.today().isoformat(),
                    })
        CrawlTaskDAO.add_tasks(init_tasks)

    pending = CrawlTaskDAO.fetch_pending('app_data', limit=500)
    logger.info("pending tasks=%d", len(pending))

    # 将任务转为参数列表
    usernames = [t['username'] for t in pending]
    users_map = UserDAO.get_users_by_emails(usernames)

    tasks = [
        (t['id'], users_map[t['username']], t['app_id'], t['start_date'].isoformat(), t['end_date'].isoformat())
        for t in pending if t['username'] in users_map
    ]

    with ThreadPoolExecutor(max_workers=CRAWLER["threads_per_process"]) as pool:
        futures = [pool.submit(_worker, t) for t in tasks]
        for fut in futures:
            try:
                fut.result(timeout=600)
            except Exception as e:
                logger.error("task timeout or error: %s", e)
    logger.info("batch done")


if __name__ == "__main__":
    run() 