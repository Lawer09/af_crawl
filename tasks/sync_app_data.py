from __future__ import annotations

"""按日期区间同步全部用户全部 App 的统计数据"""

import logging
from datetime import date, timedelta
import time
import random
from concurrent.futures import ThreadPoolExecutor

from model.user import UserDAO
from model.user_app import UserAppDAO
from model.user_app_data import UserAppDataDAO
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

    # 限制单次处理任务数量
    max_batch_size = 50  # 减少批次大小
    
    # 若无 pending 任务则初始化
    if not CrawlTaskDAO.fetch_pending('app_data', 1):
        users = UserDAO.get_enabled_users()
        activity = UserAppDataDAO.get_recent_activity(7)
        last_dates = UserAppDataDAO.get_last_data_date()

        init_tasks = []
        for user in users:
            apps = UserAppDAO.get_user_apps(user["email"])
            # 计算活跃度，过滤 <=0
            def score(app):
                key = (user['email'], app['app_id'])
                act = activity.get(key, None)
                if act is None:
                    # 从未抓取过
                    return 10
                # 活跃度值
                return act

            apps_sorted = sorted(apps, key=score, reverse=True)
            for app in apps_sorted:
                key = (user['email'], app['app_id'])
                act = activity.get(key, None)

                if act is None:
                    pass  # 首次采集
                else:
                    if act <= 0:
                        # 不活跃，但若最后一次数据在3天前也跳过
                        last = last_dates.get(key)
                        if last and (date.today() - date.fromisoformat(last)).days < 3:
                            continue
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

    pending = CrawlTaskDAO.fetch_pending('app_data', limit=max_batch_size)
    logger.info("pending tasks=%d (limited to %d)", len(pending), max_batch_size)

    # 将任务转为参数列表
    usernames = [t['username'] for t in pending]
    users_map = UserDAO.get_users_by_emails(usernames)

    tasks = [
        (t['id'], users_map[t['username']], t['app_id'], t['start_date'].isoformat(), t['end_date'].isoformat())
        for t in pending if t['username'] in users_map
    ]

    # 使用更小的线程池和队列
    with ThreadPoolExecutor(max_workers=min(2, CRAWLER["threads_per_process"])) as pool:
        futures = []
        # 分批提交任务，避免同时启动太多
        batch_size = 2
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i+batch_size]
            batch_futures = [pool.submit(_worker, t) for t in batch]
            
            # 等待当前批次完成
            for fut in batch_futures:
                try:
                    fut.result(timeout=900)  # 增加到15分钟
                except Exception as e:
                    logger.error("task timeout or error: %s", e)
            
            # 批次间延迟
            if i + batch_size < len(tasks):
                time.sleep(random.randint(30, 60))
    
    logger.info("batch done")


if __name__ == "__main__":
    run()