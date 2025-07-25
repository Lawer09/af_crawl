from __future__ import annotations

"""按用户分组的多线程数据同步 - 修复版"""

import logging
from datetime import date, timedelta, datetime
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple

from model.user import UserDAO
from model.user_app import UserAppDAO
from model.user_app_data import UserAppDataDAO
from model.crawl_task import CrawlTaskDAO
from services.data_service import fetch_and_save_table_data
from config.settings import CRAWLER
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def _daterange(days: int):
    """生成日期范围，返回字符串格式"""
    today = date.today()
    for i in range(days):
        d = today - timedelta(days=i + 1)
        yield d.isoformat(), d.isoformat()


def _sync_user_apps(user_data: Dict) -> None:
    """
    单个线程处理一个用户的所有APP数据
    
    Args:
        user_data: 包含用户信息和任务列表的字典
    """
    username = user_data['username']
    password = user_data['password']
    tasks = user_data['tasks']
    
    logger.info("开始处理用户: %s, 任务数: %d", username, len(tasks))
    
    for task in tasks:
        task_id = task['id']
        app_id = task['app_id']
        start_date = task['start_date']
        end_date = task['end_date']
        
        try:
            # 确保日期参数是字符串格式
            if isinstance(start_date, date):
                start_date = start_date.isoformat()
            if isinstance(end_date, date):
                end_date = end_date.isoformat()
                
            rows = fetch_and_save_table_data(
                {'email': username, 'password': password}, 
                app_id, start_date, end_date
            )
            logger.info("用户 %s 同步成功 -> %s [%s,%s] rows=%d", 
                       username, app_id, start_date, end_date, len(rows))
            CrawlTaskDAO.mark_done(task_id)
            
        except Exception as e:
            logger.exception("用户 %s 同步失败 -> %s : %s", username, app_id, e)
            # 延迟任务
            delay = 300 + random.randint(180, 360)
            CrawlTaskDAO.fail_task(task_id, delay)
        
        # 同一用户的任务间延迟
        if len(tasks) > 1:
            time.sleep(random.randint(5, 15))


def _group_tasks_by_user(pending_tasks: List[Dict]) -> List[Dict]:
    """将任务按用户分组"""
    user_groups = {}
    
    for task in pending_tasks:
        username = task['username']
        if username not in user_groups:
            user_groups[username] = {
                'username': username,
                'tasks': []
            }
        user_groups[username]['tasks'].append(task)
    
    return list(user_groups.values())


def run(days: int = 1):
    """按用户分组的多线程数据同步 - 先获取所有用户再分配任务"""
    CrawlTaskDAO.init_table()

    # 获取所有启用用户
    users = UserDAO.get_enabled_users()
    if not users:
        logger.info("没有启用的用户")
        return

    # 获取用户密码映射
    user_passwords = {}
    for user in users:
        user_passwords[user['email']] = user['password']

    # 获取待处理任务或初始化新任务
    pending = CrawlTaskDAO.fetch_pending('app_data', limit=1000)
    
    if not pending:
        # 初始化所有用户的任务
        logger.info("初始化所有用户的任务，用户数: %d", len(users))

        init_tasks = []
        for user in users:
            username = user['email']
            apps = UserAppDAO.get_user_apps(username)
            
            for app in apps:

                for start_date_str, end_date_str in _daterange(days):
                    init_tasks.append({
                        'task_type': 'app_data',
                        'username': username,
                        'app_id': app['app_id'],
                        'start_date': start_date_str,
                        'end_date': end_date_str,
                        'next_run_at': date.today().isoformat(),
                    })
        
        if init_tasks:
            CrawlTaskDAO.add_tasks(init_tasks)
            pending = CrawlTaskDAO.fetch_pending('app_data', limit=1000)

    logger.info("待处理任务数: %d", len(pending))
    if not pending:
        logger.info("没有待处理的任务")
        return

    # 按用户分组任务
    user_groups = _group_tasks_by_user(pending)
    logger.info("待处理用户数: %d", len(user_groups))

    # 补充密码信息并过滤无效用户
    valid_user_groups = []
    for group in user_groups:
        username = group['username']
        if username in user_passwords:
            group['password'] = user_passwords[username]
            valid_user_groups.append(group)
        else:
            logger.warning("用户不存在: %s", username)

    if not valid_user_groups:
        logger.warning("没有有效的用户任务")
        return

    # 使用线程池，每个线程处理一个用户的所有任务
    max_workers = min(len(valid_user_groups), CRAWLER["threads_per_process"])
    logger.info("使用 %d 个线程处理 %d 个用户", max_workers, len(valid_user_groups))

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = [pool.submit(_sync_user_apps, user_data) for user_data in valid_user_groups]
        
        # 等待所有任务完成
        for future in futures:
            try:
                future.result(timeout=3600)
            except Exception as e:
                logger.error("用户处理失败: %s", e)
    
    logger.info("批次完成")


if __name__ == "__main__":
    run()