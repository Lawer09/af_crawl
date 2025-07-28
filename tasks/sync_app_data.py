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
    total_tasks = CrawlTaskDAO.get_user_total_tasks(username, 'app_data')
    completed_tasks = CrawlTaskDAO.get_user_completed_tasks(username, 'app_data')
    logger.info("用户 %s 总任务数: %d, 已完成: %d", username, total_tasks, completed_tasks)
    
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
            time.sleep(random.randint(1, 3))


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
    """多线程用户任务分配 - 按线程数均衡分配用户任务"""
    CrawlTaskDAO.init_table()

    # 获取所有启用用户
    users = UserDAO.get_enabled_users()
    if not users:
        logger.info("没有启用的用户")
        return

    # 获取用户密码映射
    user_passwords = {user['email']: user['password'] for user in users}
    all_usernames = [user['email'] for user in users]
    logger.info("总启用用户数: %d", len(all_usernames))

    # 初始化任务（如果需要）
    if not CrawlTaskDAO.fetch_pending('app_data', 1):
        logger.info("初始化所有用户任务...")
        init_tasks = []

        apps = UserAppDAO.get_all_active()
        
        for app in apps:
            for start_date_str, end_date_str in _daterange(days):
                init_tasks.append({
                    'task_type': 'app_data',
                    'username': app['username'],
                    'app_id': app['app_id'],
                    'start_date': start_date_str,
                    'end_date': end_date_str,
                    'next_run_at': date.today().isoformat(),
                })
        
        if init_tasks:
            CrawlTaskDAO.add_tasks(init_tasks)
            logger.info("已初始化任务数: %d", len(init_tasks))

    # 获取线程池配置
    max_workers = CRAWLER["threads_per_process"]
    logger.info("使用线程数: %d", max_workers)

    # 用户任务处理函数
    def process_user_tasks(username_queue):
        while not username_queue.empty():
            try:
                username = username_queue.get(timeout=1)
                if not username:
                    continue
                # 获取该用户的待处理任务
                user_tasks = CrawlTaskDAO.fetch_user_pending_tasks(username, 'app_data', limit=50)
                if not user_tasks:
                    logger.info("用户 %s 没有待处理任务", username)
                    continue

                # 准备用户数据
                user_data = {
                    'username': username,
                    'password': user_passwords.get(username),
                    'tasks': user_tasks
                }

                if not user_data['password']:
                    logger.warning("用户 %s 密码不存在，跳过", username)
                    continue

                # 处理用户任务
                logger.info("线程开始处理用户: %s, 任务数: %d", username, len(user_tasks))
                _sync_user_apps(user_data)
                logger.info("线程完成处理用户: %s", username)

            except queue.Empty:
                break
            except Exception as e:
                logger.error("处理用户任务时出错: %s", str(e))
            finally:
                username_queue.task_done()

    while CrawlTaskDAO.fetch_pending('app_data', 1):
        # 创建用户队列
        import queue
        username_queue = queue.Queue()
        for username in all_usernames:
            username_queue.put(username)

        # 使用线程池处理用户队列
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            # 为每个线程分配任务处理器
            for _ in range(max_workers):
                pool.submit(process_user_tasks, username_queue)

            # 等待所有用户任务完成
            username_queue.join()

    logger.info("所有用户任务处理完成")


if __name__ == "__main__":
    run()