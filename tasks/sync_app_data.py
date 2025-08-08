from __future__ import annotations

"""按用户分组的多线程数据同步 - 修复版"""

import logging
from datetime import date, timedelta, datetime
import time
import random
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Any, Optional

from model.user import UserDAO
from model.user_app import UserAppDAO
from model.crawl_task import CrawlTaskDAO
from services.data_service import fetch_and_save_table_data
from model.af_data import AfDataDAO
from core.db import mysql_pool
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)
from config.settings import CRAWLER
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def sync_app_data(
    username: str,
    app_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> Dict[str, Any]:
    """同步应用数据 - 用于分布式任务执行器
    
    Args:
        username: 用户邮箱/用户名
        app_id: 应用ID，如果为None则同步所有应用
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        
    Returns:
        Dict containing sync results
    """
    start_time = time.time()
    synced_records = 0
    errors = []
    
    try:
        logger.info(f"Starting data sync for user: {username}, app: {app_id}")
        
        # 获取用户信息
        user = UserDAO.get_user_by_email(username)
        if not user:
            raise ValueError(f"User not found: {username}")
        
        # 处理日期参数
        if start_date:
            try:
                start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError(f"Invalid start_date format: {start_date}. Expected YYYY-MM-DD")
        else:
            start_date_obj = date.today() - timedelta(days=1)
        
        if end_date:
            try:
                end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            except ValueError:
                raise ValueError(f"Invalid end_date format: {end_date}. Expected YYYY-MM-DD")
        else:
            end_date_obj = start_date_obj
        
        # 获取要同步的应用列表
        if app_id:
            # 同步指定应用
            user_app = UserAppDAO.get_user_app(username, app_id)
            if not user_app:
                raise ValueError(f"App {app_id} not found for user {username}")
            apps_to_sync = [user_app]
        else:
            # 同步所有应用
            apps_to_sync = UserAppDAO.get_user_apps(username)
        
        if not apps_to_sync:
            logger.warning(f"No apps found for user {username}")
            return {
                "status": "success",
                "username": username,
                "app_id": app_id,
                "start_date": start_date,
                "end_date": end_date,
                "synced_records": 0,
                "execution_time": time.time() - start_time,
                "message": "No apps to sync"
            }
        
        # 同步每个应用的数据
        for app in apps_to_sync:
            try:
                current_date = start_date_obj
                while current_date <= end_date_obj:
                    try:
                        # 调用数据同步服务
                        records = fetch_and_save_table_data(
                            user=user,
                            app=app,
                            start_date=current_date.isoformat(),
                            end_date=current_date.isoformat()
                        )
                        
                        synced_records += len(records) if records else 0
                        
                        logger.debug(
                            f"Synced {len(records) if records else 0} records for "
                            f"user {username}, app {app.get('app_id')}, date {current_date}"
                        )
                        
                    except Exception as e:
                        error_msg = (
                            f"Error syncing data for app {app.get('app_id')} "
                            f"on date {current_date}: {e}"
                        )
                        logger.error(error_msg)
                        errors.append(error_msg)
                    
                    # 移动到下一天
                    current_date += timedelta(days=1)
                    
            except Exception as e:
                error_msg = f"Error processing app {app.get('app_id')}: {e}"
                logger.error(error_msg)
                errors.append(error_msg)
        
        execution_time = time.time() - start_time
        
        logger.info(
            f"Data sync completed for user {username}: "
            f"{synced_records} records synced in {execution_time:.2f}s"
        )
        
        return {
            "status": "success",
            "username": username,
            "app_id": app_id,
            "start_date": start_date,
            "end_date": end_date,
            "synced_records": synced_records,
            "execution_time": execution_time,
            "errors": errors
        }
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Failed to sync data for user {username}: {e}"
        logger.exception(error_msg)
        
        return {
            "status": "error",
            "username": username,
            "app_id": app_id,
            "start_date": start_date,
            "end_date": end_date,
            "synced_records": synced_records,
            "execution_time": execution_time,
            "error_message": str(e),
            "error_type": type(e).__name__,
            "errors": errors
        }


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


def _migrate_af_user_app_data() -> None:
    try:
        # 获取前一天日期
        prev_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        logger.info(f"开始迁移 {prev_date} 的数据到af_data表")

        # 查询前一天的数据
        sql = """
        SELECT offer_id, start_date as date, af_clicks as clicks, af_installs as installs
        FROM af_user_app_data
        WHERE start_date = %s
        """
        results = mysql_pool.select(sql, (prev_date,))

        if not results:
            logger.info(f"未找到 {prev_date} 的数据，无需迁移")
            return

        af_data_list = []
        now_datetime = datetime.now()
        for item in results:
            af_data = {
                'offer_id': item['offer_id'],
                'date': item['date'],
                'clicks': item['clicks'],
                'installs': item['installs'],
                'timezone':'UTC+8',
                'pid':'',
                'prt':'',
                'aff_id':'',
                'aff_id': 0,  # 默认值
                'app_id': 0,   # 默认值
                'created_at': now_datetime
            }
            af_data_list.append(af_data)

        # 批量保存
        AfDataDAO.save_data_bulk(af_data_list)
        logger.info(f"成功迁移 {len(af_data_list)} 条数据到af_data表")

    except Exception as e:
        logger.exception("迁移数据到af_data表失败")
        raise

def run(days: int = 1):

    if AfDataDAO.exists_prev_day_data():
        logger.info("前一天数据已存在于af_data表，跳过同步任务")
        return

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

    _migrate_af_user_app_data()


if __name__ == "__main__":
    run()