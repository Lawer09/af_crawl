# 本地任务处理，非分布式

import logging
import time
from zoneinfo import ZoneInfo
from config.settings import CRAWLER, SYSTEM_TYPE
from model.task import TaskDAO
from datetime import datetime, timedelta

from services import task_service
logger = logging.getLogger(__name__)
from model.user import UserProxyDAO
from services import fs_service
from tasks import sync_af_data
from services.task_service import create_af_now_task

def run():
    """运行任务"""
    logger.info(f"=== task_manager start : {SYSTEM_TYPE}===")
    last_midnight_date = None
    last_1am_date = None
    # 获取线程池配置
    # max_workers = CRAWLER["threads_per_process"]
    # logger.info("使用线程数: %d", max_workers)

    while True:
        # 0点/1点定时任务
        utc8_now = datetime.now(ZoneInfo("Asia/Shanghai"))
        current_date = utc8_now.date()
        if utc8_now.hour == 2 and last_midnight_date != current_date:
            TaskDAO.zero_task()
            last_midnight_date = current_date

        if utc8_now.hour == 8 and last_1am_date != current_date:
            create_af_now_task()
            last_1am_date = current_date

        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            try:
                if TaskDAO.should_create_new_tasks(interval_hours=CRAWLER["interval_hours"]):
                    create_af_now_task()
                    logger.info(f"没有待处理任务且距上次更新时间超过{CRAWLER['interval_hours']}小时，已创建新任务")
                    fs_service.send_sys_notify("添加 AF APP DATA 任务")
            except Exception as e:
                logger.error(f"检查任务状态失败: {e}")
            time.sleep(60*5)
            continue

        for task in tasks:
            now = datetime.now()
            if now.hour == 0:
                break

            if task["task_type"] == "sync_af_data":
                try:
                    success, task_data, task_ret = sync_af_data.pid_handle(task.get("task_data"), task.get("task_ret","[]"))
                except Exception as e:
                    logger.error(f"sync_af_data.pid_handle fail: {str(e)}")
                    TaskDAO.fail_task(task["id"], 0)
                    continue
                task["task_data"] = task_data
                task["task_ret"] = task_ret
                if success:
                    # 执行成功，更新任务状态
                    task["status"] = 'done'
                    TaskDAO.update_task(task)
                else:
                    # 执行失败，推迟执行
                    task["status"] = 'pending'
                    task["next_run_at"] = (datetime.now() + timedelta(minutes=15)).strftime("%Y-%m-%d %H:%M:%S")
                    task["retry"] = task.get("retry", 0) + 1
                    TaskDAO.update_task(task)
                    logger.info(f"更新任务 {task['id']} 下次执行时间为 {task['next_run_at']}")
        
