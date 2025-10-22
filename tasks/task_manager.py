# 本地任务处理，非分布式

import logging
import time
from config.settings import CRAWLER
from model.task import TaskDAO
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from tasks import sync_af_data

def run():
    """运行任务"""
    logger.info("=== task_manager start ===")
    # 获取线程池配置
    max_workers = CRAWLER["threads_per_process"]
    logger.info("使用线程数: %d", max_workers)

    while True:
        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            time.sleep(60*5)
            continue

        for task in tasks:
            if task["task_type"] == "sync_af_data":
                success, task_data = sync_af_data.handle(task.get("task_data"))
                task["task_data"] = task_data
                if success:
                    # 执行成功，更新任务状态
                    TaskDAO.mark_done(task["id"])
                else:
                    # 执行失败，推迟1小时执行
                    task["next_run_at"] = (datetime.now() + timedelta(hours=1)).isoformat()
                    task["retry"] += 1
                    TaskDAO.update_task(task)
                    logger.info(f"更新任务 {task['id']} 下次执行时间为 {task['next_run_at']}")
        
