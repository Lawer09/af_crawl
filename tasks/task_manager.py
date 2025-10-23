# 本地任务处理，非分布式

import logging
import time
from model.task import TaskDAO
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from tasks import sync_af_data

def run():
    """运行任务"""
    logger.info("=== task_manager start ===")
    # 获取线程池配置
    # max_workers = CRAWLER["threads_per_process"]
    # logger.info("使用线程数: %d", max_workers)

    while True:
        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            time.sleep(60*5)
            continue

        for task in tasks:
            if task["task_type"] == "sync_af_data":
                try:
                    success, task_data = sync_af_data.pid_handle(task.get("task_data"))
                except Exception as e:
                    logger.error(f"sync_af_data.pid_handle fail: {str(e)}")
                    TaskDAO.fail_task(task["id"])
                    continue

                if success:
                    # 执行成功，更新任务状态
                    TaskDAO.mark_done(task["id"])
                else:
                    # 执行失败，推迟1小时执行
                    task["task_data"] = task_data
                    task["status"] = 'pending'
                    task["next_run_at"] = (datetime.now() + timedelta(minutes=30)).strftime("%Y-%m-%d %H:%M:%S")
                    task["retry"] += task.get("retry", 0) + 1
                    TaskDAO.update_task(task)
                    logger.info(f"更新任务 {task['id']} 下次执行时间为 {task['next_run_at']}")
        
