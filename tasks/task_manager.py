# 本地任务处理，非分布式

import logging
import time
from model.task import TaskDAO
from datetime import datetime, timedelta
logger = logging.getLogger(__name__)
from tasks import sync_af_data
from services.task_service import create_pid_now_task

def run():
    """运行任务"""
    logger.info("=== task_manager start ===")
    last_midnight_date = None
    last_1am_date = None
    # 获取线程池配置
    # max_workers = CRAWLER["threads_per_process"]
    # logger.info("使用线程数: %d", max_workers)

    while True:
        # 0点/1点定时任务
        now = datetime.now()
        current_date = now.date()
        if now.hour == 0 and last_midnight_date != current_date:
            try:
                # 将所有 pending 任务设置为 failed
                pending_batch = TaskDAO.get_pending(limit=1000)
                while pending_batch:
                    ids = [t.get("id") for t in pending_batch if t.get("id")]
                    try:
                        affected = TaskDAO.fail_task_batch(ids, 0)
                        logger.info(f"批量失败 pending 任务: count={len(ids)}, affected={affected}")
                    except Exception as e:
                        logger.error(f"批量失败 pending 任务失败: {e}")
                    pending_batch = TaskDAO.get_pending(limit=1000)
                logger.info("已将所有 pending 任务标记为 failed")
            except Exception as e:
                logger.error(f"标记 pending->failed 失败: {e}")
            last_midnight_date = current_date

        if now.hour == 1 and last_1am_date != current_date:
            try:
                create_pid_now_task()
                logger.info("create_pid_now_task 执行成功")
            except Exception as e:
                logger.error(f"执行 create_pid_now_task 失败: {e}")
            last_1am_date = current_date

        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            time.sleep(60*2)
            continue

        for task in tasks:
            if task["task_type"] == "sync_af_data":
                try:
                    success, task_data = sync_af_data.pid_handle(task.get("task_data"))
                except Exception as e:
                    logger.error(f"sync_af_data.pid_handle fail: {str(e)}")
                    TaskDAO.fail_task(task["id"], 0)
                    continue

                if success:
                    # 执行成功，更新任务状态
                    TaskDAO.mark_done(task["id"])
                else:
                    # 执行失败，推迟执行
                    task["task_data"] = task_data
                    task["status"] = 'pending'
                    task["next_run_at"] = (datetime.now() + timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
                    task["retry"] += task.get("retry", 0) + 1
                    TaskDAO.update_task(task)
                    logger.info(f"更新任务 {task['id']} 下次执行时间为 {task['next_run_at']}")
        
