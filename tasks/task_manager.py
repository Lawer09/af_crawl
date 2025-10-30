# 本地任务处理，非分布式

import logging
import time
from config.settings import CRAWLER, SYSTEM_TYPE
from model.task import TaskDAO
from datetime import datetime, timedelta

from services import task_service
logger = logging.getLogger(__name__)
from model.user import UserProxyDAO
from services import fs_service
from tasks import sync_af_data
from services.task_service import create_pid_now_task

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
                        fs_service.send_sys_notify(f"批量清空 pending 任务: count={len(ids)}, affected={affected}")
                    except Exception as e:
                        logger.error(f"批量失败 pending 任务失败: {e}")
                        fs_service.send_sys_notify(f"批量清空 pending 任务失败")
                    pending_batch = TaskDAO.get_pending(limit=1000)
                logger.info("已将所有 pending 任务标记为 failed")
            except Exception as e:
                logger.error(f"标记 pending->failed 失败: {e}")
            last_midnight_date = current_date

        if now.hour == 1 and last_1am_date != current_date:
            try:
                create_pid_now_task()
                logger.info("create_pid_now_task 执行成功")
                fs_service.send_sys_notify("添加 AF APP DATA 任务")
            except Exception as e:
                logger.error(f"执行 create_pid_now_task 失败: {e}")
            last_1am_date = current_date

        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            try:
                if TaskDAO.should_create_new_tasks(interval_hours=CRAWLER["interval_hours"]):
                    try:
                        if SYSTEM_TYPE == "CHANGSHA":
                            # 增加郑州系统的af更新数据通知
                            user_proxies = task_service.get_tasks_pid(tasks, 1)
                            user_proxies.extend(task_service.get_tasks_pid(tasks, 2))
                            user_proxies.extend(task_service.get_tasks_pid(tasks, 3))
                            if user_proxies:
                                msg = f""
                                for proxy in user_proxies:
                                    msg += f"{proxy['system_type']} {proxy['pid']}\n"
                                fs_service.send_zhengzhou_notify(f"AF 数据更新完毕\n{msg}")

                        create_pid_now_task()
                        logger.info(f"没有待处理任务且距上次更新时间超过{CRAWLER['interval_hours']}小时，已创建新任务")
                        fs_service.send_sys_notify("添加 AF APP DATA 任务")
                    except Exception as e:
                        logger.error(f"自动创建任务失败: {e}")
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
                    task["next_run_at"] = (datetime.now() + timedelta(minutes=20)).strftime("%Y-%m-%d %H:%M:%S")
                    task["retry"] = task.get("retry", 0) + 1
                    TaskDAO.update_task(task)
                    logger.info(f"更新任务 {task['id']} 下次执行时间为 {task['next_run_at']}")
        
