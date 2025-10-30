import random
import time

from services import data_service, task_service

import logging
import logging
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def pid_handle(task_data_str:str, task_ret_str:str):
    """执行任务"""

    task_data = task_service.parse_task_data(task_data_str)
    pid = task_data.get('pid')
    date = task_data.get('date')
    app_ids:list = task_data.get('app_ids')
    task_ret:list[dict] = task_service.parse_task_ret(task_ret_str)

    if not app_ids:
        logger.warning(f"app_ids is empty for pid={pid} date={date}")
        return True, task_data_str, task_ret
    
    logger.info(f"开始任务 pid={pid} date={date}")
    
    new_app_ids = app_ids.copy()
    app_retry_count = task_data.get('app_retry_count', {})
    for app_id in app_ids:
        app_ret = {
            "app_id":app_id,
            "status":"start",
            "start_time":time.time(),
            "reason":"",
        }
        task_ret.append(app_ret)
        retry_count = app_retry_count.get(app_id, 0)
        if retry_count > 1:
            logger.info(f"{app_id} 已重试次数={retry_count}，跳过")
            app_ret["status"] = "fail"
            app_ret["reason"] =  f"{app_ret.get('reason', '')}|pid={pid} 已重试次数={retry_count}，跳过"
            app_ret["end_time"] = time.time()
            new_app_ids.remove(app_id)
            continue
        
        try:
            logger.info(f"开始获取 CSV 数据 pid={pid} app_id={app_id} date={date}")
            rows = data_service.fetch_csv_by_pid(pid=pid, app_id=app_id, date=date)
            logger.info(f"获取完成 pid={pid} app_id={app_id} rows={len(rows)}")
            t0 = time.perf_counter()
            logger.info(f"开始保存数据 pid={pid} app_id={app_id} rows={len(rows)}")
            data_service.save_data_bulk(pid=pid, date=date, rows=rows)
            elapsed = time.perf_counter() - t0
            logger.info(f"保存完成 pid={pid} app_id={app_id} 用时={elapsed:.2f}s rows={len(rows)}")
            time.sleep(random.uniform(1, 3))
            app_ret["status"] = "success"
            app_ret["reason"] = f"{app_ret.get('reason', '')}|成功 pid={pid} 用时={elapsed:.2f}s rows={len(rows)}"
            app_ret["end_time"] = time.time()
            new_app_ids.remove(app_id)
        except Exception as e:
            logger.error(f"task fail processing {app_id}: {str(e)}")
            app_retry_count[app_id] = retry_count + 1
            app_ret["status"] = "fail"
            app_ret["reason"] =  f"{app_ret.get('reason', '')}|pid={pid} 获取失败: {str(e)}"
            return False, task_service.create_csv_task_data(system_type=task_data.get('system_type'), pid=pid, date=date, app_ids=new_app_ids, app_retry_count=app_retry_count), task_service.create_task_ret(task_ret)

    return not new_app_ids, task_service.create_csv_task_data(system_type=task_data.get('system_type'), pid=pid, date=date, app_ids=new_app_ids, app_retry_count=app_retry_count), task_service.create_task_ret(task_ret)
