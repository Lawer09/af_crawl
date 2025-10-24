import random
import time

from services import data_service, task_service

import logging
import logging
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def pid_handle(task_data_str:str):
    """执行任务"""

    task_data = task_service.parse_task_data(task_data_str)
    pid = task_data.get('pid')
    date = task_data.get('date')
    app_ids:list = task_data.get('app_ids')
    
    if not app_ids:
        logger.warning(f"app_ids is empty for pid={pid} date={date}")
        return True, task_data_str
    
    logger.info(f"开始任务 pid={pid} date={date}")
    
    new_app_ids = app_ids.copy()
    app_retry_count = task_data.get('app_retry_count', {})
    for app_id in app_ids:
        retry_count = app_retry_count.get(app_id, 0)
        if retry_count > 1:
            logger.info(f"{app_id} 已重试次数={retry_count}，跳过")
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
            logger.info(f"数据已保存 pid={pid} app_id={app_id} rows={len(rows)}")
            new_app_ids.remove(app_id)
        except Exception as e:
            logger.error(f"task fail processing {app_id}: {str(e)}")
            app_retry_count[app_id] = retry_count + 1
            return False, task_service.create_csv_task_data(pid=pid, date=date, app_ids=new_app_ids, app_retry_count=app_retry_count)

    return not new_app_ids, task_service.create_csv_task_data(pid=pid, date=date, app_ids=new_app_ids, app_retry_count=app_retry_count)