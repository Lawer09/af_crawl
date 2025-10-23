from typing import Any


from model.offer import OfferDAO
import logging

from model.task import TaskDAO
from model.user import UserProxyDAO
from model.user_app import UserAppDAO
logger = logging.getLogger(__name__)

def create_csv_task_data(pid:str,date:str, app_ids:set, app_retry_count:dict|None = None) -> str:
    import json
    data = {
        "pid": pid,
        "date": date,
        "app_ids": list(app_ids),
    }
    if app_retry_count:
        data["app_retry_count"] = app_retry_count
    return json.dumps(data)


def parse_task_data(task_data: str) -> dict:
    
    import json
    try:
        return json.loads(task_data)
    except Exception as e:
        logger.warning("Invalid task_data format: %s", e)
    return {}


def add_pid_app_data_task(pid: str, date: str):
    """添加pid任务, date 爬取日期"""
    try:
        from datetime import datetime

        now_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        offers = OfferDAO.get_list_by_pid(pid)
        
        if not offers:
            logger.info(f"create task for pid={pid} with no offers.")
            
        # 获取当前pid下的app
        apps = UserAppDAO.get_list_by_pid(pid)

        if not apps:
            logger.info(f"{pid} : apps is empty")
            return

        app_id_set = set([str(app.get("app_id")) for app in apps])
        
        sys_app_id_set = set()
        for offer in offers:
            app_id = str(offer.get("app_id"))
            if not app_id or (app_id not in app_id_set):
                continue
            sys_app_id_set.add(app_id)

        if not sys_app_id_set:
            logger.info(f"{pid} : apps is not in af user apps")
            return

        task_data = create_csv_task_data(pid, date, sys_app_id_set)
        TaskDAO.add_task(task_type='sync_af_data',
            task_data=task_data,
            next_run_at=now_date_time,
            max_retry_count=len(sys_app_id_set)
        )
        logger.info(f"create task for pid={pid} success, task_data={task_data}")
    except Exception as e:
        logger.error(f"create task for pid={pid} fail: {str(e)}")
        raise


def create_pid_task(date:str) -> None:
    """
    创建应用数据任务, csv 数据
    根据配置的静态代理 pid 并根据 活跃的offer 的 af数据任务
    """
    TaskDAO.init_table()

    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error("No enable user proxy found for daily data update.")
        return

    pids = list({p.get("pid") for p in user_proxies if p.get("pid")})
    logger.info(f"create pid task for {len(pids)} pids.")

    for pid in pids:
        try:
            add_pid_app_data_task(
                pid=pid,
                date=date
            )
        except Exception as e:
            logger.error(f"add_pid_app_data_task fail: {str(e)}")


def create_pid_now_task():
    """创建应用数据任务, csv 数据, 昨天日期"""
    from datetime import datetime, timedelta

    yesterday_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"create task for date={yesterday_str}")
    create_pid_task(yesterday_str)
