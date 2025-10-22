from __future__ import annotations
import json

from model.aff import AffDAO, OfferAffDAO
from model.user_app_data import UserAppDataDAO
from services import data_service

"""按用户分组的多线程数据同步 - 修复版"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Any, Optional

from model.user import UserProxyDAO
from model.task import TaskDAO
from model.offer import OfferDAO
from services.data_service import try_get_and_save_data
from datetime import datetime
import logging
from core.logger import setup_logging  # noqa

logger = logging.getLogger(__name__)


def create_task_data(pid:str,date:str, app_affs_map:dict) -> str:
    import json
    data = {
        "pid": pid,
        "date": date,
        "app_affs_map": app_affs_map,
    }
    return json.dumps(data)

def parse_task_data(task_data: str) -> Dict:
    
    import json
    try:
        return json.loads(task_data)
    except Exception as e:
        logger.warning("Invalid task_data format: %s", e)
    return {}

def create_task(date:str) -> None:
    """
    创建应用数据任务
    根据配置的静态代理 pid 并根据 活跃的offer 的 djj渠道的af数据任务
    """
    TaskDAO.init_table()

    user_proxies = UserProxyDAO.get_enable()
    if not user_proxies:
        logger.error("No enable user proxy found for daily data update.")
        return

    pids = [p.get("pid") for p in user_proxies if p.get("pid")]
    logger.info(f"create task for {len(pids)} pids.")

    aff_map = AffDAO.get_ddj_map_aff_id()
    logger.info(f"aff_map init")
    
    # pid下对应的所有可用的offer数据
    pid_offer_map = OfferDAO.get_list_by_pids_group_pid(pids)
    logger.info(f"pid_offer_map init")

    offer_aff_map = OfferAffDAO.get_list_by_offer_ids_group([int(o.get("id")) for offers in pid_offer_map.values() for o in offers])
    logger.info(f"offer_aff_map init")
    
    now_date_time = datetime.now().isoformat()

    task_list = []

    for pid in pids:
        offers = pid_offer_map.get(pid)
        if not offers:
            logger.info(f"create task for pid={pid} with no offers.")
            continue
        app_affs_map = {}

        for offer in offers:
            offer_id = int(offer.get("id"))
            app_id = offer.get("app_id")
            if not app_id:
                continue
            if app_id not in app_affs_map:
                app_affs_map[app_id] = []
            affs = offer_aff_map.get(offer_id, [])
            app_affs_map[app_id].extend([aff.get("aff_id") for aff in affs if aff_map.get(aff.get("aff_id"))])
        
        max_retry_count = sum(len(affs) for affs in app_affs_map.values())
        
        task_list.append({
            'max_retry_count': max_retry_count,
            'task_type': 'sync_af_data',
            'task_data': create_task_data(pid, date, app_affs_map),
            'next_run_at': now_date_time
        })

    logger.info(f"create {len(task_list)} tasks.")
    TaskDAO.add_tasks(task_list)


def create_now_task():
    now_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"create task for date={now_date}")
    create_task(now_date)


def handle(task_data_str:str):
    """执行任务"""

    task_data = parse_task_data(task_data_str)
    pid = task_data.get('pid')
    date = task_data.get('date')
    app_affs_map:dict = task_data.get('app_affs_map')
    if not app_affs_map:
        logger.warning(f"app_affs_map is empty for pid={pid} date={date}")
        return False
    
    logger.info(f"开始任务 pid={pid} date={date}")
    
    # 数据库获取当前日期最近的更新数据
    recent_data = UserAppDataDAO.get_recent_by_pid(pid, date, 120)
    recent_key = [f"{pid}_{data.get('app_id')}_{data.get('aff_id')}" for data in recent_data]
    recent_key_set = set[str](recent_key)
    
    new_app_affs_map = app_affs_map.copy()
    for app_id, aff_ids in app_affs_map.items():
        for aff_id in aff_ids:
            key = f"{pid}_{app_id}_{aff_id}"
            if key in recent_key_set:
                recent_key_set.remove(key)
                logger.info(f"新数据已存在 pid={pid} app_id={app_id} aff_id={aff_id}")
                new_app_affs_map[app_id].remove(aff_id)
                if not new_app_affs_map[app_id]:
                    del new_app_affs_map[app_id]
                continue

            try:
                data_service.fetch_and_save_data(pid=pid, app_id=app_id, date=date, aff_id=aff_id)
                logger.info(f"新数据已保存 pid={pid} app_id={app_id} aff_id={aff_id}")
                new_app_affs_map[app_id].remove(aff_id)
                if not new_app_affs_map[app_id]:
                    del new_app_affs_map[app_id]
            except Exception as e:
                logger.error(f"task fail processing {key}: {str(e)}")
                continue

    return not new_app_affs_map, create_task_data(pid=pid, date=date, app_affs_map=new_app_affs_map)
