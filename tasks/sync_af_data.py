from __future__ import annotations
import json

from model.aff import AffDAO, OfferAffDAO

"""按用户分组的多线程数据同步 - 修复版"""

import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Tuple, Any, Optional

from model.user import UserProxyDAO
from model.task import TaskDAO
from model.offer import OfferDAO
from services.data_service import get_app_aff_map_from_offers, try_get_and_save_data
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
from config.settings import CRAWLER
from core.logger import setup_logging  # noqa


def create_task_data(type:str, pid:str, app_id:str, aff_id:str, date:str) -> Dict:
    import json
    return json.dumps({
        'type': type,
        'pid': pid,
        'app_id': app_id,
        'aff_id': aff_id,
        'date': date,
    })


def parse_task_data(task_data:str) -> Dict:
    import json
    return json.loads(task_data)


def create_task(date:str) -> None:
    """
    创建应用数据任务
    根据配置的静态代理 pid
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
        app_aff_map = {}

        for offer in offers:
            offer_id = int(offer.get("id"))
            app_id = offer.get("app_id")
            if not app_id:
                continue
            if app_id not in app_aff_map:
                app_aff_map[app_id] = []
            affs = offer_aff_map.get(offer_id, [])
            app_aff_map[app_id].extend([aff.get("aff_id") for aff in affs])

        for app_id, aff_ids in app_aff_map.items():
            aff_ids = list(set(aff_ids))
            for aff_id in aff_ids:
                aff = aff_map.get(aff_id)
                if not aff:
                    continue
                task = {
                    'task_data': create_task_data('app_data', pid, app_id, aff_id, date),
                    'next_run_at': now_date_time
                }
                task_list.append(task)
        logger.info(f"task for pid={pid} with {len(task_list)} tasks.")
    
    TaskDAO.add_tasks(task_list)


def handle(tasks:dict):
    """执行任务"""

    # 获取线程池配置
    max_workers = CRAWLER["threads_per_process"]
    logger.info("使用线程数: %d", max_workers)
    import queue

    # 用户任务处理函数
    def process_task(task_queue: queue.Queue):
        while not task_queue.empty():
            try:
                task = task_queue.get(timeout=1)
                if not task:
                    continue
                # 解析任务数据
                task_data = parse_task_data(task['task_data'])
                type = task_data.get('type')
                if type != 'app_data':
                    continue
                pid = task_data.get('pid')
                app_id = task_data.get('app_id')
                aff_id = task_data.get('aff_id')
                date = task_data.get('date')
                try_get_and_save_data(pid, app_id,date,date,aff_id)
                logger.info("任务: %s 处理完成", task['task_data'])
            except queue.Empty:
                break
            except Exception as e:
                logger.error("处理任务时出错: %s", str(e))
            finally:
                task_queue.task_done()

    # 创建任务队列
    task_queue = queue.Queue()
    for task in tasks:
        task_queue.put(task)

    # 使用线程池处理任务队列
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # 为每个线程分配任务处理器
        for _ in range(max_workers):
            pool.submit(process_task, task_queue)
        # 等待所有任务完成
        task_queue.join()

    logger.info("所有任务处理完成")

    
if __name__ == "__main__":
    run()