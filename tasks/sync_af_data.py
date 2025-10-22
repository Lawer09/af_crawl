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
from services.data_service import try_get_and_save_data
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

def parse_task_data(task_data: Any) -> Dict:
    import json
    try:
        if isinstance(task_data, dict):
            return task_data
        if isinstance(task_data, (bytes, bytearray)):
            task_data = task_data.decode('utf-8', errors='ignore')
        if isinstance(task_data, str):
            return json.loads(task_data)
    except Exception as e:
        logger.warning("Invalid task_data format: %s", e)
    return {}

def create_task(date:str) -> None:
    """
    创建应用数据任务
    根据配置的静态代理 pid 并根据 活跃的offer创建内部 djj渠道的af数据任务
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

    logger.info(f"create {len(task_list)} tasks.")
    TaskDAO.add_tasks(task_list)


def create_now_task():
    now_date = datetime.now().strftime("%Y-%m-%d")
    logger.info(f"create task for date={now_date}")
    create_task(now_date)


def handle(tasks:dict):
    """执行任务"""

    # 获取线程池配置
    max_workers = CRAWLER["threads_per_process"]
    logger.info("使用线程数: %d", max_workers)

    import queue

    # 用户任务处理函数
    def process_tasks(task_queue: queue.Queue):
        finished_tasks_total = []
        while True:
            try:
                group_tasks = task_queue.get(timeout=1)
                if not group_tasks:
                    task_queue.task_done()
                    continue
                pid = group_tasks[0].get("task_data")
                pid = parse_task_data(pid).get("pid")
                logger.info(f"process {len(group_tasks)} tasks for pid={pid}")
                finished_tasks = []
                for task in group_tasks:
                    # 解析任务数据
                    task_data = parse_task_data(task['task_data'])
                    type = task_data.get('type')
                    if type != 'app_data':
                        continue
                    pid = task_data.get('pid')
                    app_id = task_data.get('app_id')
                    aff_id = task_data.get('aff_id')
                    date = task_data.get('date')
                    ret = try_get_and_save_data(pid, app_id, date, date, aff_id)
                    if ret and len(ret) > 0:
                        logger.info(f"success save {len(ret)} rows for pid={pid} app_id={app_id} aff_id={aff_id} date={date}")
                        finished_tasks.append(task)
                finished_tasks_total.extend(finished_tasks)
                logger.info(f"finish process {len(group_tasks)} tasks for pid={pid}")
                task_queue.task_done()
            except queue.Empty:
                break
            except Exception as e:
                logger.error("处理任务时出错: %s", str(e))
        return finished_tasks_total

    # 创建任务队列
    task_queue = queue.Queue()

    # 将任务根据pid分组
    pid_task_map = {}
    for task in tasks:
        task_data = parse_task_data(task['task_data'])
        pid = task_data.get('pid')
        if pid not in pid_task_map:
            pid_task_map[pid] = []
        pid_task_map[pid].append(task)
    
    # 将pid任务放入队列
    for pid, tasks in pid_task_map.items():
        task_queue.put(tasks)
    
    return
    # 使用线程池处理任务队列
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        # 为每个线程分配任务处理器
        futures = [pool.submit(process_tasks, task_queue) for _ in range(max_workers)]
        # 等待所有任务完成
        task_queue.join()
        # 汇总成功任务，并从任务列表中移除（标记为完成）
        finished_tasks_all = []
        for f in futures:
            try:
                res = f.result()
                if res:
                    finished_tasks_all.extend(res)
            except Exception as e:
                logger.exception("任务线程执行异常: %s", e)

        if finished_tasks_all:
            # 批量标记完成，避免逐条更新
            task_ids = list({t.get('id') for t in finished_tasks_all if t.get('id')})
            try:
                affected = TaskDAO.mark_done_batch(task_ids)
                logger.info("本轮完成任务数: %d", affected)
            except Exception as e:
                logger.exception("批量标记任务完成失败: ids=%s, error=%s", task_ids, e)
            # 对未完成的任务增加重试次数（pending 状态）
            try:
                all_task_ids = list({t.get('id') for group in pid_task_map.values() for t in group if t.get('id')})
                undone_ids = [tid for tid in all_task_ids if tid not in set(task_ids)]
                if undone_ids:
                    affected_retry = TaskDAO.mark_retry_batch(undone_ids)
                    logger.info("本轮重试+1任务数: %d", affected_retry)
            except Exception as e:
                logger.exception("批量增加重试次数失败: error=%s", e)

    logger.info("所有任务处理完成")
