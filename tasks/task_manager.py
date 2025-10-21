# 本地任务处理，非分布式

import logging
import time
from model.task import TaskDAO
from datetime import datetime
logger = logging.getLogger(__name__)
from tasks import sync_af_data

def run():
    
    while True:
        # 获取今日的任务
        tasks = TaskDAO.get_all_pending_task()
        if not tasks:
            logger.info("没有待处理任务")
            time.sleep(60)
            continue
        sync_af_data.handle(tasks)
