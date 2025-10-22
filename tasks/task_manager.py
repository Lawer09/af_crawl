# 本地任务处理，非分布式

import logging
import time
from model.task import TaskDAO
from datetime import datetime
logger = logging.getLogger(__name__)
from tasks import sync_af_data

def run():
    """运行任务管理器"""
    logger.info("=== task_manager start ===")
    
    while True:
        # 获取任务
        tasks = TaskDAO.get_pending()
        if not tasks:
            logger.info("没有待处理任务")
            time.sleep(60)
            continue

        for task in tasks:
            if task["task_type"] == "sync_af_data":
                sync_af_data.handle([task])
        
