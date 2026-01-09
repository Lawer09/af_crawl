import threading
import time
import schedule

from model.task import TaskDAO
from schedulers.af_jobs import crawl_users_onelink_templates_job
from schedulers.crawl_token import simple_sync_af_aws_waf_token
from services.task_service import create_af_now_task

def async_run(func):
    """异步执行任务的包装器"""
    def wrapper(*args, **kwargs):
        # 启动子线程执行任务，主线程立即返回，不阻塞
        t = threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True)
        t.start()
    return wrapper

# 定义定时任务
# schedule.every(6).hours.do(async_run(crawl_users_onelink_templates_job))
# schedule.every().day.at("03:00").do(async_run(create_af_now_task))
# schedule.every().day.at("00:00").do(async_run(TaskDAO.zero_task))

schedule.every(5).minutes.do(async_run(simple_sync_af_aws_waf_token))

def start() -> None:
    """运行定时任务。"""
    while True:
        schedule.run_pending()
        time.sleep(1)  # 每1秒检查一次任务