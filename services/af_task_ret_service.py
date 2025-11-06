from model import task
from model.task import AfTaskRetDAO


def get_pending_map():
    """获取待处理任务, key为 pid app_id fetch_date 的键"""
    pending_map = {}
    for item in AfTaskRetDAO.get_by_status(status=task.PENDING, limit=1000):
        key = f"{item['pid']}_{item['app_id']}_{item['fetch_date']}"
        pending_map[key] = item
    return pending_map


def add_task_ret_list(task_ret_list:list[dict]):
    """添加任务返回列表"""
    AfTaskRetDAO.insert_many(task_ret_list)