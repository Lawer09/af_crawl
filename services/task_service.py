from model.offer import OfferDAO
import logging

from model.task import TaskDAO
from model.user import UserProxyDAO
from model.user_app import UserAppDAO
from services import fs_service, proxy_service
logger = logging.getLogger(__name__)

def create_csv_task_data(system_type: int | None, pid: str, date: str, app_ids: set, app_retry_count: dict | None = None) -> str:
    import json
    data = {
        "pid": pid,
        "date": date,
        "app_ids": list(app_ids),
    }
    # 将系统类型写入任务数据（可选，兼容旧调用）
    if system_type is not None:
        data["system_type"] = system_type
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

class OldAppDataErr(Exception):
    """旧版app数据"""
    pass

def add_pid_app_data_task(pid: str, date: str, system_type: int | None = None):
    """添加pid任务, date 爬取日期（可携带 system_type）"""
    try:
        from datetime import datetime

        now_date_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        offers = OfferDAO.get_list_by_pid(pid)
        
        if not offers:
            logger.info(f"create task for pid={pid} with no offers.")
            return
            
        # 获取当前pid下的app
        apps = UserAppDAO.get_list_by_pid(pid)

        if not apps:
            logger.info(f"{pid} : apps is empty")
            raise OldAppDataErr(f"pid={pid} 没有app数据")

        app_id_set = set([str(app.get("app_id")) for app in apps])
        
        sys_app_id_set = set()
        for offer in offers:
            app_id = str(offer.get("app_id"))
            if not app_id or (app_id not in app_id_set):
                continue
            sys_app_id_set.add(app_id)

        if not sys_app_id_set:
            logger.info(f"{pid} : apps is not in af user apps")
            raise OldAppDataErr(f"pid={pid} app没有在af app列表中的数据")

        task_data = create_csv_task_data(system_type=system_type, pid=pid, date=date, app_ids=sys_app_id_set)
        TaskDAO.add_task(task_type='sync_af_data',
            task_data=task_data,
            next_run_at=now_date_time,
            max_retry_count=len(sys_app_id_set)
        )
        logger.info(f"create task for pid={pid} success, task_data={task_data}")
    except Exception as e:
        logger.error(f"create task for pid={pid} fail: {str(e)}")
        raise


def build_proxy_fail_notify(rets:list[dict]) -> str:
    """
    构建代理失效通知消息
    """
    msg = "代理延成功率小与50%：\n"
    for r in rets:
        if r.get("success_rate") < 0.5:
            msg += f"{r.get('pid')} - {r.get('proxy_url')} (成功率: {r.get('success_rate')*100:.2f}%)\n"
    return msg


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

    # 检测一下pid的静态代理并通知失效（成功率小与50%）的静态代理
    rets = proxy_service.validate_user_proxies_stability(
        users=user_proxies,
        attempts=5,
        test_url="https://ipinfo.io",
        timeout=8,
    )

    msg = build_proxy_fail_notify(rets)
    if msg:
        fs_service.send_message(msg, webhook_url="https://open.feishu.cn/open-apis/bot/v2/hook/5adf3173-701b-4533-b5f8-2f1dbfaf2068")

    pids = list({r.get("pid") for r in rets if r.get("success_rate") >= 0.5})
    # 从 user_proxies 建立 pid -> system_type 映射
    pid_system_type_map = {u.get("pid"): u.get("system_type") for u in user_proxies if u.get("pid")}
    logger.info(f"create pid task for {len(pids)} pids.")

    update_app_data_pids = set()
    for pid in pids:
        try:
            add_pid_app_data_task(
                pid=pid,
                date=date,
                system_type=pid_system_type_map.get(pid)
            )
        except OldAppDataErr as e:
            # 需要更新App数据
            update_app_data_pids.add(pid)
        except Exception as e:
            logger.error(f"add_pid_app_data_task fail: {str(e)}")
    fs_service.send_sys_notify(f"需要更新App数据的pid如下\n{','.join(update_app_data_pids)}")

def create_pid_now_task():
    """创建应用数据任务, csv 数据, 昨天日期"""
    from datetime import datetime, timedelta

    yesterday_str = (datetime.now().date() - timedelta(days=1)).strftime("%Y-%m-%d")
    logger.info(f"create task for date={yesterday_str}")
    create_pid_task(yesterday_str)

def get_tasks_pid(tasks: list[dict], system_type: int) -> list[str]:
    """获取任务列表数据中的 pid"""
    pids = []
    for task in tasks:
        data = parse_task_data(task.get("task_data", ""))
        if data.get("system_type") == system_type:
            pids.append(data.get("pid", ""))
    return pids
