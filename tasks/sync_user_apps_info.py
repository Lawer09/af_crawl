import stores_m as stores
import json
import setting
import log
import exception as ex
from crawl import af_crawl as ac
from utils import timeGen
from config import click_gap_task_config as cfg
import time

TASK_TYPE = "sync_user_apps"

log.info(f"当前用户App同步模式：是否为追加[{cfg.APPEND_MODEL}]")

def sync_user_apps(af_user, append_model=cfg.APPEND_MODEL):
    """同步用户应用列表到 SQLite 数据库"""
    # stores.user_apps.delete_user_apps(af_user["email"])
    try:
        user_apps = ac.get_home_apps(af_user)

        if not user_apps:
            return False, "获取应用列表失败"
        
        log.info(f"app数量: {len(user_apps)}")
        _is_pid_user = ac.is_pid_user(af_user)
        username = af_user["email"]
        user_apps_list = []

        for app in user_apps:
            app_id = app["app_id"]
            log.info(f"app: {app_id}")
            if append_model:
                u_app = stores.user_apps.get_user_app(username, app_id)
                if u_app and "app_id" in u_app:
                    continue

            if _is_pid_user:
                app_info = ac.get_app_info(af_user, app_id)
                if not app_info and "pid" not in app_info:
                    log.error(f"{username} 获取应用 {app_id} 信息失败")
                    continue
                app.update(app_info)
                user_type_id = app_info["pid"]
                
            else:
                user_type_id = app["prt"]

            user_apps_list.append({
                "username": username,
                "app_id": app_id,
                "timezone": app["timezone"],
                "user_type_id": str(user_type_id)
            })

        stores.user_apps.save_apps(user_apps_list)
        log.info(f"{app_id} 完成")
        return True, "用户apps同步完毕"
    
    except ex.IPLimitError as e:
        if not setting.USE_PROXY:
            raise e
        return False, str(e)

def _update_tasks(fail_tasks, delay_tasks, done_tasks):
    stores.user_apps_tasks.delay_tasks(delay_tasks)
    stores.user_apps_tasks.mark_done_tasks(done_tasks)
    stores.user_apps_tasks.fail_tasks(fail_tasks)


def upload_user_apps():
    UPDATE_COUNT = 1000
    af_user_apps = stores.AdbLinkUserAppsStore()
    # 每次保存2000条数据
    apps = stores.user_apps.get_all_upload_apps()
    seg_datas = [apps[i:i + UPDATE_COUNT] for i in range(0, len(apps), UPDATE_COUNT)]
    for seg_data in seg_datas:
        af_user_apps.save_apps(seg_data)
        print("已保存",len(seg_data))


def sync_task(af_user):
    
    username = af_user["email"]
    _is_pid_user = ac.is_pid_user(af_user)
    print(f"开始同步{af_user['email']}app信息")
    cur = time.time()
    while tasks := stores.user_apps_tasks.get_pending_tasks(username, 20):
        if len(tasks) < 1:
            break
        delay_tasks = []
        fail_tasks = []
        done_tasks = []
        curTaskId = None
        try:
            while task:= tasks.pop():
                curTaskId = task["id"]
                user_app_list = []
                # 如果有app_id则不需要get_home_apps
                if "app_id" not in task or not task["app_id"] or task["app_id"] == "":
                    user_app_list = ac.get_home_apps(af_user)
                    stores.user_apps.save_apps_s(user_app_list)
                    if _is_pid_user:
                        new_tasks = []
                        start_at = timeGen.after_now(minutes=15)
                        for a in user_app_list:
                            new_tasks.append({
                                "username": username,
                                "app_id": a["app_id"],
                                "start_at": start_at,
                            })
                        stores.user_apps_tasks.add_tasks_a(new_tasks)
                    done_tasks.append(curTaskId)
                    continue
                app_id = task["app_id"]
                info = ac.get_app_info(af_user, app_id)
                user_app_info = {
                    "app_id": app_id,
                    "username": username,
                    "user_type_id": info["pid"],
                    "timezone": info["timezone"],
                }
                user_app_list.append(user_app_info)
                stores.user_apps.save_apps(user_app_list)  

                done_tasks.append(curTaskId)

                if len(task) < 1:
                    break
        except ex.CrawlErr as e:
            if curTaskId:
                fail_tasks.append({"id": curTaskId, "reason": str(e)})
            for fail_task in tasks:
                fail_tasks.append({"id": fail_task["id"], "reason": str(e)})
            return False, e
        except ex.CrawlRetryErr as e:
            if curTaskId:
                delay_tasks.append({"id": curTaskId, "delay_seconds": e.value, "reason": str(e)})
            for delay_task in tasks:
                delay_tasks.append({"id": delay_task["id"], "delay_seconds": e.value, "reason": str(e)})
            return True, e
        except Exception as e:
            if curTaskId:
                fail_tasks.append({"id": curTaskId, "reason": str(e)})
            for fail_task in tasks:
                fail_tasks.append({"id": fail_task["id"], "reason": str(e)})
            return False, e
        finally:
            _update_tasks(fail_tasks, delay_tasks, done_tasks)
            print(f"同步完成{af_user['email']}，耗时{time.time() - cur}")
    return True, username


def reset():
    stores.user_apps_tasks.clear()
    date = timeGen.after_now()
    for af_user in stores.users.get_enable_af_users():
        stores.user_apps_tasks.add_task(af_user["email"], date)
    print("用户apps同步任务已重置")


def reset_fail_task():
    """重置失败任务"""
    stores.user_apps_tasks.reset_fail_task()
    print("用户apps重置失败任务")


def reset_non_pid_user_task():
    stores.user_apps_tasks.clear()
    date = timeGen.after_now()
    reset_datas = []
    for app_user in stores.user_apps.get_non_type_id_users_apps():
        reset_datas.append({
            "username": app_user["username"],
            "app_id": app_user["app_id"],
            "start_at": date
        })
    stores.user_apps_tasks.add_tasks_a(reset_datas)
    print("用户apps同步任务已重置")

def start():
    is_delay_task = False
    log.info("开始同步用户apps信息")
    while tasks := stores.tasks.get_pending_tasks(TASK_TYPE, 100):
        if len(tasks) < 1:
            print("当前无可执行任务")
            break

        for task in tasks:
            if is_delay_task:
                stores.tasks.delay_task(task["id"], setting.TASK_DELAY)
                continue

            task_id = task["id"]
            log.set_header(str(task_id))
            log.info(f"任务开始")
            af_user = json.loads(task["task"])
            try:
                ret, msg = sync_user_apps(af_user)
                if ret:
                    stores.tasks.mark_done(task_id)
                    finshed += 1
                    log.info(f"任务 {task_id}, {msg}")
                    continue

                log.warning(f"任务 {task_id}, {msg}")
                raise Exception()
            
            except ex.IPLimitError as e: # 在未使用代理时抛出这个错误意味所有接下来的任务都需要延迟执行
                is_delay_task = True
                log.warning(f"当前IP限制，无代理，推迟所有任务")
                stores.tasks.delay_task(task_id, setting.TASK_DELAY)

            except Exception as e:
                stores.tasks.delay_task(task_id, setting.TASK_DELAY)
                log.info(f"任务{task_id} 结束, 延迟{setting.TASK_DELAY}s")