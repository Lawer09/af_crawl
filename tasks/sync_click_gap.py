import stores_m as stores
import json
import log
import setting
from crawl import af_crawl as ac
import exception as ex
from config import click_gap_task_config as cfg
from utils import timeGen

UPLOAD_TABLE_NAME = "af_click_gap"
TASK_TYPE = "click_gap_task"


def upload_click_gap_data():
    UPDATE_COUNT = 1000
    db = stores.AdbinkAfClickGapStore()
    apps = stores.user_apps_statics.get_update_all()
    date = setting.CRAWL_DATE
    seg_datas = [apps[i:i + UPDATE_COUNT] for i in range(0, len(apps), UPDATE_COUNT)]
    for seg_data in seg_datas:
        filter_datas = []
        for data in seg_data:
            try:
                data["offer_id"] = int(data["offer_id"])
                data["date"] = date
                filter_datas.append(data)
            except ValueError:
                continue
        db.save_all(filter_datas)
        print("已保存",len(filter_datas))


def _update_tasks(fail_tasks, delay_tasks, done_tasks):
    """更新任务"""
    stores.user_apps_tasks.fail_tasks(fail_tasks)
    stores.user_apps_tasks.delay_tasks(delay_tasks)
    stores.user_apps_tasks.mark_done_tasks(done_tasks)


def sync_task(af_user):
    """同步任务"""
    # date = timeGen.get_now_date()
    username = af_user["email"]
    _is_pid_user = ac.is_pid_user(af_user)
    date = setting.CRAWL_DATE
    while tasks := stores.user_apps_tasks.get_pending_tasks(username, 100):
        if len(tasks) < 1:
            break
        
        delay_tasks = []
        fail_tasks = []
        done_tasks = []
        _is_pid_user = ac.is_pid_user(af_user)
        curTaskId = None
        try:
            while task:= tasks.pop():
                app_static_list = []
                curTaskId = task["id"]
                app_id = task["app_id"]
                tapp = stores.user_apps.get_user_app(username, app_id)
                if not tapp or not tapp["timezone"]:
                    fail_tasks.append({"id": curTaskId, "reason": f"{username} and {app_id} no timezone"})
                    continue

                timezone = tapp["timezone"]
                datas = ac.get_table_data_f(af_user, app_id, date, date)
                # datas = ac.get_table_data_new(af_user, app_id, date, date)
                
                type_id_key = "pid" if _is_pid_user else "prt"
                type_id_value = tapp["user_type_id"]
                for data in datas:
                    app_static_list.append({
                        "app_id": app_id,
                        "offer_id": data["offer_id"],
                        "username": username,
                        "last_clicks": data["af_clicks"],
                        "last_installs": data["af_install"],
                        "timezone": timezone,
                        type_id_key: type_id_value,
                    })
                stores.user_apps_statics.save_all(app_static_list)
                done_tasks.append(curTaskId)

                if len(tasks) < 1:
                    break

        except ex.CrawlErr as e:
            if curTaskId:
                fail_tasks.append({"id": curTaskId, "reason": str(e)})
            for fail_task in tasks:
                fail_tasks.append({"id": fail_task["id"], "reason": str(e)})
            return False, e

        except ex.CrawlRetryErr as e:
            if curTaskId:
                delay_tasks.append({"id": curTaskId,"delay_seconds": e.value, "reason": str(e)})
            reason_str = str(e)
            for delay_task in tasks:
                delay_tasks.append({"id": delay_task["id"], "delay_seconds": e.value, "reason":reason_str})
            return True, e
        
        except Exception as e:
            if curTaskId:
                fail_tasks.append({"id": curTaskId, "reason": str(e)})
            for fail_task in tasks:
                fail_tasks.append({"id": fail_task["id"], "reason": str(e)})
            return False, e
        
        finally:
            _update_tasks(fail_tasks, delay_tasks, done_tasks)

    return True, username

import time

def reset_date():
    """重置任务"""
    cur = time.time()
    # 删除所有任务
    stores.user_apps_tasks.clear()
    start_at = timeGen.get_now_str()
    task_datas = []
    for user_app in stores.user_apps.get_all_apps():
        task_datas.append({
            "app_id": user_app["app_id"],
            "username": user_app["username"],
            "start_at": start_at,
        })
    stores.user_apps_tasks.add_tasks_a(task_datas)
    cur = time.time() - cur
    print(f"重置任务耗时：{cur}s")