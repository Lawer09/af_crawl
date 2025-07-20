import setting
from tasks import sync_user_apps_info as st
from tasks import sync_click_gap as ct
from utils import timeGen
import stores_m as stores
import time
SYNC_USER_APPS = 1
SYNC_CLICK_GAP = 2

if __name__ == '__main__':
    import sys
    cmd = SYNC_USER_APPS
    try:
        cmd = int(sys.argv[1])
    except Exception as e:
        pass
    # sync_user_apps_info_task.reset()
    # sync_user_apps_info_task.start()
    # click_gap_task.reset()

    af_users = stores.users.get_enable_af_users()
    results =[]
    cur = time.time()

    if cmd == SYNC_USER_APPS:
        print("同步用户app信息")
        while True:
            for af_user in af_users:
                _cur = time.time()
                print("开始 "+timeGen.get_now_str())
                st.sync_task(af_user)
                print("本次执行时间 ",time.time()-_cur)
            wait_execute_task = stores.user_apps_tasks.get_enable_tasks()
            if not wait_execute_task or len(wait_execute_task) < 1:
                break
            near_start_time = wait_execute_task[0]["start_at"]
            print(f"剩余待完成任务数:{len(wait_execute_task)} 下次执行时间：{near_start_time}")
            sleep_time = timeGen.seconds_between(timeGen.get_now_str(), str(near_start_time))
            if sleep_time > 0:
                time.sleep(sleep_time)
            print("上传 数据")
            st.upload_user_apps()
    elif cmd == SYNC_CLICK_GAP:
        print("同步APP统计信息,同步日期：", setting.CRAWL_DATE)
        date_str = timeGen.get_now_date()
        while True:
            for af_user in af_users:
                _cur = time.time()
                cur = time.time()
                print("开始 "+timeGen.get_now_str())
                ct.sync_task(af_users)
                print("本次执行时间 ",time.time()-_cur)
            wait_execute_task = stores.user_apps_tasks.get_enable_tasks()
            if not wait_execute_task or len(wait_execute_task) < 1:
                break
            near_start_time = wait_execute_task[0]["start_at"]
            print(f"剩余待完成任务数:{len(wait_execute_task)} 下次执行时间：{near_start_time}")
            sleep_time = timeGen.seconds_between(timeGen.get_now_str(), str(near_start_time))
            if sleep_time > 0:
                time.sleep(sleep_time)
    print(f"耗时：{time.time() - cur}")