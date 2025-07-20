from multiprocessing import Pool
import setting
from tasks import sync_user_apps_info_task as st
import stores

if __name__ == '__main__':
    pool = Pool(setting.FORK_NUM)

    while tasks := stores.tasks.get_pending_tasks(st.TASK_TYPE, 50):
        pool.apply_async(st.get_user_list_apps, (tasks,))

    pool.close()
    pool.join()
