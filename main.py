from multiprocessing import Pool
import setting
from tasks import sync_user_apps_info_task as st
from tasks import click_gap_task as ct
from utils import timeGen

IS_SYNC_USER_APPS = False

def sync_user_apps(af_user):
    st.sync_user_apps(af_user)

def sync_user_apps_statics(af_user):
    last_date = timeGen.get_last_N_days(n=2)[1]
    ct.sync_user_click_gap_data(af_user, last_date)

if __name__ == '__main__':
    import stores_m
    af_users = stores_m.users.get_enable_af_users()

    if IS_SYNC_USER_APPS:
        with Pool(processes=setting.FORK_NUM) as pool:
            pool.map(sync_user_apps, af_users)
    else:
        af_users = stores_m.users.get_enable_af_users()
        with Pool(processes=setting.FORK_NUM) as pool:
            pool.map(sync_user_apps_statics, af_users)


