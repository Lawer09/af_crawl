import stores_m as stores
import setting
import exception as ex
from crawl import af_crawl as ac

def sync_task():

    af_users = stores._get_enable_af_users_from_db()
    enable_af_users = []
    for af_user in af_users:
        try:
            ac.af_login(af_user)
            enable_af_users.append(af_user) 
        except Exception as e:
            print(e)
            continue
    # for af_user in enable_af_users:
    #     stores.users.save_user
