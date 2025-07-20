from crawl import af_crawl
from utils import timeGen
if __name__ == '__main__':
    af_user = {
        "email":"emily@sparkworkse.com",
        "password":"Sparkwork#2024",
        "account_type":"pid",
    }

    af_crawl.af_login()

    appid="com.bybit.app"
    date = "2025-05-17"
    print(af_crawl.get_table_data_new(af_user,appid,date,date)) 
    # print(timeGen.is_before_today_time("13:00:00"))
    # print(timeGen.seconds_between("2025-06-07 12:00:00", "2025-06-07 11:05:30"))
    # strs = timeGen.get_now_str()
    # print(strs)
    # print(timeGen.after_now(seconds=1200))
    pass