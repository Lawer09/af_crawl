from datetime import timezone, timedelta

FORK_NUM = 4


LOCAL_DB = {
    "host": "localhost",
    "port": 3306,
    # "user": "ztsis",
    "user": "root",
    "password": "123456",
    "database": "craw"
}

ADBLINK_DB = {
    "host": "rm-0xitsd6nu26k8z5z9oo.mysql.rds.aliyuncs.com",
    "port": 3306,
    "user": "adbink",
    "password": "YiQoRi1afo&e8wAneglz",
    "database": "adbink"
}

CRAWL_DATE = "2025-07-07"

LOCAL_DB_NAME = "crawl.db"
TASK_DB_NAME = "tasks.db"
COOKIES_EXPIRE_TIME = 10 * 60
# 如果小于最小时间则重新获取
COOKIES_MIN_TIME = 1 * 60

# 每次取出的任务数量
ONECE_TASK_NUM = 100

TASK_DEATH_TIME = "23:30:00"
TASK_DELAY = 10 * 60

USE_PROXY = False

TIMEOUT = 15 
REQUEST_INTERVAL = 4
RETRY_TIMES = 2
RETRY_INTERVAL = 5

TIMEZONE = timezone(timedelta(hours=8))