import os
from pathlib import Path
from dotenv import load_dotenv

# 项目根目录（保证此文件可被其它模块引用时找到 .env）
BASE_DIR = Path(__file__).resolve().parent.parent

# 如果存在 .env，则加载
dotenv_path = BASE_DIR / '.env'
if dotenv_path.exists():
    load_dotenv(dotenv_path)

SYSTEM_TYPE = os.getenv('SYSTEM_TYPE', 'CHSANGSHA')

# 主数据库连接配置
MYSQL = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'af_crawl'),
    'pool_name': 'af_pool',
    'pool_size': int(os.getenv('MYSQL_POOL_SIZE', '5')),
}

FEISHU = {
    'sys_notify_webhook': os.getenv('FEISHU_SYS_NOTIFY_WEBHOOK', "https://open.feishu.cn/open-apis/bot/v2/hook/5adf3173-701b-4533-b5f8-2f1dbfaf2068"),
    'zhengzhou_notify_webhook': os.getenv('FEISHU_ZHENGZHOU_NOTIFY_WEBHOOK', "https://open.feishu.cn/open-apis/bot/v2/hook/9ccd5e08-4cd3-4c31-bc31-94b839fe0100"),
}

# 报表数据库连接配置
REPORT_MYSQL = {
    'host': os.getenv('REPORT_MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('REPORT_MYSQL_PORT', '3306')),
    'user': os.getenv('REPORT_MYSQL_USER', 'root'),
    'password': os.getenv('REPORT_MYSQL_PASSWORD', ''),
    'database': os.getenv('REPORT_MYSQL_DATABASE', 'adbink_report'),
    'pool_name': os.getenv('REPORT_MYSQL_POOL_NAME', 'report_pool'),
    'pool_size': int(os.getenv('REPORT_MYSQL_POOL_SIZE', '5')),
}

# 是否使用代理
USE_PROXY = os.getenv('USE_PROXY', 'false').lower() == 'true'

# 代理池配置
# 代理健康检查配置
PROXY = {
    'ipweb_token': os.getenv('IPWEB_TOKEN', ''),
    'default_country': os.getenv('PROXY_COUNTRY', 'SG'),
    'default_times': int(os.getenv('PROXY_TIMES', '15')),
    'default_count': int(os.getenv('PROXY_COUNT', '3')),  # 减少代理数量
    'timeout': int(os.getenv('PROXY_TIMEOUT', '30')),
    'max_failures': int(os.getenv('PROXY_MAX_FAILURES', '3')),
}

# Playwright 相关
# 增加超时时间
PLAYWRIGHT = {
    'user_agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36'),
    'timezone_id': os.getenv('TIMEZONE_ID', 'America/New_York'),
    'headless': os.getenv('PW_HEADLESS', 'true').lower() == 'true',
    'slow_mo': int(os.getenv('PW_SLOWMO', '1000')),  # 增加延迟
    'timeout': int(os.getenv('PW_TIMEOUT', '180000')),  # 增加到3分钟
}

# 登录后的会话（Cookie）有效时间，单位：分钟
# 默认为 15 分钟，可在 .env 中通过 SESSION_EXPIRE_MINUTES 覆盖
SESSION_EXPIRE_MINUTES = int(os.getenv('SESSION_EXPIRE_MINUTES', '15'))

_proc_default = int(os.getenv('CRAWLER_PROCESSES', '1'))
_thread_default = int(os.getenv('CRAWLER_THREADS', '1'))

CRAWLER = {
    'interval_hours': int(os.getenv('CRAWLER_INTERVAL_HOURS', '3')),
    'processes': _proc_default,
    'threads_per_process': _thread_default,
    'max_retry': int(os.getenv('CRAWLER_MAX_RETRY', '5')),  # 增加重试次数
    'retry_delay_seconds': int(os.getenv('CRAWLER_RETRY_DELAY', '300')),  
    # 在 202 且响应未带新 token 时，是否轻量 GET 登录页播种 aws-waf-token
    'seed_waf_on_202': os.getenv('SEED_WAF_ON_202', 'true').lower() in ('true','1','yes'),
    # 播种节流（同一用户名最小间隔秒数）
    'seed_waf_cooldown_seconds': int(os.getenv('SEED_WAF_COOLDOWN_SECONDS', '180')),
}

AF_DATA_FILTERS = {
    'groups_dim1': os.getenv('AF_DATA_GROUPS_DIM1', 'adgroup'),
    'groups_dim2': os.getenv('AF_DATA_GROUPS_DIM2', 'adgroup-id'),
}

# 其他常量
aWS_WAF_TOKEN_NAME = 'aws-waf-token'