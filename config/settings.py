import os
from pathlib import Path
from dotenv import load_dotenv

# 项目根目录（保证此文件可被其它模块引用时找到 .env）
BASE_DIR = Path(__file__).resolve().parent.parent

# 如果存在 .env，则加载
dotenv_path = BASE_DIR / '.env'
if dotenv_path.exists():
    load_dotenv(dotenv_path)

# 数据库连接配置
MYSQL = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'af_crawl'),
    'pool_name': 'af_pool',
    'pool_size': int(os.getenv('MYSQL_POOL_SIZE', '10')),
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
    'headless': os.getenv('PW_HEADLESS', 'true').lower() == 'true',
    'slow_mo': int(os.getenv('PW_SLOWMO', '1000')),  # 增加延迟
    'timeout': int(os.getenv('PW_TIMEOUT', '180000')),  # 增加到3分钟
}

# 降低并发数量
_proc_default = int(os.getenv('CRAWLER_PROCESSES', '2')) if USE_PROXY else 1
_thread_default = int(os.getenv('CRAWLER_THREADS', '2')) if USE_PROXY else 1

CRAWLER = {
    'processes': _proc_default,
    'threads_per_process': _thread_default,
    'max_retry': int(os.getenv('CRAWLER_MAX_RETRY', '5')),  # 增加重试次数
    'retry_delay_seconds': int(os.getenv('CRAWLER_RETRY_DELAY', '300')),  
}

# 其他常量
aWS_WAF_TOKEN_NAME = 'aws-waf-token'