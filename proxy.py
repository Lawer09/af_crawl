import requests
import logging
import time
from exception import ProxyGetError
import logging
import setting

SERVER = "gate2.ipweb.cc"
PORT = 7778
IP_WEB_TOKEN = "W330LHLGU1HWXFHSUUC260KUSEP8ERR3"
IP_WEB_API = "http://api.ipweb.cc:8004/api/agent/account2"

DEFAULT_IP_COUNT = 5
DEFAULT_TIME = 15

# 每个代理结构体：{"proxy": proxy_url, "expire_time": timestamp}
proxy_pool = []

source_proxy = {}

# 获取IP代理
def _get_ipweb_proxy(country="AR", times=setting.COOKIES_EXPIRE_TIME, limit=1):
    headers = {
        "Token": IP_WEB_TOKEN
    }
    params = {
        "country": country,
        "times": times,
        "limit": limit
    }

    try:
        response = requests.get(IP_WEB_API, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("code") != 200 or "data" not in data:
            logging.warning(f"IPWEB API返回错误: {data}")
            raise ProxyGetError()

        accounts = data["data"]
        if isinstance(accounts, list):
            proxies = []
            for account in accounts:
                proxies.append({"proxy": f"http://{account}@{SERVER}:{PORT}", "expire_time": time.time() + times * 60})
            return proxies
        else:
            logging.warning(f"IPWEB 返回accounts格式错误: {accounts}")
            raise ProxyGetError()
    except Exception as e:
        logging.error(f"获取代理异常: {e}")
        raise ProxyGetError()


# 初始化代理池
def init_proxy_pool(country="SG", times=DEFAULT_TIME, count=DEFAULT_IP_COUNT):
    logging.info("初始化代理池")
    global proxy_pool
    try:
        proxies = _get_ipweb_proxy(country=country, times=times, limit=count)
        proxy_pool.extend(proxies)
        return True
    except ProxyGetError:
        logging.warning(" init_proxy_pool 失败")
        return False


# 清理和补充代理池
def _check():
    global proxy_pool
    now = time.time()
    proxy_pool = [p for p in proxy_pool if p["expire_time"] > now]
    if not proxy_pool and init_proxy_pool():
        return True
    if len(proxy_pool) < 1:
        return init_proxy_pool()
    return False

# 获取一个代理
def get_proxy(source, frush = False, default_proxy = ""):
    if not _check():
        logging.warning("代理池为空")
        return default_proxy
    
    if frush or source not in source_proxy:
        proxy = proxy_pool.pop(0)["proxy"]
        source_proxy[source] = proxy
        return proxy

    return source_proxy[source]

def get_new_proxy():
    return _get_ipweb_proxy()[0]["proxy"]


def build_requests_proxy(proxy_url):
    if not proxy_url:
        return None
    return {
        "http": proxy_url,
        "https": proxy_url,
    }

if __name__ == "__main__":
    proxy = get_proxy("1")
    print("可用代理:", proxy)
