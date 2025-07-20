import requests
import setting as cfg
import exception 
import time

# 提供 requests 相关的错误处理等
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Content-Type": "application/json;charset=UTF-8",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language":"zh-CN,zh;q=0.9"
}

def _chekc_response_err(reponse):
    if not reponse:
        raise exception.CrawlErr("未返回响应")
    if reponse.status_code == 401:
        raise exception.CrawlCookieExpireErr("登录过期，请重新登录")
    if reponse.status_code == 403 or reponse.status_code == 429:
        raise exception.CrawlRetryErr(15*60,"请求过于频繁，请稍后再试")
    if reponse.status_code == 202:
        raise exception.CrawlRetryErr(8*60, "异步响应202错误")

__last_request_time = 0

def __avoid_frequent_request():
    if time.time() - __last_request_time < cfg.REQUEST_INTERVAL:
        time.sleep(cfg.REQUEST_INTERVAL)

def __reset_request_time():
    global __last_request_time
    __last_request_time = time.time()

# 获取cookie
def login_request(url, data, extend_headers = {}, proxies=None, timeout = cfg.TIMEOUT, times = cfg.RETRY_TIMES):
    __avoid_frequent_request()

    response = None
    new_header = HEADERS.copy()
    new_header.update(extend_headers)
    try:
        response = requests.post(url, json=data, headers=new_header, timeout=timeout, proxies=proxies)
        _chekc_response_err(response)
        response.raise_for_status()
        if not response.cookies:
            raise exception.CrawlErr(f"未获取Cookies，data:{data}, proxies:{proxies}")
        cookie_str = "; ".join([f"{c.name}={c.value}" for c in response.cookies])
        return cookie_str
    
    except exception.CrawlRetryErr as e:
        raise e
    
    except Exception as e:
        raise exception.CrawlErr(f"登录失败，错误：{e}")
    
    finally:
        __reset_request_time()


def post(url, data, extend_headers = {}, proxies=None, timeout = cfg.TIMEOUT, times = cfg.RETRY_TIMES):
    __avoid_frequent_request()
    response = None
    new_header = HEADERS.copy()
    new_header.update(extend_headers)

    try:
        response = requests.post(url, json=data, headers=new_header, timeout=timeout, proxies=proxies)
        _chekc_response_err(response)
        response.raise_for_status()
        return response.json()
    
    except exception.CrawlCookieExpireErr as e:
        raise exception.CrawlRetryErr(10*60, f"POST请求失败，错误：{e}")

    except requests.Timeout as e:
        if times > 0:
            time.sleep(cfg.RETRY_INTERVAL)
            return post(url, data, extend_headers, proxies, timeout, times-1)
        else:
            raise exception.CrawlRetryErr(10*60, f"POST请求超时，已重试{cfg.RETRY_TIMES}次，错误：{e}")
    
    except requests.exceptions.ConnectionError as e:
        raise exception.CrawlRetryErr(15*60,f"POST请求失败，链接错误：{e}")
    
    except requests.HTTPError as e:
        raise exception.CrawlRetryErr(10*60,f"POST 请求失败，网络错误：{e}")
    
    except exception.CrawlFrequentErr as e:
        raise exception.CrawlRetryErr(20*60,f"POST请求失败，错误：{e}")
    
    except exception.CrawlRetryErr as e:
        raise exception.CrawlRetryErr(e.value,f"POST请求失败，已重试{cfg.RETRY_TIMES}次，错误：{e}")

    except Exception as e:
        raise exception.CrawlErr(f"POST {url} 请求失败，错误：{e}")
    
    finally:
        __reset_request_time()
    

def get(url, extend_headers = {}, proxies=None, timeout=cfg.TIMEOUT, times = cfg.RETRY_TIMES):
    __avoid_frequent_request()
    response = None
    new_header = HEADERS.copy()
    new_header.update(extend_headers)
    try:
        response = requests.get(url, headers=new_header, timeout=timeout, proxies=proxies)
        _chekc_response_err(response)
        response.raise_for_status()
        return response.json()
    
    # 处理超时错误
    except exception.CrawlCookieExpireErr as e:
        raise exception.CrawlRetryErr(10*60,f"Url: {url} GET请求失败，错误：{e}")
    
    except requests.exceptions.ConnectionError as e:
        raise exception.CrawlRetryErr(15*60, f"POST请求失败，链接错误：{e}")
    
    except requests.Timeout as e:
        if times > 0:
            time.sleep(cfg.RETRY_INTERVAL)
            return post(url, extend_headers, proxies, timeout, times-1)
        else:
            raise exception.CrawlRetryErr(10*60,f"GET请求超时，已重试{cfg.RETRY_INTERVAL}次，错误：{e}")

    except requests.HTTPError as e:
        raise exception.CrawlRetryErr(10*60, f"GET请求失败，网络错误：{e}")
    
    except exception.CrawlFrequentErr as e:
        raise exception.CrawlRetryErr(10*60, f"GET请求失败，错误：{e}")
    
    except exception.CrawlRetryErr as e:
        raise exception.CrawlRetryErr(e.value, f"GET请求失败，错误：{e}")

    except Exception as e:
        raise exception.CrawlErr(f"GET请求失败，错误：{e}")

    finally:
        __reset_request_time()