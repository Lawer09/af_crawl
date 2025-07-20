class LoginExpiredError(Exception):
    """表示登录过期的异常"""
    def __init__(self, message="登录已过期，请重新登录。"):
        self.message = message
        super().__init__(self.message)

class IPLimitError(Exception):
    def __init__(self, message="IP请求限制，请稍后再试。"):
        self.message = message
        super().__init__(self.message)

class ProxyGetError(Exception):
    def __init__(self, message="获取代理ip失败"):
        self.message = message
        super().__init__(self.message)

class CrawlLoginErr(Exception):
    def __init__(self, message="登录失败"):
        self.message = message
        super().__init__(self.message)

class CrawlErr(Exception):
    def __init__(self, message="爬取请求失败"):
        self.message = message
        super().__init__(self.message)

class CrawlTimeoutErr(Exception):
    def __init__(self, message="爬取请求超时"):
        self.message = message
        super().__init__(self.message)

class CrawlCookieExpireErr(Exception):
    def __init__(self, message="Cookie过期"):
        self.message = message
        super().__init__(self.message)

class CrawlFrequentErr(Exception):
    def __init__(self, message="请求频繁，稍后重试"):
        self.message = message
        super().__init__(self.message)

class CrawlRetryErr(Exception):
    def __init__(self, delay_times=60, message="请求频繁，稍后重试"):
        self.value = delay_times
        self.message = message
        super().__init__(self.message)

class AFFormatErr(Exception):
    def __init__(self, message="AF数据格式错误"):
        self.message = message

class UserVerifyErr(Exception):
    def __init__(self, message="用户认证信息错误"):
        self.message = message

class UserInfoNotSync(Exception):
    def __init__(self, message="用户信息未同步"):
        self.message = message


class TaskDelayErr(Exception):
    def __init__(self, message="任务延迟"):
        self.message = message

class TaskFailErr(Exception):
    def __init__(self, message="任务失败"):
        self.message = message

class TaskDelayAllErr(Exception):
    def __init__(self, message="任务全部延迟"):
        self.message = message