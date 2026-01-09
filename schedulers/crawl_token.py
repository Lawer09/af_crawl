import logging
from core import redis_client
from core.crawl_token import get_aws_waf_token
AF_LOGIN_URL = "https://hq1.appsflyer.com/auth/login"

logger = logging.getLogger(__name__)

def simple_sync_af_aws_waf_token():
    """简单获取token"""
    logger.info("=== simple_sync_af_aws_waf_token start ===")
    logger.info(f"before token: {redis_client.redis_client.get('aws-waf-token')}")
    token = get_aws_waf_token(goto_url=AF_LOGIN_URL)
    if not token:
        logger.warning("token is None")
    else:
        redis_client.redis_client.set("aws-waf-token", f"aws_waf_token={token}", ex=60*6)
        logger.info(f"token: {token} set to redis")
    logger.info("=== simple_sync_af_aws_waf_token end ===")