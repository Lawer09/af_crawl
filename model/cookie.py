from datetime import datetime
from typing import Optional, Dict, List
from core.db import mysql_pool
import json

class CookieDAO:

    TABLE = "af_user_cookies"

    CREATE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        username VARCHAR(255) NOT NULL UNIQUE,
        password VARCHAR(255) NOT NULL,
        cookies JSON NOT NULL,
        aws_waf_token TEXT,
        af_jwt TEXT,
        auth_tkt TEXT,
        expired_at DATETIME,
        user_agent VARCHAR(512),
        last_used DATETIME DEFAULT CURRENT_TIMESTAMP,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """

    def __init__(self):
        self.db = mysql_pool
        self._init_table()

    def _init_table(self):
        try:
            self.db.execute(self.CREATE_SQL)
        except Exception as e:
            print(f"[DB ERROR] create af_user_cookies failed: {e}")

    def _serialize_cookies(self, cookies_list):
        """将Cookie列表序列化为JSON字符串"""
        return json.dumps(cookies_list, ensure_ascii=False)
    
    def _deserialize_cookies(self, cookies_str):
        """将JSON字符串反序列化为Cookie列表"""
        return json.loads(cookies_str) if cookies_str else []
    
    def _extract_special_cookies(self, cookies_list):
        """提取特殊Cookie"""
        special_cookies = {
            'aws_waf_token': None,
            'af_jwt': None,
            'auth_tkt': None
        }
        
        for cookie in cookies_list:
            name = cookie.get('name', '')
            if name == 'aws-waf-token':
                special_cookies['aws_waf_token'] = cookie.get('value')
            elif name == 'af_jwt':
                special_cookies['af_jwt'] = cookie.get('value')
            elif name == 'auth_tkt':
                special_cookies['auth_tkt'] = cookie.get('value')
        
        return special_cookies
    
    def add_or_update_cookie(
        self,
        username: str,
        password: str,
        cookies: list,
        expired_at: datetime,
        user_agent: str = None
    ) -> bool:
        """
        添加或更新cookie记录（增强版）
        :param username: 用户名
        :param password: 密码
        :param cookies: cookie字典列表
        :param expired_at: 过期时间
        :param user_agent: 使用的User-Agent
        :return: 是否成功
        """
        try:
            # 提取特殊Cookie
            special_cookies = self._extract_special_cookies(cookies)
            
            query = f"""
            REPLACE INTO {self.TABLE} 
            (username, password, cookies, aws_waf_token, af_jwt, auth_tkt,
             expired_at, user_agent, last_used)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """
            params = (
                username,
                password,
                self._serialize_cookies(cookies),
                special_cookies['aws_waf_token'],
                special_cookies['af_jwt'],
                special_cookies['auth_tkt'],
                expired_at,
                user_agent
            )
            self.db.execute(query, params)
            return True
        except Exception as e:
            print(f"添加/更新cookie失败: {e}")
            return False
    
    def get_cookie_by_username(self, username: str) -> Optional[Dict]:
        """
        根据用户名获取完整cookie记录
        :param username: 用户名
        :return: 包含所有cookie信息的字典
        """
        query = f"""
        SELECT id, username, password, cookies, aws_waf_token, af_jwt, auth_tkt,
               created_at, expired_at, user_agent, last_used
        FROM {self.TABLE}
        WHERE username = %s
        """
        record = self.db.fetch_one(query, (username,))
        if record:
            # 反序列化cookies
            record['cookies'] = self._deserialize_cookies(record['cookies'])
            return record
        return None
    
    def restore_browser_context(self, context, username: str) -> bool:
        """
        将保存的Cookie恢复到浏览器上下文
        :param context: Playwright的BrowserContext对象
        :param username: 用户名
        :return: 是否成功
        """
        record = self.get_cookie_by_username(username)
        if not record:
            return False
        
        try:
            # 清空现有Cookie
            context.clear_cookies()
            
            # 添加基础Cookie
            for cookie in record['cookies']:
                # 确保必要的Cookie字段存在
                cookie.setdefault('domain', '.appsflyer.com')
                cookie.setdefault('path', '/')
                cookie.setdefault('httpOnly', False)
                cookie.setdefault('secure', True)
                cookie.setdefault('sameSite', 'Lax')
                context.add_cookies([cookie])
            
            # 添加特殊Cookie（确保它们存在）
            special_cookies = []
            if record['aws_waf_token']:
                special_cookies.append({
                    'name': 'aws-waf-token',
                    'value': record['aws_waf_token'],
                    'domain': '.appsflyer.com',
                    'path': '/',
                    'httpOnly': False,
                    'secure': True,
                    'sameSite': 'Lax'
                })
            
            if record['af_jwt']:
                special_cookies.append({
                    'name': 'af_jwt',
                    'value': record['af_jwt'],
                    'domain': '.appsflyer.com',
                    'path': '/',
                    'httpOnly': True,
                    'secure': True,
                    'sameSite': 'Lax',
                    'expires': record['expired_at'].timestamp()
                })
            
            if record['auth_tkt']:
                special_cookies.append({
                    'name': 'auth_tkt',
                    'value': record['auth_tkt'],
                    'domain': '.appsflyer.com',
                    'path': '/',
                    'httpOnly': True,
                    'secure': True,
                    'sameSite': 'Lax',
                    'expires': record['expired_at'].timestamp()
                })
            
            if special_cookies:
                context.add_cookies(special_cookies)
            
            return True
        except Exception as e:
            print(f"恢复Cookie到浏览器失败: {e}")
            return False
        
cookie_model = CookieDAO()