from __future__ import annotations

import logging
from typing import List, Dict, Optional

from core.db import mysql_pool
from urllib.parse import urlsplit, quote

logger = logging.getLogger(__name__)


class UserDAO:
    """af_user 表简单封装"""

    TABLE = "af_user"

    @classmethod
    def get_enabled_users(cls) -> List[Dict]:
        sql = f"SELECT email, password, account_type FROM {cls.TABLE} WHERE enable = 1 AND account_type in ('pid','agency') AND (email IS NOT NULL AND TRIM(email) <> '') AND (password IS NOT NULL AND TRIM(password) <> '') "
        return mysql_pool.select(sql)

    @classmethod
    def get_user_by_email(cls, email: str) -> Optional[Dict]:

        try:
            rows = mysql_pool.select(
                f"SELECT pid, email, password, account_type, 2fa_key FROM {cls.TABLE} WHERE email = %s",
                (email,)
            )

            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user by email: {e}")
            return None

    @classmethod
    def get_user_by_pid(cls, pid: str) -> Optional[Dict]:
        """根据 pid 查询用户（当 pid='pid'）"""
        try:
            rows = mysql_pool.select(
                f"SELECT id, email, password, account_type, 2fa_key FROM {cls.TABLE} WHERE pid = %s",
                (pid,)
            )
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user by pid: {e}")
            return None

    @classmethod
    def get_user_id_by_pid(cls, pid: str) -> Optional[int]:
        """仅返回用户 id（便于外部关系映射）"""
        try:
            rows = mysql_pool.select(
                f"SELECT id FROM {cls.TABLE} WHERE pid = %s LIMIT 1",
                (pid,)
            )
            if rows:
                return int(rows[0]["id"])  # type: ignore
            return None
        except Exception as e:
            logger.error(f"Error fetching user id by pid: {e}")
            return None

    @classmethod
    def update_2fa_key_by_pid(cls, pid: str, secret: str) -> int:
        """更新指定 pid 的 2FA 密钥，返回受影响的行数。"""
        try:
            sql = f"UPDATE {cls.TABLE} SET 2fa_key = %s WHERE pid = %s"
            affected = mysql_pool.execute(sql, (secret, pid))
            return int(affected or 0)
        except Exception as e:
            logger.error(f"Error updating 2fa_key for pid={pid}: {e}")
            return 0

    @classmethod
    def save_user(cls, email: str, password: str, account_type: str):
        sql = f"""
        INSERT INTO {cls.TABLE} (email, password, account_type, enable)
        VALUES (%s,%s,%s,1)
        ON DUPLICATE KEY UPDATE password=VALUES(password), account_type=VALUES(account_type)
        """
        mysql_pool.execute(sql, (email, password, account_type)) 

    @classmethod
    def get_users_by_emails(cls, emails: List[str]) -> Dict[str, Dict]:
        if not emails:
            return {}
        placeholders = ','.join(['%s'] * len(emails))
        sql = f"SELECT email, password, account_type FROM {cls.TABLE} WHERE email IN ({placeholders})"
        rows = mysql_pool.select(sql, tuple(emails))
        return {row['email']: row for row in rows}

    @classmethod
    def get_users_by_pids(cls, pids: List[str]) -> Dict[str, Dict]:
        """批量根据 pid 查询用户，返回 {pid: {email,password,account_type}}"""
        if not pids:
            return {}
        placeholders = ','.join(['%s'] * len(pids))
        sql = f"SELECT pid, email, password, account_type FROM {cls.TABLE} WHERE pid IN ({placeholders})"
        rows = mysql_pool.select(sql, tuple(pids))
        return {row['pid']: {'email': row['email'], 'password': row['password'], 'account_type': row['account_type']} for row in rows}


def _mask_proxy_for_log(proxy_url: str) -> str:
    try:
        s = str(proxy_url).strip()
        if '://' not in s:
            s = f"http://{s}"
        parsed = urlsplit(s)
        host = parsed.hostname or ''
        port = f":{parsed.port}" if parsed.port else ''
        user = parsed.username or ''
        masked_user = (user[:2] + '...') if user else ''
        auth = f"{masked_user}:***@" if (parsed.username or parsed.password) else ''
        scheme = parsed.scheme if parsed.scheme in ('http','https') else 'http'
        return f"{scheme}://{auth}{host}{port}"
    except Exception:
        return str(proxy_url)


def _sanitize_proxy_url(raw: str | None) -> tuple[Optional[str], Optional[str]]:
    if raw is None:
        return None, "empty"
    s = str(raw).strip()
    # 去除包裹的引号/反引号/尖括号
    while len(s) >= 2 and ((s[0] == s[-1] and s[0] in ("'", '"', "`")) or (s[0] == "<" and s[-1] == ">")):
        s = s[1:-1].strip()
    s = s.strip(",;")
    if not s:
        return None, "empty"
    # 自动补齐 scheme
    if "://" not in s:
        s = "http://" + s
    try:
        parsed = urlsplit(s)
        if parsed.scheme not in ("http", "https"):
            return None, f"invalid_scheme:{parsed.scheme}"
        if not parsed.hostname:
            return None, "missing_host"
        if parsed.port is not None:
            try:
                _ = int(parsed.port)
            except Exception:
                return None, "invalid_port"
        # 编码用户名/密码中的特殊字符
        if parsed.username is not None or parsed.password is not None:
            user_enc = quote(parsed.username or "", safe="")
            pass_enc = quote(parsed.password or "", safe="")
            auth = f"{user_enc}:{pass_enc}@"
        else:
            auth = ""
        host = parsed.hostname
        port = f":{parsed.port}" if parsed.port else ""
        clean = f"{parsed.scheme}://{auth}{host}{port}"
        return clean, None
    except Exception as e:
        return None, f"parse_error:{type(e).__name__}"

class UserProxyDAO:
    """_tb_static_proxy 表：用户静态代理配置（与 af_user.pid 一一对应）"""

    TABLE = "_tb_static_proxy"

    @classmethod
    def get_by_pid(cls, pid: str) -> Optional[Dict]:
        """根据 pid 查询一条代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE pid = %s LIMIT 1"
            )
            rows = mysql_pool.select(sql, (pid,))
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching user proxy by pid: {e}")
            return None
    
    @classmethod
    def get_list_in_system_types(cls, system_types: List[str]) -> Optional[List[Dict]]:
        """根据 system_type 查询所有代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE system_type IN ({','.join(['%s']*len(system_types))})"
            )
            rows = mysql_pool.select(sql, tuple(system_types))
            if rows:
                return rows
            return None
        except Exception as e:
            logger.error(f"Error fetching user proxy by system_type: {e}")
            return None

    @classmethod
    def get_random_one(cls) -> Optional[Dict]:
        """随机获取一条未停用的代理记录"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE deactivate = 0 ORDER BY RAND() LIMIT 1"
            )
            rows = mysql_pool.select(sql)
            if rows:
                return rows[0]
            return None
        except Exception as e:
            logger.error(f"Error fetching random user proxy: {e}")
            return None

    @classmethod
    def get_enable(cls) -> List[Dict]:
        """加载启用的静态代理，执行 proxy_url 清洗与校验"""
        try:
            sql = (
                f"SELECT id, pid, proxy_url, country, sub_at, end_at, created_at, "
                f"system_type, ua, timezone_id FROM {cls.TABLE} WHERE deactivate = 0"
            )
            rows = mysql_pool.select(sql) or []
            cleaned_rows: List[Dict] = []
            for rec in rows:
                raw = rec.get("proxy_url")
                sanitized, err = _sanitize_proxy_url(raw) if raw else (None, "empty")
                if sanitized:
                    if raw != sanitized:
                        logger.info(
                            "proxy_url sanitized: pid=%s raw=%s -> %s",
                            rec.get("pid"),
                            _mask_proxy_for_log(raw),
                            _mask_proxy_for_log(sanitized),
                        )
                    rec["proxy_url"] = sanitized
                    rec.pop("proxy_error", None)
                else:
                    logger.warning(
                        "proxy_url invalid: pid=%s url=%s err=%s; mark empty",
                        rec.get("pid"),
                        _mask_proxy_for_log(raw or ""),
                        err,
                    )
                    rec["proxy_url"] = ""
                    rec["proxy_error"] = err
                cleaned_rows.append(rec)
            return cleaned_rows
        except Exception as e:
            logger.error(f"Error fetching enabled user proxies: {e}")
            return []

    @classmethod
    def add_or_update(
        cls,
        pid: str,
        proxy_url: str,
        system: int,
        user_agent: Optional[str] = None,
        country: Optional[str] = None,
        timezone_id: Optional[str] = None,
    ) -> bool:
        """新增或更新静态代理配置（按 pid 唯一）。

        - 清洗并校验 `proxy_url`（自动补 http://，校验主机与端口格式）。
        - 若存在记录则更新字段：`proxy_url`, `system_type`, `ua`, `country`, `timezone_id`。
        - 若不存在则插入一条记录，默认 `deactivate=0`。
        """
        try:
            if not pid:
                logger.warning("add_or_update invalid pid")
                return False

            sanitized, err = _sanitize_proxy_url(proxy_url)
            if not sanitized:
                logger.warning("add_or_update invalid proxy_url: pid=%s url=%s err=%s", pid, _mask_proxy_for_log(proxy_url), err)
                # 允许写入空代理，以便后续补充
                sanitized = ""

            rows = mysql_pool.select(f"SELECT id FROM {cls.TABLE} WHERE pid = %s LIMIT 1", (pid,))
            if rows:
                rid = rows[0]["id"]
                sql = (
                    f"UPDATE {cls.TABLE} SET proxy_url=%s, system_type=%s, ua=%s, country=%s, timezone_id=%s "
                    f"WHERE id=%s"
                )
                mysql_pool.execute(sql, (sanitized, system, user_agent, country, timezone_id, rid))
                logger.info("Updated static proxy: pid=%s url=%s system=%s country=%s tz=%s", pid, _mask_proxy_for_log(sanitized), system, country, timezone_id)
            else:
                sql = (
                    f"INSERT INTO {cls.TABLE} (pid, proxy_url, system_type, ua, country, timezone_id, deactivate) "
                    f"VALUES (%s,%s,%s,%s,%s,%s,0)"
                )
                mysql_pool.execute(sql, (pid, sanitized, system, user_agent, country, timezone_id))
                logger.info("Inserted static proxy: pid=%s url=%s system=%s country=%s tz=%s", pid, _mask_proxy_for_log(sanitized), system, country, timezone_id)
            return True
        except Exception as e:
            logger.exception("UserProxyDAO.add_or_update failed: pid=%s err=%s", pid, e)
            return False
