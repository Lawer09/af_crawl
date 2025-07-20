from utils.dbUtils import db
from database import MySqlDatabase
import sqlite3
import setting
from typing import List, Dict, Optional,Tuple
import log
from utils import timeGen
""" 从adnet 数据库 获取 af_user信息 """
# af_user表
# id, email, password, account_type, enable

# 获取可用的用户
def _get_enable_af_users_from_db():
    query_where = """
    enable = 1 
    AND account_type in ('pid','agency') 
    AND (email IS NOT NULL AND TRIM(email) <> '') 
    AND (password IS NOT NULL AND TRIM(password) <> '') 
    ORDER BY id
    """
    return db.query("af_user", query_where)

def get_db(db_setting):
    return MySqlDatabase(db_setting)

_db = MySqlDatabase(setting.LOCAL_DB)

class UserStore:
    def __init__(self):
        self.db = _db
        self._init_db()

    def _init_db(self):
        try:
            self.db.execute("""
                CREATE TABLE IF NOT EXISTS af_users (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    email VARCHAR(255) NOT NULL UNIQUE,
                    password VARCHAR(255) NOT NULL,
                    account_type VARCHAR(50) NOT NULL
                )
            """)
        except Exception as e:
            print(f"[DB ERROR] Failed to init user table: {e}")

    def export_to_file(self, file_path: str):
        users = self.get_enable_af_users()
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write("email,password,account_type\n")
                for user in users:
                    f.write(f"{user['email']},{user['password']},{user['account_type']}\n")
        except Exception as e:
            print(f"[FILE ERROR] Failed to export users: {e}")

    def get_enable_af_users(self) -> List[Dict[str, str]]:
        try:
            rows = self.db.select("SELECT email, password, account_type FROM af_users")
            return rows
        except Exception as e:
            print(f"[DB ERROR] Failed to fetch users: {e}")
            return []

    def get_user(self, email: str) -> Optional[Dict[str, str]]:
        try:
            rows = self.db.select("SELECT email, password, account_type FROM af_users WHERE email = %s", (email,))
            return rows[0] if rows else None
        except Exception as e:
            print(f"[DB ERROR] Failed to get user {email}: {e}")
            return None

    def save_user(self, email: str, password: str, account_type: str):
        try:
            self.db.execute("""
                INSERT INTO af_users (email, password, account_type)
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    password = VALUES(password),
                    account_type = VALUES(account_type)
            """, (email, password, account_type)
            )
        except Exception as e:
            print(f"[DB ERROR] Failed to save users: {e}")

    def sync_users(self):
        try:
            users = _get_enable_af_users_from_db()  # 请确保此函数返回符合结构的用户列表
            print("[INFO] 已获取用户信息")

            for user in users:
                self.db.execute("""
                    INSERT INTO af_users (email, password, account_type)
                    VALUES (%s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        password = VALUES(password),
                        account_type = VALUES(account_type)
                """, (user["email"], user["password"], user["account_type"]))

            print("[INFO] 同步用户信息完成")
        except Exception as e:
            print(f"[DB ERROR] Failed to sync users: {e}")


class UserAppsStore:
    def __init__(self):
        self.db = _db
        self._init_db()


    def _init_db(self):
        sql = """
        CREATE TABLE IF NOT EXISTS user_apps (
            id INT AUTO_INCREMENT PRIMARY KEY,
            username VARCHAR(255) NOT NULL,
            app_id VARCHAR(255) NOT NULL,
            timezone VARCHAR(64),
            user_type_id VARCHAR(64),
            app_status INT DEFAULT 0,
            created_at DATETIME,
            update_at DATETIME,
            UNIQUE KEY (username, app_id)
        )
        """
        try:
            self.db.execute(sql)
        except Exception as e:
            print(f"[DB ERROR] Failed to init user_apps: {e}")

    def get_non_type_id_users_apps(self) -> List[Dict[str, str]]:
        try:
            rows = self.db.select("SELECT username, app_id, timezone, user_type_id, created_at FROM user_apps where user_type_id = 'PID_None'")
            return rows
        except Exception as e:
            print(f"[DB ERROR] Failed to get_non_type_id_users_apps: {e}")
            return []

    def save_apps_s(self, apps_data: list):
        now_str = timeGen.get_now_str()
        try:
            filtered_data = [(data['username'], data['app_id'], data['timezone'], data['user_type_id'], now_str, now_str)
                     for data in apps_data
                     if data['timezone'] is not None and data['user_type_id'] is not None]
            self.db.executemany("""
                INSERT INTO user_apps (username, app_id, timezone, user_type_id, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    user_type_id = VALUES(user_type_id),
                    update_at = VALUES(update_at)
            """, filtered_data)
        except Exception as e:
            print(f"[DB ERROR] Failed to save apps: {e}")


    def save_apps(self, apps_data: list):
        now_str = timeGen.get_now_str()
        try:
            self.db.executemany("""
                INSERT INTO user_apps (username, app_id, timezone, user_type_id, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    user_type_id = VALUES(user_type_id),
                    update_at = VALUES(update_at)
            """, [(data['username'], data['app_id'], data['timezone'] , data['user_type_id'], now_str, now_str) for data in apps_data])
        except Exception as e:
            print(f"[DB ERROR] Failed to save apps: {e}")


    def save_app(self, username: str, app_id: str, timezone: str, user_type_id: str):
        now_str = timeGen.get_now_str()
        try:
            self.db.execute("""
                INSERT INTO user_apps (username, app_id, timezone, user_type_id, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    user_type_id = VALUES(user_type_id),
                    update_at = VALUES(update_at)
            """, (username, app_id, timezone, user_type_id, now_str, now_str))
        except Exception as e:
            print(f"[DB ERROR] Failed to save app ({username}, {app_id}): {e}")

    def get_all_apps(self) -> List[Dict[str, str]]:
        try:
            return self.db.select("SELECT username, app_id, timezone, user_type_id, created_at FROM user_apps where user_type_id != 'PID_None' AND app_status = 0")
        except Exception as e:
            print(f"[DB ERROR] Failed to get apps: {e}")
            return []
        
    def get_all_upload_apps(self) -> List[Dict[str, str]]:
        try:
            return self.db.select("SELECT username, app_id, timezone, user_type_id, created_at FROM user_apps where user_type_id != 'PID_None' AND app_status = 0 AND timezone != 'PID_None'")
        except Exception as e:
            print(f"[DB ERROR] Failed to get apps: {e}")
            return []
        
    def get_user_apps(self, username: str) -> List[Dict[str, str]]:
        try:
            return self.db.select(
                "SELECT username, app_id, timezone, user_type_id, created_at FROM user_apps WHERE username = %s AND user_type_id != 'PID_None' AND app_status = 0",
                (username,)
            )
        except Exception as e:
            print(f"[DB ERROR] Failed to get user app list: {e}")
            return []

    def get_user_app(self, username: str, app_id: str) -> Optional[Dict[str, str]]:
        try:
            rows = self.db.select(
                "SELECT username, app_id, timezone, user_type_id, created_at FROM user_apps WHERE username = %s AND app_id = %s AND user_type_id != 'PID_None' AND app_status = 0",
                (username, app_id)
            )
            return rows[0] if rows and rows else None
        except Exception as e:
            print(f"[DB ERROR] Failed to get user app: {e}")
            return None

    def delete_user_apps(self, username: str):
        try:
            self.db.execute("DELETE FROM user_apps WHERE username = %s", (username,))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete apps for {username}: {e}")

    def delete_user_app(self, username: str, app_id: str):
        try:
            self.db.execute("DELETE FROM user_apps WHERE username = %s AND app_id = %s", (username, app_id))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete app ({username}, {app_id}): {e}")

    def clear_all(self):
        try:
            self.db.execute("DELETE FROM user_apps")
        except Exception as e:
            print(f"[DB ERROR] Failed to clear apps: {e}")


class AdbLinkUserAppsStore:

    TABLE_NAME = "af_user_apps"

    def __init__(self):
        self.db = get_db(setting.ADBLINK_DB)

    def save_apps(self, apps_data: list):
        now_str = timeGen.get_now_str()
        try:
            self.db.executemany("""
                INSERT INTO af_user_apps (username, app_id, timezone, user_type_id, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    timezone = VALUES(timezone),
                    user_type_id = VALUES(user_type_id),
                    update_at = VALUES(update_at)
            """, [(data['username'], data['app_id'], data['timezone'] , data['user_type_id'], now_str, now_str) for data in apps_data])
        except Exception as e:
            print(f"[DB ERROR] Failed to save apps: {e}")


class AdbinkAfClickGapStore:

    TABLE_NAME = "af_click_gap"

    def __init__(self):
        self.db = get_db(setting.ADBLINK_DB)

    def save_all(self, datas: list):
        now_str = timeGen.get_now_str()
        try:
            self.db.executemany("""
                INSERT INTO af_click_gap (date, offer_id, pid, prt, app_id, timezone, af_clicks, af_install)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    af_clicks = VALUES(af_clicks),
                    af_install = VALUES(af_install)
            """, [(data['date'], data['offer_id'], data['pid'] , data['prt'], data['app_id'],data['timezone'], data['af_clicks'], data['af_install']) for data in datas])
        except Exception as e:
            print(f"[DB ERROR] Failed to save apps: {e}")


class UserAppsStaticsStore:

    TABLE_NAME = "user_apps_statics"

    CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS user_apps_statics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            offer_id INT NOT NULL,
            username VARCHAR(255) NOT NULL,
            prt VARCHAR(255),
            pid VARCHAR(255),
            date CHAR(10),
            app_id VARCHAR(64) NOT NULL,
            last_clicks BIGINT DEFAULT 0,
            last_installs INT DEFAULT 0,
            timezone VARCHAR(64) NOT NULL,
            created_at DATETIME,
            update_at DATETIME,
            UNIQUE KEY (offer_id, username, app_id)
        )
    """
    def __init__(self):
        self.db = _db
        self._init_db()

    def _init_db(self):
        try:
            self.db.execute(self.CREATE_TABLE_SQL)
        except Exception as e:
            print(f"[DB ERROR] Failed to init {self.TABLE_NAME}: {e}")

    def save_all(self, apps_data: list):
        now_str = timeGen.get_now_str()
        try:
            values = [
                (
                    d['offer_id'], d['username'], d.get('prt'), d.get('pid'),
                    d['app_id'], d.get('last_clicks', 0), d.get('last_installs', 0),
                    d['timezone'], now_str, now_str
                )
                for d in apps_data
            ]
            self.db.executemany(f"""
                INSERT INTO {self.TABLE_NAME} 
                    (offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    last_clicks = VALUES(last_clicks),
                    last_installs = VALUES(last_installs),
                    timezone = VALUES(timezone),
                    update_at = VALUES(update_at)
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to save apps in batch: {e}")

    def save(self, offer_id: int, username: str, prt: str, pid: str,
             app_id: str, last_clicks: int, last_installs: int, timezone: str):
        now_str = timeGen.get_now_str()
        try:
            self.db.execute(f"""
                INSERT INTO {self.TABLE_NAME}
                    (offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, created_at, update_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    last_clicks = VALUES(last_clicks),
                    last_installs = VALUES(last_installs),
                    timezone = VALUES(timezone),
                    update_at = VALUES(update_at)
            """, (offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, now_str, now_str))
        except Exception as e:
            print(f"[DB ERROR] Failed to save app ({username}, {app_id}): {e}")

    def get_all(self) -> List[Dict[str, str]]:
        try:
            return self.db.select(f"""
                SELECT offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, created_at 
                FROM {self.TABLE_NAME}
            """)
        except Exception as e:
            print(f"[DB ERROR] Failed to get all apps: {e}")
            return []
        
    def get_update_all(self) -> List[Dict[str, str]]:
        try:
            return self.db.select(f"""
                SELECT offer_id, prt, pid, app_id, last_clicks as af_clicks, last_installs as af_install, timezone, update_at as date
                FROM {self.TABLE_NAME}
            """)
        except Exception as e:
            print(f"[DB ERROR] Failed to get all apps: {e}")
            return []
        
    def get_user_all(self, username: str) -> List[Dict[str, str]]:
        try:
            return self.db.select(f"""
                SELECT offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, created_at 
                FROM {self.TABLE_NAME} 
                WHERE username = %s
            """, (username,))
        except Exception as e:
            print(f"[DB ERROR] Failed to get apps for user {username}: {e}")
            return []

    def get_user_app(self, username: str, app_id: str) -> Optional[Dict[str, str]]:
        try:
            rows = self.db.select(f"""
                SELECT offer_id, username, prt, pid, app_id, last_clicks, last_installs, timezone, created_at 
                FROM {self.TABLE_NAME}
                WHERE username = %s AND app_id = %s
            """, (username, app_id))
            return rows[0] if rows else None
        except Exception as e:
            print(f"[DB ERROR] Failed to get app ({username}, {app_id}): {e}")
            return None

    def delete_user(self, username: str):
        try:
            self.db.execute(f"DELETE FROM {self.TABLE_NAME} WHERE username = %s", (username,))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete apps for {username}: {e}")

    def delete_user_app(self, username: str, app_id: str):
        try:
            self.db.execute(f"DELETE FROM {self.TABLE_NAME} WHERE username = %s AND app_id = %s", (username, app_id))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete app ({username}, {app_id}): {e}")

    def clear_all(self):
        try:
            self.db.execute(f"DELETE FROM {self.TABLE_NAME}")
        except Exception as e:
            print(f"[DB ERROR] Failed to clear {self.TABLE_NAME}: {e}")


import csv

class UserAppTask:

    TABLE_NAME = "user_app_task"

    CREATE_TABLE_SQL = """
        CREATE TABLE IF NOT EXISTS user_app_task (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(255),
                    app_id VARCHAR(255),
                    status VARCHAR(32) DEFAULT 'pending',
                    reason TEXT,
                    start_at DATETIME,
                    create_at DATETIME,
                    update_at DATETIME,
                    finish_at DATETIME,
                    death_at DATETIME
                )
    """

    def __init__(self):
        self.db = _db
        self._create_table()

    def _create_table(self):
        try:
            self.db.execute(UserAppTask.CREATE_TABLE_SQL)
        except Exception as e:
            print(f"[DB ERROR] Failed to create user_app_task table: {e}")

    def add_task(self, username: str, start_at: str):
        try:
            now = timeGen.get_now_str()
            death_at = timeGen.get_now_date_with_give_clock_time(setting.TASK_DEATH_TIME)
            self.db.execute(f"""
                INSERT INTO {self.TABLE_NAME} 
                (username, start_at, status, create_at, update_at, finish_at, death_at)
                VALUES (%s, %s,  'pending', %s, %s, %s, %s)
            """, (username, start_at, now, now, now, death_at))
        except Exception as e:
            print(f"[DB ERROR] Failed to add task: {e}")

    def fail_task(self, task_id: int, reason: str):
        try:
            now = timeGen.get_now_str()
            self.db.execute(f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'fail', reason = %s, update_at = %s, finish_at = %s
                WHERE id = %s
            """, (reason, now, now, task_id))
        except Exception as e:
            print(f"[DB ERROR] Failed to mark task {task_id} as failed: {e}")

    def mark_done(self, task_id: int):
        try:
            now = timeGen.get_now_str()
            self.db.execute(f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'done', update_at = %s, finish_at = %s
                WHERE id = %s
            """, (now, now, task_id))
        except Exception as e:
            print(f"[DB ERROR] Failed to mark task {task_id} as done: {e}")

    def delay_task(self, task_id: int, delay_seconds: int):
        try:
            update_now = timeGen.get_now_str()
            delay_start_at = timeGen.after_now(seconds=delay_seconds)
            self.db.execute(f"""
                UPDATE {self.TABLE_NAME}
                SET start_at = %s, update_at = %s
                WHERE id = %s
            """, (delay_start_at, update_now, task_id))
        except Exception as e:
            print(f"[DB ERROR] Failed to delay task {task_id}: {e}")


    def add_tasks(self, tasks: List[Dict]):
        try:
            now = timeGen.get_now_str()
            death_at = timeGen.get_now_date_with_give_clock_time(setting.TASK_DEATH_TIME)
            values = [
                (
                    task["username"],
                    task["start_at"],
                    now,
                    now,
                    now,
                    death_at,
                )
                for task in tasks
            ]
            self.db.executemany(f"""
                INSERT INTO {self.TABLE_NAME}
                (username, start_at, status, create_at, update_at, finish_at, death_at)
                VALUES (%s, %s, 'pending', %s, %s, %s, %s)
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to add tasks in batch: {e}")

    def add_tasks_a(self, tasks: List[Dict]):
        try:
            now = timeGen.get_now_str()
            death_at = timeGen.get_now_date_with_give_clock_time(setting.TASK_DEATH_TIME)
            values = [
                (
                    task["username"],
                    task["app_id"],
                    task["start_at"],
                    now,
                    now,
                    now,
                    death_at,
                )
                for task in tasks
            ]
            self.db.executemany(f"""
                INSERT INTO {self.TABLE_NAME}
                (username, app_id, start_at, status, create_at, update_at, finish_at, death_at)
                VALUES (%s, %s, %s, 'pending', %s, %s, %s, %s)
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to add tasks in batch: {e}")

    def fail_tasks(self, task_updates: List[Dict]):
        try:
            now = timeGen.get_now_str()
            values = [(task["reason"], now, now, task["id"]) for task in task_updates]
            self.db.executemany(f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'fail', reason = %s, update_at = %s, finish_at = %s
                WHERE id = %s
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to fail tasks in batch: {e}")

    def mark_done_tasks(self, task_ids: List[int]):
        try:
            now = timeGen.get_now_str()
            values = [(now, now, task_id) for task_id in task_ids]
            self.db.executemany(f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'done', update_at = %s, finish_at = %s
                WHERE id = %s
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to mark tasks done in batch: {e}")

    def delay_tasks(self, task_delays: List[Dict]):
        try:
            update_now = timeGen.get_now_str()
            values = []
            for task in task_delays:
                delay_time = timeGen.after_now(seconds=task["delay_seconds"])
                values.append((task["reason"], delay_time, update_now, task["id"]))
            self.db.executemany(f"""
                UPDATE {self.TABLE_NAME}
                SET reason=%s, start_at = %s, update_at = %s
                WHERE id = %s
            """, values)
        except Exception as e:
            print(f"[DB ERROR] Failed to delay tasks in batch: {e}")

    def delay_task_username(self, username: str, delay_seconds: int):
        try:
            update_now = timeGen.get_now_str()
            delay_start_at = timeGen.after_now(seconds=delay_seconds)
            self.db.execute(f"""
                UPDATE {self.TABLE_NAME}
                SET start_at = %s, update_at = %s
                WHERE username = %s
            """, (delay_start_at, update_now, username))
        except Exception as e:
            print(f"[DB ERROR] Failed to delay task {username}: {e}")

    def delete_with_ids(self, task_ids: List[int]):
        try:
            values = [(task_id,) for task_id in task_ids]
            self.db.executemany(f"DELETE FROM {self.TABLE_NAME} WHERE id = %s", values)
        except Exception as e:
            print(f"[DB ERROR] Failed to delete tasks in batch: {e}")

    def get_enable_tasks(self) -> List[Dict]:
        try:
            return self.db.select(f"""
                SELECT * FROM {self.TABLE_NAME}
                WHERE status = 'pending' AND death_at >= start_at
                ORDER BY start_at ASC
            """)
        
        except Exception as e:
            print(f"[DB ERROR] Failed to get_near_fature_task tasks: {e}")
            return None

    def get_pending_tasks(self, username: str, limit: int = 100) -> List[Dict]:
        """获取当前可执行任务"""
        try:
            now_str = timeGen.get_now_str()
            return self.db.select(f"""
                SELECT * FROM {self.TABLE_NAME}
                WHERE status = 'pending' AND username = %s AND start_at <= %s AND death_at >= start_at
                ORDER BY update_at ASC
                LIMIT %s
            """, (username, now_str, limit))
        except Exception as e:
            print(f"[DB ERROR] Failed to get pending tasks: {e}")
            return []

    def reset_fail_task(self):
        try:
            now_str = timeGen.get_now_str()
            self.db.execute(f"""
                UPDATE {self.TABLE_NAME}
                SET status = 'pending', update_at = %s
                WHERE status = 'fail'
            """,(now_str,))
            return True
        except Exception as e:
            print(f"[DB ERROR] Failed to reset_fail_task: {e}")
            return False

    def delete_with_id(self, task_id: int):
        try:
            self.db.execute(f"DELETE FROM {self.TABLE_NAME} WHERE id = %s", (task_id,))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete task {task_id}: {e}")

    def export_fail_tasks(self, file_path: str = "fail_tasks.csv"):
        try:
            rows = self.db.select("""
                SELECT id, username, app_id, status, reason, start_at, create_at, update_at, finish_at, death_at
                FROM user_app_task
                WHERE status = 'fail'
            """)
            if not rows:
                print("[INFO] No failed tasks found.")
                return

            # 获取字段名（假设返回的是 dict）
            fieldnames = rows[0].keys() if isinstance(rows[0], dict) else []

            with open(file_path, mode='w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            print(f"[INFO] Exported {len(rows)} failed tasks to {file_path}")
        except Exception as e:
            print(f"[DB ERROR] Failed to get fail tasks: {e}")
            return None
        
    def clear(self):
        try:
            self.db.execute(f"DELETE FROM {self.TABLE_NAME}")
        except Exception as e:
            print(f"[DB ERROR] Failed to clear: {e}")

class CookieStore:
    def __init__(self):
        self.db = _db
        self._create()

    def _create(self):
        """初始化 cookies 表结构"""
        sql = """
        CREATE TABLE IF NOT EXISTS cookies (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(255) UNIQUE NOT NULL,
            cookie TEXT,
            proxy TEXT,
            login_count INT DEFAULT 0,
            created_at DATETIME,
            updated_at DATETIME
        )
        """
        try:
            self.db.execute(sql)
        except Exception as e:
            print(f"[DB ERROR] Failed to create cookies table: {e}")


    def save_cookie_proxy(self, source: str, cookie: str, proxy: str):
        now = timeGen.get_now_str()
        try:
            self.db.execute("""
                INSERT INTO cookies (source, cookie, proxy, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    cookie = VALUES(cookie),
                    proxy = VALUES(proxy),
                    updated_at = VALUES(updated_at)
            """, (source, cookie, proxy, now, now))
        except Exception as e:
            print(f"[DB ERROR] Failed to save cookie: {e}")

    def get_login_count(self, source: str) -> int:
        try:
            rows = self.db.select("SELECT login_count FROM cookies WHERE source = %s", (source,))
            return rows[0]["login_count"] if rows else 0
        except Exception as e:
            print(f"[DB ERROR] Failed to get login count: {e}")
            return 0

    def set_login_count(self, source: str, count: int):
        try:
            self.db.execute("UPDATE cookies SET login_count = %s WHERE source = %s", (count, source))
        except Exception as e:
            print(f"[DB ERROR] Failed to set login count: {e}")

    def increment_login_count(self, source: str):
        try:
            self.db.execute("""
                UPDATE cookies
                SET login_count = login_count + 1
                WHERE source = %s
            """, (source,))
        except Exception as e:
            print(f"[DB ERROR] Failed to increment login count: {e}")

    @staticmethod
    def IsExpire(created_at_str: str):
        delta = timeGen.seconds_between(created_at_str, timeGen.get_now_str())
        return delta > setting.COOKIES_EXPIRE_TIME or setting.COOKIES_EXPIRE_TIME - delta < setting.COOKIES_MIN_TIME

    def get_cookie_proxy(self, source: str) -> Tuple[Optional[str], Optional[str], int]:
        try:
            rows = self.db.select("""
                SELECT cookie, created_at, proxy, login_count
                FROM cookies
                WHERE source = %s
            """, (source,))
            if not rows:
                return None, None, 0

            row = rows[0]
            if self.IsExpire(row["created_at"]):
                self.delete_cookie(source)
                return None, None, 0

            return row["cookie"], row["proxy"], row["login_count"]
        except Exception as e:
            print(f"[DB ERROR] Failed to get cookie: {e}")
            return None, None, 0

    def delete_cookie(self, source: str):
        try:
            self.db.execute("DELETE FROM cookies WHERE source = %s", (source,))
        except Exception as e:
            print(f"[DB ERROR] Failed to delete cookie: {e}")

    def clear_all(self):
        try:
            self.db.execute("DELETE FROM cookies")
        except Exception as e:
            print(f"[DB ERROR] Failed to clear cookies: {e}")


cookies = CookieStore()
user_apps = UserAppsStore()
user_apps_statics = UserAppsStaticsStore()
users = UserStore()
user_apps_tasks = UserAppTask()
# user_apps_static_tasks = UserAppStaticsTask()

if __name__ == "__main__":
    user_apps_tasks._create_table()
    # users.sync_users()
    # update_table(CookieStore.TABLE_NAME, CookieStore.CREATE_TABLE_SQL)
    # update_table(UserAppsStore.TABLE_NAME, UserAppsStore.CREATE_TABLE_SQL)
    # update_table(TaskProgressManager.TABLE_NAME, TaskProgressManager.CREATE_TABLE_SQL, setting.TASK_DB_NAME)
    # tasks.export_to_file("tasks_click_gap_task.csv", "click_gap_task")
    # cookies.expore_file("cookies.csv")
    # user_apps.export_to_file("apps.csv")
    # print(CookieStore.IsExpire("2023-10-10 10:10:10")) 
    # tasks._drop()
    # tasks._create_table()
    # data = tasks.get_pending_tasks("sync_user_apps")
    # print(len(data))
    # tasks.export_to_file("tasks.csv")
    # users.export_to_file("users.csv")
    # try:
    #     users.sync_users()
    # except Exception as e:
    #     print(f"[ERROR] Failed during main sync: {e}")
    pass