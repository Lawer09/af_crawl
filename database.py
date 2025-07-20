from mysql.connector import connect, Error
from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
from typing import List, Dict, Tuple, Optional

class MySqlDatabase:
    def __init__(self, db_setting):
        try:
            self.setting = db_setting
            self.connection = None
            self.connect()
        except Error as e:
            print(f"[MySQL ERROR] Failed to initialize connection pool: {e}")
            raise

    def connect(self):
        try:
            self.connection = connect(
                host=self.setting["host"],
                port=self.setting["port"],
                user=self.setting["user"],
                password=self.setting["password"],
                database=self.setting["database"],
                charset='utf8mb4'
            )
        except Error as e:
            print(f"[MySQL ERROR] Failed to initialize database connection: {e}")
            raise

    def get_conn(self):
        if not self.connection or not self.connection.is_connected():
            self.connect()
        return self.connection

    def select(self, query: str, params: Tuple = ()) -> List[Dict]:
        """执行 SELECT 查询，返回字典列表"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params)
            result = cursor.fetchall()
            return result
        except Error as e:
            print(f"[MySQL ERROR] Failed to execute select: {e}\nSQL: {query}")
            return []
        finally:
            cursor.close()
            conn.close()

    def execute(self, query: str, params: Tuple = ()):
        """执行 INSERT / UPDATE / DELETE 等操作"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()
        except Error as e:
            conn.rollback()
            print(f"[MySQL ERROR] Failed to execute query: {e}\nSQL: {query}")
            raise
        finally:
            cursor.close()
            conn.close()

    def executemany(self, query: str, param_list: List[Tuple]):
        """批量执行（如批量插入）"""
        conn = self.get_conn()
        try:
            cursor = conn.cursor()
            cursor.executemany(query, param_list)
            conn.commit()
        except Error as e:
            conn.rollback()
            print(f"[MySQL ERROR] Failed to execute many: {e}\nSQL: {query}")
            raise
        finally:
            cursor.close()
            conn.close()

    def fetch_one(self, query: str, params: Tuple = ()) -> Optional[Dict]:
        """获取单条记录"""
        rows = self.select(query, params)
        return rows[0] if rows else None
