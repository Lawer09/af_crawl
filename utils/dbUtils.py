import logging
import mysql.connector
from mysql.connector import Error

class MysqlDBHelper:
    def __init__(self, host, port, username, password, database):
        try:
            self.conn = mysql.connector.connect(
                host=host,
                port=port,
                username=username,
                password=password,
                database=database
            )
            self.cursor = self.conn.cursor(dictionary=True)
        except Error as e:
            logging.error(f"数据库连接失败: {e}")
            raise

    def update_with_filters(self, table_name, update_data: dict, filters: dict):
        try:
            if not update_data:
                raise ValueError("更新内容不能为空")
            if not filters:
                raise ValueError("更新操作必须提供过滤条件，防止更新所有数据")

            set_clause = ", ".join([f"{key} = %s" for key in update_data])
            where_clause = " AND ".join([f"{key} = %s" for key in filters])
            sql = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"

            params = list(update_data.values()) + list(filters.values())

            self.cursor.execute(sql, params)
            self.conn.commit()
        except Error as e:
            logging.error(f"更新数据失败: {e}")

    def select_with_filters(self, table_name, filters: dict):
        try:
            if filters:
                where_clause = " AND ".join([f"{key} = %s" for key in filters])
                sql = f"SELECT * FROM {table_name} WHERE {where_clause}"
                params = list(filters.values())
            else:
                sql = f"SELECT * FROM {table_name}"
                params = []

            self.cursor.execute(sql, params)
            return self.cursor.fetchall()
        except Error as e:
            logging.error(f"查询数据失败: {e}")
            return []

    def query(self, table_name, where_param):
        try:
            sql = f"SELECT * FROM {table_name} WHERE {where_param}"
            self.cursor.execute(sql)
            return self.cursor.fetchall()
        except Error as e:
            logging.error(f"查询数据失败: {e}")
            return []
        
    def insert_multiple_with_data(self, table_name, data_list: list):
        try:
            if not data_list:
                logging.error("插入数据列表不能为空")
                raise ValueError("插入数据列表不能为空")

            columns = ", ".join(data_list[0].keys())
            
            placeholders = ", ".join(["%s"] * len(data_list[0]))

            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

            params = [tuple(data.values()) for data in data_list]

            self.cursor.executemany(sql, params)
            self.conn.commit()
        except Error as e:
            logging.error(f"批量插入数据失败: {e}")

    def delete_with_filters(self, table_name, filters: dict):
        try:
            if not filters:
                raise ValueError("删除操作必须提供过滤条件，防止误删所有数据")

            where_clause = " AND ".join([f"{key} = %s" for key in filters])
            sql = f"DELETE FROM {table_name} WHERE {where_clause}"
            params = list(filters.values())

            self.cursor.execute(sql, params)
            self.conn.commit()
        except Error as e:
            logging.error(f"删除数据失败: {e}")

    def insert_with_data(self, table_name, data: dict):
        try:
            if not data:
                logging.error("插入数据不能为空")
                raise ValueError("插入数据不能为空")

            columns = ", ".join(data.keys())
            placeholders = ", ".join(["%s"] * len(data))
            sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            params = list(data.values())

            self.cursor.execute(sql, params)
            self.conn.commit()
        except Error as e:
            logging.error(f"插入数据失败: {e}")

    def close(self):
        self.cursor.close()
        self.conn.close()
        logging.info("数据库连接已关闭")


# 实例化数据库对象
db = MysqlDBHelper(
    host="rm-0xitsd6nu26k8z5z9oo.mysql.rds.aliyuncs.com",
    port=3306,
    username="adbink",
    password="YiQoRi1afo&e8wAneglz",
    database="adbink"
)
