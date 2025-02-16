from datetime import datetime, timedelta
import random
import sys
import os

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": config.TIDB_DB_NAME,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True
    }

    if config.ca_path:
        db_conf["ssl_verify_cert"] = True
        db_conf["ssl_verify_identity"] = True
        db_conf["ssl_ca"] = config.ca_path
    return mysql.connector.connect(**db_conf)

# 生成指定范围内的随机日期时间
def generate_random_datetime():
    start_time = datetime.strptime('2024-10-23 17:00:00', '%Y-%m-%d %H:%M:%S')
    end_time = datetime.strptime('2024-11-01 15:15:05', '%Y-%m-%d %H:%M:%S')
    time_delta = end_time - start_time
    random_seconds = random.randint(0, time_delta.total_seconds())
    return start_time + timedelta(seconds=random_seconds)

if __name__ == "__main__":
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:   
        # 查询 orders 表中的所有记录
        select_query = "SELECT o_w_id,o_d_id FROM orders"  # 假设表中有 o_id 作为主键
        cur.execute(select_query)
        rows = cur.fetchall()
        print(rows)

        for row in rows:
            o_w_id = row[0]
            o_d_id = row[1]
            random_datetime = generate_random_datetime()
            update_query = f"UPDATE orders SET o_entry_d = '{random_datetime}' WHERE o_w_id = {o_w_id} and o_d_id = {o_d_id}"
            cur.execute(update_query)
