import time
import os
import sys

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
    "database": "ch", #config.TIDB_DB_NAME,
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

def read_sqls(sql_file):
    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()
    # 简单分割，假设每条SQL以分号结尾
    sqls = [sql.strip() for sql in content.split(";") if sql.strip()]
    return sqls

def main():
    SQL_FILE = "/data3/dzh/project/grep/dev/workload/workloadd.sql"
    sqls = read_sqls(SQL_FILE)

    # 统计连接和关闭的时间
    conn_start = time.perf_counter()
    conn = get_connection()
    conn_end = time.perf_counter()
    cursor = conn.cursor()
    cursor.close()
    conn.close()
    conn_close = time.perf_counter()
    connect_time = (conn_end - conn_start) + (conn_close - conn_end)
    print(f"connect_time: {conn_end - conn_start:.6f} seconds")
    print(f"cursor_time: {conn_close - conn_end:.6f} seconds")
    print(f"Connect + Close time: {connect_time:.6f} seconds\n")

    # 重新连接用于实际SQL测试
    conn = get_connection()
    cursor = conn.cursor()
    times = []

    for idx, sql in enumerate(sqls, 1):
        explain_sql = f"EXPLAIN {sql}"
        start_time = time.perf_counter()
        cursor.execute(explain_sql)
        result = cursor.fetchall()
        end_time = time.perf_counter()
        elapsed = end_time - start_time
        # 扣除连接-关闭时间
        real_elapsed = max(0.0, elapsed - connect_time)
        times.append(real_elapsed)
        print(f"SQL {idx}: {real_elapsed:.6f} seconds (raw: {elapsed:.6f})")
        # 可选：打印 explain 结果
        # print(result)

    print("\nSummary:")
    for idx, t in enumerate(times, 1):
        print(f"SQL {idx}: {t:.6f} seconds")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()