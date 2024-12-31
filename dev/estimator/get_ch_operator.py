import random
import string
import sys
import os
import time

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config
from collector import replica_progresss_collector
from collector import sync_metrics_collector


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

# 读取SQL查询文件
def read_queries_from_file(file_path):
    with open(file_path, 'r') as f:
        queries = f.readlines()
    return [query.strip() for query in queries]

# 执行EXPLAIN ANALYZE并解析结果
def get_explain_analyze(conn, query):
    with conn.cursor() as cursor:
        cursor.execute(f"EXPLAIN ANALYZE {query}")
        result = cursor.fetchall()
    return result

def extract_operators(explain_result):
    operators = set()
    for operator in explain_result:    
        operators.add(operator[0])
        print(operator[0])
    return operators


# 主程序
def main():
    # 读取查询文件
    queries = read_queries_from_file('../workload/workload.bak.sql')
    
    # 创建数据库连接
    with get_connection(autocommit=False) as conn:
        # 遍历每个查询
        for query in queries:
            #print(f"Executing query: {query}")
            explain_result = get_explain_analyze(conn, query)
            operators = extract_operators(explain_result)
            
            # 输出算子名称
            #print(f"Operators for the query: {operators}")

if __name__ == "__main__":
    main()
