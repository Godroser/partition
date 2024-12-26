import sys
import os
import re
import csv
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
      
def extract_time(s):
    pattern = r'time:(\d+(\.\d+)?)(ms)?'
    match = re.search(pattern, s)
    if match:
        return match.group(1)
    return None
def test_in_db(sql,op):
  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur:
      cur.execute(sql)
      context = cur.fetchall()
      #return context[op]
      #selectivity = context[op][2] / context[op][3]
      return extract_time(context[op][5]), context[op][2]

def multi_test_in_db(N, sql, op):
  card = 0
  latency = 0.0
  cnt = 0
  for i in range(N):
    time, card = test_in_db(sql,op)
    if i > 3:
      latency += float(time)
      cnt += 1
  latency = latency / cnt
  print("{}".format(round(latency, 2)), card)
  return round(latency, 2), card


def init_test_in_db(sql): ##fetch all
  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur:
      cur.execute(sql)
      context = cur.fetchall()
      return context

if __name__ == "__main__":
  N = 10
  

  sql = "explain analyze select coins,goods from players where coins>100;"
  op = 0 ## TableReader_16
  #print(init_test_in_db(sql))
  latency, card = multi_test_in_db(N, sql, op)





# # 将结果写入CSV文件
#   csv_file_path = "results.csv"
#   with open(csv_file_path, mode='a', newline='') as file:
#     writer = csv.writer(file)
#     #writer.writerow(["Latency", "Card"])  # 写入表头
#     if latency is not None and card is not None:
#       writer.writerow([latency, card])  