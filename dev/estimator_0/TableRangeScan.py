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
      

def transform_into_ms(time_str):
  # 匹配带有单位的数字
  pattern = re.compile(r'(\d+(?:\.\d+)?)(ms|m|s)')
  total_sum = 0

  # 遍历每个匹配到的数字和单位
  for match in pattern.findall(time_str):
      value, unit = match
      value = float(value)
      # 根据单位进行加权计算
      if unit == 'm':
          total_sum += value * 60000
      elif unit == 's':
          total_sum += value * 1000
      elif unit == 'ms':
          total_sum += value
  return total_sum      
def extract_time(s): ## return in ms
    pattern = r'time:([^,]+)'
    match = re.search(pattern, s)
    if match:
        print(match.group(1))  #initial time
        time = transform_into_ms(match.group(1))  #time in ms
        return time
    return None
def test_in_db(sql,op):  ## execute explain analyze in db
  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur:
      cur.execute(sql)
      context = cur.fetchall()
      #return context[op]
      selectivity = float(context[op][2]) / float(context[op+2][2])

      return extract_time(context[op][5]), context[op][2], selectivity

def multi_test_in_db(N, sql, op):
  card = 0
  latency = 0.0
  cnt = 0
  for i in range(N):
    time, card, selectivity = test_in_db(sql,op)
    if i > 3:
      latency += float(time)
      cnt += 1
  latency = latency / cnt
  print("{}".format(round(latency, 2)), card, selectivity)
  return round(latency, 2), card, selectivity


def init_test_in_db(sql): ##fetch all
  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur:
      cur.execute(sql)
      context = cur.fetchall()
      return context

if __name__ == "__main__":
  N = 10
  
  sql = "explain analyze select id,coins from players where coins<4000;"
  op = 0 ## TableReader_16

  #print(init_test_in_db(sql))  #看算子输出
  print(test_in_db(sql,op))  #看时间计算
  #latency, card, selectivity = multi_test_in_db(N, sql, op)





