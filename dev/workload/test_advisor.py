import random
import string
import sys
import os
import time
from datetime import datetime

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
      "database": "ch_bak", #config.TIDB_DB_NAME,
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



class Workload_Parameter:
  def __init__(self):
    self.max_w_id = 4
    self.max_d_id = 10
    self.max_c_id = 3000
    self.max_stock_id = 100000
    self.max_o_id = 3223
    self.max_item = 100000
    self.max_nation = 24
    self.max_supplier_id = 10000
    self.max_region_id = 4
    self.max_ol_cnt = 10 # max item cnt in an order
    self.max_ol_quantity = 10 #max item quatity in an order
    self.max_carrier_id = 10 # max carrier id in delivery txn
    self.max_stock_cnt = 5 # max order cnt in Stock-Level txn
    self.quantity_threshold = 10 # threshold of item quantity in Stock-Level txn

    self.local_new_order_ratio = 0.9 #default 0.9
    self.lcoal_payment_ratio = 0.85 #default 0.85

    self.new_order_ratio = 0.45
    self.payment_ratio = 0.43
    self.order_status_ratio = 0.04
    self.delivery_ratio = 0.04
    self.stock_level_ratio = 0.04

    self.sql_file_path = 'workloadd.sql'
    self.sql_date_min = '2024-10-23 17:00:00'    # used in ap select
    self.sql_date_max = '2025-10-23 17:00:00'
    self.sql_date_mid = '2024-10-28 17:00:00'
    # Default
    # New-Order: 45%
    # Payment: 43%
    # Order-Status: 4%
    # Delivery: 4%
    # Stock-Level: 4% 


class Workload_Statistics:
  def __init__(self):
    self.neworder_cnt = 0
    self.neworder_lat = 0.0
    self.neworder_lat_sum = 0.0    
    self.payment_cnt = 0
    self.payment_lat = 0.0
    self.payment_lat_sum = 0.0
    self.orderstatus_cnt = 0
    self.orderstatus_lat = 0.0
    self.orderstatus_lat_sum = 0.0    
    self.delivery_cnt = 0
    self.delivery_lat = 0.0
    self.delivery_lat_sum = 0.0    
    self.stocklevel_cnt = 0
    self.stocklevel_lat = 0.0
    self.stocklevel_lat_sum = 0.0    

    self.query_cnt = [0] * 22
    self.query_lat = [0.0] * 22
    self.query_lat_sum = [0.0] * 22

wl_stats = Workload_Statistics()

def generate_ap(max_qry_cnt):
  wl_param = Workload_Parameter()
  qry_cnt = 0
  
  with open(wl_param.sql_file_path, 'r') as file:
      sql_script = file.read()
      sqls = [statement.strip() for statement in sql_script.split(';') if statement.strip()]


  while True:  
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        for sql_no in range(1, 23):
          wl_stats.query_cnt[sql_no-1] += 1
          
          start_time = time.time()
          cur.execute(sqls[sql_no-1])
          print("Query {}".format(sql_no), end=' ')
          
          end_time = time.time()
          cur.fetchall()
          delay = end_time - start_time
          print(f"Execution delay: {delay:.6f} seconds")   
          wl_stats.query_lat_sum[sql_no-1] += delay
        
          qry_cnt += 1

        if qry_cnt >= max_qry_cnt:
          break
  
  print("\n")
  print("Summary:")
  
  for i in range(22):
    if wl_stats.query_cnt[i] == 0:
      wl_stats.query_lat[i] = 0
    else:
      wl_stats.query_lat[i] = wl_stats.query_lat_sum[i] / wl_stats.query_cnt[i]
    print("Query {} cnt:{}, latency (avg):{:.6f}s".format(i+1, wl_stats.query_cnt[i], wl_stats.query_lat[i]))

  for i in range(22):
    print(wl_stats.query_lat[i])   

if __name__ == '__main__':
  # connection自定义测试的数据库
  generate_ap(100)