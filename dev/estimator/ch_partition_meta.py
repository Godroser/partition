import math
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import get_connection, Config

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

##根据sql本身的物理执行计划,找到tablescan算子对应的表,然后在现有分区配置下估计tablescan需要扫描到的数据量,最后修改ch_query_params里的Qparams类的变量

##建立保存表分区元数据的类
##每个表选定分区键之后默认四个分区
class Customer_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica = False #True: use row-store, is table_meta; False: use column-store, is table_replica_meta
    self.keys = [] # partition keys []
    self.count = 120000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM customer;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "c_id": [35203, 35250, 34970, 34546],
      "c_d_id": [30000, 30000, 30000, 49969],
      "c_w_id": [0, 49969, 30000, 60000],
      "c_payment_cnt": [134604, 5005, 321, 39]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM customer;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM customer WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class District_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 40 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM district;") 
    #     self.count = cur.fetchall()[0][0]     
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "d_id": [8, 8, 8, 16],
      "d_w_id": [0, 10, 10, 20],
      "d_next_o_id": [5, 10, 10, 15]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM district;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM district WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class History_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 124913 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM history;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "h_c_id": [32079, 31268, 30849, 30717],
      "h_c_d_id": [24978, 25007, 24992, 49936],
      "h_c_w_id": [0, 31248, 31226, 62439],
      "h_d_id": [24993, 24999, 24999, 49922],
      "h_w_id": [0, 31247, 31233,62433],
      "h_date": [123883, 0, 0, 1030]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM history;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM history WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Item_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 100000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM item;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "i_id": [24499, 25000, 25000, 25001],
      "i_im_id": [24990, 24957, 25002, 25051]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM item;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM item WHERE " + " AND ".join(conditions) + ";"
          # print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Nation_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 120000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM nation;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "n_nationkey": [6,6,6,7],
      "n_regionkey": [5,5,5,10]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM nation;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM nation WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class New_Order_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 120000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM new_order;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "no_o_id": [9100, 9480, 9480, 8358],
      "no_d_id": [7299, 7278, 7342, 14499],
      "no_w_id": [0, 8986, 9201, 18231]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM new_order;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM new_order WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Order_Line_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 1250435 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM order_line;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "ol_o_id": [312994, 314379, 315736, 307326],
      "ol_d_id": [250093, 251197, 250023, 499122],
      "ol_w_id": [0, 312935, 312604, 624896],
      "ol_number": [375114, 363788, 272344, 239189],
      "ol_i_id": [311951, 313776, 312130, 312578],
      "ol_supply_w_id": [0, 312932, 312605, 624898],
      "ol_quantity": [9969, 10098, 1210137,20231],
      "ol_delivery_d": [437420, 438568, 4465,4609]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM order_line;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM order_line WHERE " + " AND ".join(conditions) + ";"
          # print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Orders_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 120000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM orders;") 
    #     self.count = cur.fetchall()[0][0]        
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "o_id": [31400, 31480, 31480, 30638],
      "o_d_id": [25023, 25002, 25066, 49947],
      "o_w_id": [9, 31276, 31261, 62501],
      "o_c_id": [31301, 31339, 31397, 31001],
      "o_entry_d": [62012, 61956, 525, 545],
      "o_carrier_id": [17541, 17792, 17856, 35431],
      "o_ol_cnt": [0, 22870, 34050, 68118],
      "o_all_local": [0, 0, 451, 124587]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM orders;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM orders WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Region_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 5 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM region;") 
    #     self.count = cur.fetchall()[0][0]        
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "r_regionkey": [1, 1, 1, 2]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM region;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM region WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Stock_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 400000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM stock;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "s_i_id": [99996, 100000, 100000, 100004],
      "s_w_id": [0, 100000, 100000, 200000],
      "s_ytd": [399233, 681, 72, 14],
      "s_order_cnt": [398970, 910, 97, 23],
      "s_remote_cnt": [0, 0, 399534, 466]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM stock;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM stock WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Supplier_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 10000 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM supplier;") 
    #     self.count = cur.fetchall()[0][0]         
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "s_suppkey": [2499, 2500, 2500, 2501],
      "s_nationkey": [2437, 2449, 2315, 2799]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM supplier;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM supplier WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

class Warehouse_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.isreplica =False #True: use row-store, False: use column-store
    self.keys = [] # partition keys []
    self.count = 4 # count(*) 
    # with get_connection(autocommit=False) as connection:
    #   with connection.cursor() as cur:    
    #     cur.execute("SELECT count(*) FROM warehouse;") 
    #     self.count = cur.fetchall()[0][0]      
    # each partition tuple cnt []
    self.partition_cnt = []
    # each partition range end value [[],[]]
    self.partition_range = []
    # 新增：每个partitionable_columns的分区范围，手动填写
    self.keys_partition_cnt = {
      "w_id": [0, 1, 1, 2]
    }

  # 更新meta类的keys partition_cnt partition_range
  # keys: [] ranges: [[],[]] ranges[i]:第i个分区键的范围
  def update_partition_metadata(self, keys, ranges):
    self.keys.clear()
    self.partition_range = []
    self.partition_cnt = []

    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range.append(ranges[i])
    
    # 新增：直接用keys_partition_cnt
    if all(key in self.keys_partition_cnt for key in keys):
      for key in keys:
        self.partition_cnt = self.keys_partition_cnt[key]
      return

    # 原有数据库查询代码，已注释
    """
    # print("ranges: ", ranges)
    # print("partition_range: ", self.partition_range)

    # 根据keys和ranges,查询每个分区的tuple数量
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM warehouse;") 
        self.count = cur.fetchall()[0][0]     

        #遍历每个分区
        for i in range(len(ranges[0])):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            # 构建条件，支持多个列
            value = ranges[key_idx][i]
            if isinstance(value, datetime):
              value = f"'{value}'"
            conditions.append(f"{key} <= {value}")

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM warehouse WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          self.partition_cnt.append(result)
          for j in range(i):
            self.partition_cnt[i] -= self.partition_cnt[j]
    """

##根据每个表，和表分区元数据，估算每个表在不同过滤条件下的基数

if __name__ == "__main__":
  customer_meta = Customer_Meta()
  district_meta = District_Meta()
  history_meta = History_Meta()
  item_meta = Item_Meta()
  nation_meta = Nation_Meta()
  new_order_meta = New_Order_Meta()
  order_line_meta = Order_Line_Meta()
  orders_meta = Orders_Meta()
  region_meta = Region_Meta()
  stock_meta = Stock_Meta()
  supplier_meta = Supplier_Meta()
  warehouse_meta = Warehouse_Meta()
 
  #print(item_meta.partition_range)
  datetime(2024, 10, 24, 17, 0, 0)
  ranges = [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)]]
  keys = ['ol_delivery_d']
  order_line_meta.update_partition_metadata(keys, ranges)
  print("partition keys: ", order_line_meta.keys)
  print("partition_cnt: ", order_line_meta.partition_cnt)
  print("partition_range: ", order_line_meta.partition_range)

  keys = ['c_id', 'c_d_id']
  ranges = [[750, 1500, 2250, 3001], [2, 4, 6, 11]]
  customer_meta.update_partition_metadata(keys, ranges)
  print("partition keys: ", customer_meta.keys)
  print("partition_cnt: ", customer_meta.partition_cnt)
  print("partition_range: ", customer_meta.partition_range)

  ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)]]
  keys = ['o_entry_d']
  orders_meta.update_partition_metadata(keys, ranges)   
  print("partition keys: ", orders_meta.keys)
  print("partition_cnt: ", orders_meta.partition_cnt)
  print("partition_range: ", orders_meta.partition_range)   
