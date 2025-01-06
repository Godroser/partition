import math
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

##根据sql本身的物理执行计划,找到tablescan算子对应的表,然后在现有分区配置下估计tablescan需要扫描到的数据量,最后修改ch_query_params里的Qparams类的变量

##建立保存表分区元数据的类
##每个表选定分区键之后默认四个分区
class Customer_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM customer;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM customer WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]


class District_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 40 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM district;") 
        self.count = cur.fetchall()[0][0]
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM district WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]

class History_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 124913 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:   
        cur.execute("SELECT count(*) FROM history;") 
        self.count = cur.fetchall()[0][0]
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM customer WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]


class Item_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 100000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  # 这是针对单列的,已废弃
  # def update_partition_metadata(self, keys, ranges):
  #   for i in range(len(ranges)-1):
  #     self.partition_range[i] = ranges[i]

  #   self.partition_cnt[len(ranges)-1] = self.count
  #   with get_connection(autocommit=False) as connection:
  #     with connection.cursor() as cur:    
  #       for i in range(len(ranges)-1):
  #         self.partition_range[i] = ranges[i][0]
  #         print("SELECT count(*) FROM item where {} < {};".format(keys[0], ranges[i][0]))
  #         cur.execute("SELECT count(*) FROM item where {} < {};".format(keys[0], ranges[i][0]))
  #         result = cur.fetchall()[0][0]
  #         print(result)
  #         if i==0:
  #           self.partition_cnt[i] = result
  #         elif i==1:
  #           self.partition_cnt[i] = result - self.partition_cnt[i-1]
  #         else:
  #           self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]
  #         self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM item;") 
        self.count = cur.fetchall()[0][0]              
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM item WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]


class Nation_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM nation;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM nation WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]

class New_Order_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM new_order;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM new_order WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]

class Order_Line_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM order_line;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM order_line WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]


class Orders_Meta:
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM orders;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM orders WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]

class Region_Meta:  
  def __init__(self):
    self.ispartition = True #True: use partition_metadata, False:full table scan
    self.keys = [] # partition keys
    self.count = 120000 # count(*) 
    # each partition tuple cnt
    self.partition_cnt = [0]*4
    # each partition range end value
    self.partition_range = [0]*4

  def update_partition_metadata(self, keys, ranges):
    for i in range(len(keys)):
      self.keys.append(keys[i])
    for i in range(len(ranges)):
      self.partition_range[i] = ranges[i]
    
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        cur.execute("SELECT count(*) FROM region;") 
        self.count = cur.fetchall()[0][0]        
        self.partition_cnt[len(ranges)-1] = self.count
        for i in range(len(ranges)-1):
          # 构建 WHERE 条件，支持多个列
          conditions = []
          for key_idx, key in enumerate(keys):
            conditions.append(f"{key} < {ranges[i][key_idx]}")  # 构建条件，支持多个列

          # 拼接 SQL 查询
          query = "SELECT count(*) FROM region WHERE " + " AND ".join(conditions) + ";"
          #print(f"Executing query: {query}")
          
          # 执行查询
          cur.execute(query)
          result = cur.fetchall()[0][0]
          #print(f"Result: {result}")

          # 更新 partition_cnt 数组
          if i == 0:
            self.partition_cnt[i] = result
          elif i == 1:
            self.partition_cnt[i] = result - self.partition_cnt[i-1]
          else:
            self.partition_cnt[i] = result - self.partition_cnt[i-1] - self.partition_cnt[i-2]

          # 更新最后一个 partition 的计数
          self.partition_cnt[len(ranges)-1] -= self.partition_cnt[i]


##根据每个表，和表分区元数据，估算每个表在不同过滤条件下的基数

#def get_q1_card():

if __name__ == "__main__":
  item_meta = Item_Meta()
  #print(item_meta.partition_range)
  ranges =  [[25000,2500], [50000,5000], [75000,7500], [math.inf,math.inf]]
  keys = ['i_id', 'i_im_id']
  item_meta.update_partition_metadata(keys, ranges)
  print("partition keys: ", item_meta.keys)
  print("partition_cnt: ", item_meta.partition_cnt)
  print("partition_range: ", item_meta.partition_range)
