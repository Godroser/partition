import random
import string
import sys
import os
import time
import subprocess

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

def repartition_table(table, sql):
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}';".format(config.TIDB_DB_NAME, table))

          if len(cur.fetchall()) > 0:
              cur.execute('DROP TABLE {};'.format(table))
              print("Drop table {}".format(table))
          cur.execute(sql)
          print("Table {} is created and partitioned!".format(table))

def load_data(table):
  config = Config()
  command = "conda activate prodzh"

  result = subprocess.run(command, shell=True, capture_output=True, text=True)

  # search for matching data files
  data_dir = "/data3/dzh/CH-data"
  prefix = "tpcc." + table + "."
  extension = ".sql"
  matching_files = []
  try:
    # 遍历目录中的文件
    for file_name in os.listdir(data_dir):
      # 检查文件是否符合条件
      if file_name.startswith(prefix) and file_name.endswith(extension):
        matching_files.append(file_name)
  except Exception as e:
      print(f"无法访问目录 {data_dir}: {e}")
  #print(matching_files)

  
  for file in matching_files:
    command = "mysql -h {} -u {} -P {} {} < {}/{}".format(config.TIDB_HOST, config.TIDB_USER, config.TIDB_PORT, config.TIDB_DB_NAME, data_dir, file)
    print(command)
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    #print(result)
  print("Table {} data loaded!".format(table))


def repartition_table_and_load_data(table, sql):
  repartition_table(table, sql)
  load_data(table)



if __name__ == "__main__":
  sql_customer = """
    CREATE TABLE `customer` (
    `c_id` int(11) NOT NULL,
    `c_d_id` int(11) NOT NULL,
    `c_w_id` int(11) NOT NULL,
    `c_first` varchar(16) DEFAULT NULL,
    `c_middle` char(2) DEFAULT NULL,
    `c_last` varchar(16) DEFAULT NULL,
    `c_street_1` varchar(20) DEFAULT NULL,
    `c_street_2` varchar(20) DEFAULT NULL,
    `c_city` varchar(20) DEFAULT NULL,
    `c_state` char(2) DEFAULT NULL,
    `c_zip` char(9) DEFAULT NULL,
    `c_phone` char(16) DEFAULT NULL,
    `c_since` datetime DEFAULT NULL,
    `c_credit` char(2) DEFAULT NULL,
    `c_credit_lim` decimal(12,2) DEFAULT NULL,
    `c_discount` decimal(4,4) DEFAULT NULL,
    `c_balance` decimal(12,2) DEFAULT NULL,
    `c_ytd_payment` decimal(12,2) DEFAULT NULL,
    `c_payment_cnt` int(11) DEFAULT NULL,
    `c_delivery_cnt` int(11) DEFAULT NULL,
    `c_data` varchar(500) DEFAULT NULL,
    KEY `idx_customer` (`c_w_id`,`c_d_id`,`c_last`,`c_first`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
  """

  sql_district = """
    CREATE TABLE `district` (
    `d_id` int(11) NOT NULL,
    `d_w_id` int(11) NOT NULL,
    `d_name` varchar(10) DEFAULT NULL,
    `d_street_1` varchar(20) DEFAULT NULL,
    `d_street_2` varchar(20) DEFAULT NULL,
    `d_city` varchar(20) DEFAULT NULL,
    `d_state` char(2) DEFAULT NULL,
    `d_zip` char(9) DEFAULT NULL,
    `d_tax` decimal(4,4) DEFAULT NULL,
    `d_ytd` decimal(12,2) DEFAULT NULL,
    `d_next_o_id` int(11) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
  """

  sql_history = """
    CREATE TABLE `history` (
    `h_c_id` int(11) NOT NULL,
    `h_c_d_id` int(11) NOT NULL,
    `h_c_w_id` int(11) NOT NULL,
    `h_d_id` int(11) NOT NULL,
    `h_w_id` int(11) NOT NULL,
    `h_date` datetime DEFAULT NULL,
    `h_amount` decimal(6,2) DEFAULT NULL,
    `h_data` varchar(24) DEFAULT NULL,
    KEY `idx_h_w_id` (`h_w_id`),
    KEY `idx_h_c_w_id` (`h_c_w_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
  """

  sql_item = """
    CREATE TABLE `item` (
    `i_id` int(11) NOT NULL,
    `i_im_id` int(11) DEFAULT NULL,
    `i_name` varchar(24) DEFAULT NULL,
    `i_price` decimal(5,2) DEFAULT NULL,
    `i_data` varchar(50) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin 

    PARTITION BY RANGE COLUMNS(i_id)
    (PARTITION `i_p0` VALUES LESS THAN (25000),
    PARTITION `i_p1` VALUES LESS THAN (50000),
    PARTITION `i_p2` VALUES LESS THAN (75000),
    PARTITION `i_p3` VALUES LESS THAN (MAXVALUE));     
  """

  sql_nation = """
    CREATE TABLE `nation` (
    `N_NATIONKEY` bigint(20) NOT NULL,
    `N_NAME` char(25) NOT NULL,
    `N_REGIONKEY` bigint(20) NOT NULL,
    `N_COMMENT` varchar(152) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin

    PARTITION BY RANGE COLUMNS(N_NATIONKEY)
    (PARTITION `n_p0` VALUES LESS THAN (6),
    PARTITION `n_p1` VALUES LESS THAN (12),
    PARTITION `n_p2` VALUES LESS THAN (18),
    PARTITION `n_p3` VALUES LESS THAN (MAXVALUE));        
  """

  sql_new_order = """
    CREATE TABLE `new_order` (
    `no_o_id` int(11) NOT NULL,
    `no_d_id` int(11) NOT NULL,
    `no_w_id` int(11) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  """

  sql_order_line = """
    CREATE TABLE `order_line` (
    `ol_o_id` int(11) NOT NULL,
    `ol_d_id` int(11) NOT NULL,
    `ol_w_id` int(11) NOT NULL,
    `ol_number` int(11) NOT NULL,
    `ol_i_id` int(11) NOT NULL,
    `ol_supply_w_id` int(11) DEFAULT NULL,
    `ol_delivery_d` datetime DEFAULT NULL,
    `ol_quantity` int(11) DEFAULT NULL,
    `ol_amount` decimal(6,2) DEFAULT NULL,
    `ol_dist_info` char(24) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin     
  """

  sql_orders = """
    CREATE TABLE `orders` (
    `o_id` int(11) NOT NULL,
    `o_d_id` int(11) NOT NULL,
    `o_w_id` int(11) NOT NULL,
    `o_c_id` int(11) DEFAULT NULL,
    `o_entry_d` datetime DEFAULT NULL,
    `o_carrier_id` int(11) DEFAULT NULL,
    `o_ol_cnt` int(11) DEFAULT NULL,
    `o_all_local` int(11) DEFAULT NULL,
    KEY `idx_order` (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  """

  sql_region = """
    CREATE TABLE `region` (
    `R_REGIONKEY` bigint(20) NOT NULL,
    `R_NAME` char(25) NOT NULL,
    `R_COMMENT` varchar(152) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  """

  sql_stock = """
    CREATE TABLE `stock` (
    `s_i_id` int(11) NOT NULL,
    `s_w_id` int(11) NOT NULL,
    `s_quantity` int(11) DEFAULT NULL,
    `s_dist_01` char(24) DEFAULT NULL,
    `s_dist_02` char(24) DEFAULT NULL,
    `s_dist_03` char(24) DEFAULT NULL,
    `s_dist_04` char(24) DEFAULT NULL,
    `s_dist_05` char(24) DEFAULT NULL,
    `s_dist_06` char(24) DEFAULT NULL,
    `s_dist_07` char(24) DEFAULT NULL,
    `s_dist_08` char(24) DEFAULT NULL,
    `s_dist_09` char(24) DEFAULT NULL,
    `s_dist_10` char(24) DEFAULT NULL,
    `s_ytd` int(11) DEFAULT NULL,
    `s_order_cnt` int(11) DEFAULT NULL,
    `s_remote_cnt` int(11) DEFAULT NULL,
    `s_data` varchar(50) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin

    PARTITION BY RANGE COLUMNS(s_i_id)
    (PARTITION `st_p0` VALUES LESS THAN (25000),
    PARTITION `st_p1` VALUES LESS THAN (50000),
    PARTITION `st_p2` VALUES LESS THAN (75000),
    PARTITION `st_p3` VALUES LESS THAN (MAXVALUE)); 
  """

  sql_supplier = """
    CREATE TABLE `supplier` (
    `S_SUPPKEY` bigint(20) NOT NULL,
    `S_NAME` char(25) NOT NULL,
    `S_ADDRESS` varchar(40) NOT NULL,
    `S_NATIONKEY` bigint(20) NOT NULL,
    `S_PHONE` char(15) NOT NULL,
    `S_ACCTBAL` decimal(15,2) NOT NULL,
    `S_COMMENT` varchar(101) NOT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin

    PARTITION BY RANGE COLUMNS(S_SUPPKEY, S_NATIONKEY)
    (PARTITION `s_p0` VALUES LESS THAN (2500, 6),
    PARTITION `s_p1` VALUES LESS THAN (5000, 12),
    PARTITION `s_p2` VALUES LESS THAN (7500, 18),
    PARTITION `s_p3` VALUES LESS THAN (MAXVALUE, MAXVALUE));     
  """

  repartition_table_and_load_data('warehouse', sql_)