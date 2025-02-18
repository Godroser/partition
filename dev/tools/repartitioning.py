import random
import string
import sys
import os
import time
import subprocess
from datetime import datetime, timedelta

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
  data_dir = "/data3/dzh/CH-data/ch"
  prefix = "ch_bak." + table + "."
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

# 生成指定表的创建分区SQL语句
def generate_partition_sql(tables, partition_keys):
    config = Config()
    partition_sqls = []

    with get_connection() as connection:
        for table, keys in zip(tables, partition_keys):
            with connection.cursor() as cur:
                # 查询得到每个partition_key的范围
                min_vals = []
                max_vals = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM {table}")
                    min_val, max_val = cur.fetchone()
                    if min_val is None or max_val is None:
                        raise ValueError(f"Table {table} or partition key {key} has no data")
                    min_vals.append(min_val)
                    max_vals.append(max_val)

                # 计算每个分区的范围
                partition_ranges = []
                for min_val, max_val in zip(min_vals, max_vals):
                    if isinstance(min_val, int) and isinstance(max_val, int):
                        step = round((max_val - min_val) / 4)
                        partition_ranges.append([int(min_val + i * step) for i in range(1, 5)])
                    elif isinstance(min_val, datetime) and isinstance(max_val, datetime):
                        total_seconds = (max_val - min_val).total_seconds()
                        step = round(total_seconds / 4)
                        partition_ranges.append([f"'{(min_val + timedelta(seconds=i * step)).strftime('%Y-%m-%d %H:%M:%S')}'" for i in range(1, 5)])
                    else:
                        raise ValueError(f"Unsupported type for partition key {key}")

                # 生成建表SQL语句
                maxvalue_str = ', '.join(['MAXVALUE'] * len(keys))
                partition_sql = f"""
                PARTITION BY RANGE COLUMNS({', '.join(keys)})
                (PARTITION `{table}_p0` VALUES LESS THAN ({', '.join(str(partition_ranges[i][0]) for i in range(len(keys)))}),
                PARTITION `{table}_p1` VALUES LESS THAN ({', '.join(str(partition_ranges[i][1]) for i in range(len(keys)))}),
                PARTITION `{table}_p2` VALUES LESS THAN ({', '.join(str(partition_ranges[i][2]) for i in range(len(keys)))}),
                PARTITION `{table}_p3` VALUES LESS THAN ({maxvalue_str}));
                """
                partition_sqls.append(partition_sql)

    return partition_sqls

# 将指定表的建表语句和分区语句合并
def comnbine_repartition_sql(create_table_sqls, partition_sqls):
    repartition_sqls = []
    for create_table_sql, partition_sql in zip(create_table_sqls, partition_sqls):
        # 去掉原有建表语句末尾的分号
        # create_table_sql = create_table_sql.rstrip(';')
        # 合并建表语句和分区语句
        repartition_sql = f"{create_table_sql}\n{partition_sql}"
        repartition_sqls.append(repartition_sql)
    return repartition_sqls

def get_create_table_sql(table_name):
    sql_statements = {
        'customer': """
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """,
        'district': """
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """,
        'history': """
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """,
        'item': """
            CREATE TABLE `item` (
            `i_id` int(11) NOT NULL,
            `i_im_id` int(11) DEFAULT NULL,
            `i_name` varchar(24) DEFAULT NULL,
            `i_price` decimal(5,2) DEFAULT NULL,
            `i_data` varchar(50) DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin   
        """,
        'nation': """
            CREATE TABLE `nation` (
            `N_NATIONKEY` bigint(20) NOT NULL,
            `N_NAME` char(25) NOT NULL,
            `N_REGIONKEY` bigint(20) NOT NULL,
            `N_COMMENT` varchar(152) DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin      
        """,
        'new_order': """
            CREATE TABLE `new_order` (
            `no_o_id` int(11) NOT NULL,
            `no_d_id` int(11) NOT NULL,
            `no_w_id` int(11) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """,
        'order_line': """
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
        """,
        'orders': """
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
        """,
        'region': """
            CREATE TABLE `region` (
            `R_REGIONKEY` bigint(20) NOT NULL,
            `R_NAME` char(25) NOT NULL,
            `R_COMMENT` varchar(152) DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
        """,
        'stock': """
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
        """,
        'supplier': """
            CREATE TABLE `supplier` (
            `S_SUPPKEY` bigint(20) NOT NULL,
            `S_NAME` char(25) NOT NULL,
            `S_ADDRESS` varchar(40) NOT NULL,
            `S_NATIONKEY` bigint(20) NOT NULL,
            `S_PHONE` char(15) NOT NULL,
            `S_ACCTBAL` decimal(15,2) NOT NULL,
            `S_COMMENT` varchar(101) NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin   
        """,
        'warehouse': """
            CREATE TABLE `warehouse` (
              `w_id` int(11) NOT NULL,
              `w_name` varchar(10) DEFAULT NULL,
              `w_street_1` varchar(20) DEFAULT NULL,
              `w_street_2` varchar(20) DEFAULT NULL,
              `w_city` varchar(20) DEFAULT NULL,
              `w_state` char(2) DEFAULT NULL,
              `w_zip` char(9) DEFAULT NULL,
              `w_tax` decimal(4,4) DEFAULT NULL,
              `w_ytd` decimal(12,2) DEFAULT NULL,
              PRIMARY KEY (`w_id`) /*T![clustered_index] CLUSTERED */
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin  
        """
    }
    return sql_statements.get(table_name)

# 创建新的数据库, 根据CH-DATA下原有的数据
def create_new_database():
    tables = ['warehouse', 'supplier', 'stock', 'region', 'orders', 'order_line', 'new_order', 'nation', 'item', 'history', 'district', 'customer']
    for table in tables:
        sql = get_create_table_sql(table)
        repartition_table(table, sql)
        load_data(table)   


if __name__ == "__main__":
    tables = ['district', 'nation', 'order_line', 'orders', 'stock']
    partition_keys = [['d_id'], ['n_nationkey'], ['ol_delivery_d'], ["o_carrier_id", "o_all_local"], ["s_order_cnt", "s_w_id"]]
    partition_sqls = generate_partition_sql(tables, partition_keys)
    
    create_table_sqls = [get_create_table_sql(table) for table in tables]
    combined_sqls = comnbine_repartition_sql(create_table_sqls, partition_sqls)
    
    for sql in combined_sqls:
        print(sql)
    
    for table, sql in zip(tables, combined_sqls):
        repartition_table(table, sql)
        load_data(table)       


