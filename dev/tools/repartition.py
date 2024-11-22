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
    #print(command)
    #result = subprocess.run(command, shell=True, capture_output=True, text=True)
  print("Table {} data loaded!".format(table))




if __name__ == "__main__":
  load_data("order_line")