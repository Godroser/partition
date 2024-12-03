import random
import string
import sys
import os
import time

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


def add_tiflash_replica(table):
  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur: 
      cur.execute(    
          "ALTER TABLE {} SET TIFLASH REPLICA 1;".format(table)
      )

def check_replica_status(table):

  with get_connection(autocommit=False) as connection:
    with connection.cursor() as cur: 
      is_start = 0
      start_time = time.time()

      sql = "SELECT * FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA = 'ch' and TABLE_NAME = '{}';".format(table)

      while True:
        cur.execute(sql)
        context = cur.fetchall()
        if context == []:
          if is_start == 0:
            is_start = 1
            start_time = time.time()
          else:
            continue
        else:
          status = context[0][-1]  ## Get PROGRESS status
          if status != 1:    ##SYNCING
            if is_start == 1:
              continue
            else:
              start_time = time.time()            
              is_start = 1
          else:
            end_time = time.time()  ## FINISH SYNC
            if is_start == 1:
              print(status, "latency: ",end_time - start_time)
            else:
              print("fail")
            break
