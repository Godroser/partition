import sys
import os
import time

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

if __name__ == "__main__":
    start_time = time.time()
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:    
        # cur.execute("SELECT count(*) FROM history;")
        # print(cur.fetchall()[0][0])
        cur.execute("show create table region;")
        cur.fetchall()

    end_time = time.time()        
    delay = end_time - start_time
    print("delay: ", delay)