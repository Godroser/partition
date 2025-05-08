import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector import pooling


class Config:
    def __init__(self) -> None:
      self.TIDB_HOST='127.0.0.1'
      self.TIDB_PORT='4000'
      self.TIDB_USER='root'
      self.TIDB_PASSWORD=''
      self.TIDB_DB_NAME='oracle_redshift'
      self.ca_path = ''

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
    #return mysql.connector.connect(**db_conf)
    return mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=10, **db_conf).get_connection()