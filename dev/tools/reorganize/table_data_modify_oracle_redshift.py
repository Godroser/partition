import os
import sys
import json
import re

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from tools.repartitioning import *
from tools.repartitioning import get_create_table_sql
from tools.repartitioning import generate_partition_sql
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config# 生成指定表的创建分区SQL语句


def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": 'oracle_redshift', #指定测试库
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
    # 生成的配置方案
	# Table columns in R:
	# nation: ['n_nationkey']
	# order_line: ['ol_i_id']
	# stock: ['s_i_id']
	# supplier: ['s_nationkey']
	# item: ['i_id']
	# orders: ['o_w_id']

	# table column store:
	# order_line
	# orders
	# customer    

    tables = ['item', 'nation', 'order_line', 'orders', 'supplier', 'stock']
    partition_keys = [['i_id'], ['n_nationkey'], ['ol_i_id'], ['o_w_id'],[ 's_nationkey'], ['s_i_id']]
    replica_tables = ['order_line', "orders", "customer"]
    
    partition_sqls = generate_partition_sql(tables, partition_keys)
    
    create_table_sqls = [get_create_table_sql(table) for table in tables]
    combined_sqls = comnbine_repartition_sql(create_table_sqls, partition_sqls)	

    for table, sql in zip(tables, combined_sqls):
        repartition_table(table, sql)
        load_data(table)
    for table in replica_tables:
        set_replica_sql = f"ALTER TABLE `oarcle_redshift`.`{table}` SET TIFLASH REPLICA 1;"
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                cur.execute(set_replica_sql)
                print(f"Table {table} is set to replica!")