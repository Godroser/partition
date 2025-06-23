import os
import sys
import json
import re

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from tools.repartitioning import *
from tools.repartitioning import get_create_table_sql
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
        "database": '50oracle_redshift', #指定测试库
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

# 生成指定表的创建分区SQL语句
def generate_partition_sql(tables, partition_keys):
    partition_sqls = []
    config = Config()
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
        # load_data(table)  load data改成通过linghtning完成
    for table in replica_tables:
        set_replica_sql = f"ALTER TABLE `50oarcle_redshift`.`{table}` SET TIFLASH REPLICA 1;"
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                cur.execute(set_replica_sql)
                print(f"Table {table} is set to replica!")