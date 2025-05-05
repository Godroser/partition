import random
import string
import sys
import os
import math

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


def get_operator_execution_time(sql: str, operator: str = "TableFullScan") -> float:
    """
    获取指定算子(如TableFullScan)的执行延时。

    :param sql: 要执行的 EXPLAIN ANALYZE SQL 语句
    :param operator: 要查询的算子名称，默认为 "TableFullScan"
    :return: 算子的执行延时（单位：毫秒），如果未找到则返回 -1
    """
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            cur.execute(f"EXPLAIN ANALYZE {sql}")
            result = cur.fetchall()
            print(result)
            for row in result:
                if operator in row[0]:  # 假设算子名称在第一列
                    for item in row:
                        if isinstance(item, str) and "time:" in item:
                            # 提取 time: 后的延时信息
                            time_str = item.split("time:")[1].split(",")[0].strip()
                            return float(time_str.replace("ms", "").strip())
            return -1  # 如果未找到指定算子
        
def get_operator_parameters(sql: str, operator: str = "TableFullScan") -> dict:
    """
    获取指定算子的参数信息。

    :param sql: 要执行的 EXPLAIN ANALYZE SQL 语句
    :param operator: 要查询的算子名称，默认为 "TableFullScan"
    :return: 算子的参数信息，格式为字典
    """
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            cur.execute(f"EXPLAIN ANALYZE {sql}")
            result = cur.fetchall()
            for row in result:
                if operator in row[0]:  # 假设算子名称在第一列
                    return row[1:]  # 返回算子的参数信息
            return {}  # 如果未找到指定算子
        
if __name__ == "__main__":
    sql = "select /*+ read_from_storage(tikv[order_line]) */  ol_number,  sum(ol_quantity) as sum_qty,  sum(ol_amount) as sum_amount,  avg(ol_quantity) as avg_qty,  avg(ol_amount) as avg_amount,  count(*) as count_order from order_line group by ol_number order by ol_number;"
    # sql = "select ol_number,  sum(ol_quantity) as sum_qty,  sum(ol_amount) as sum_amount,  avg(ol_quantity) as avg_qty,  avg(ol_amount) as avg_amount,  count(*) as count_order from order_line group by ol_number order by ol_number;"
    operator = "Projection"  # 替换为你要查询的算子名称, TableFullScan Projection
    execution_time = get_operator_execution_time(sql, operator)

    calculate_cost_tablescan = 1250435 * math.log2(65) * 11.6 + (10000 * math.log2(65) * 11.6)
    print("calculate cost: ", calculate_cost_tablescan)

    calculate_cost_projection = 1250435 * 2.4
    if execution_time != -1:
        print(f"{operator} 执行延时: {execution_time} ms")
    else:
        print(f"未找到 {operator} 的执行延时信息")