import mysql.connector
from mysql.connector import MySQLConnection
from typing import Dict, List, Any
import re
import sys
import os
sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))
from config import Config
import csv

def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()  # 假设您有一个Config类提供配置
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": "ch",
        "autocommit": autocommit,
        "use_pure": True
    }

    if config.ca_path:
        db_conf.update({
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
            "ssl_ca": config.ca_path
        })
    return mysql.connector.connect(**db_conf)

def extract_plan_features(plan: List[Dict[str, Any]]) -> Dict[str, float]:
    """解析EXPLAIN VERBOSE输出并提取特征"""
    features = {
        'p_tot_cost': 0.0,
        "p_st_cost": 0.0,
        'p_rows': 0.0,
        'op_count': 0,
        'row_count': 0.0,
        'TableFullScan_cnt': 0,
        'HashAgg_cnt': 0,
        'HashAgg_rows': 0.0,
        'TableFullScan_rows': 0.0
    }

    if not plan:
        return features
    # print(plan)

    # 提取所有算子的estCost和estRows
    all_costs = []
    for op in plan:
        # 算子计数
        features['op_count'] += 1
        
        # 提取estCost（可能为None或字符串）
        est_cost = op.get('estCost')
        if est_cost and isinstance(est_cost, (int, float, str)):
            try:
                cost = float(est_cost) if isinstance(est_cost, str) else est_cost
                all_costs.append(cost)
            except (ValueError, TypeError):
                pass

        # 提取estRows
        est_rows = op.get('estRows')
        op_id = op['id'].strip()
        if est_rows and isinstance(est_rows, (int, float, str)):
            try:
                rows = float(est_rows) if isinstance(est_rows, str) else est_rows
                if 'HashAgg' in op_id and est_rows is not None:
                    features['HashAgg_rows'] = rows  # always assign, so last one is kept
                elif 'TableFullScan' in op_id and est_rows is not None:
                    features['TableFullScan_rows'] = rows  # always assign, so last one is kept
            except (ValueError, TypeError):
                pass

        # 算子类型计数
        if 'TableFullScan' in op_id:
            features['TableFullScan_cnt'] += 1
        elif 'HashAgg' in op_id:
            features['HashAgg_cnt'] += 1

    # 设置成本相关特征
    if all_costs:
        features['p_tot_cost'] = max(all_costs)
        features['p_st_cost'] = min(all_costs)  # 通常扫描算子的成本是最小的

    # 设置行数特征（取根算子的estRows）
    root_op = plan[0]
    if 'estRows' in root_op:
        try:
            features['p_rows'] = float(root_op['estRows'])
        except (ValueError, TypeError):
            pass

    # row_count取底层扫描算子的estRows
    for op in reversed(plan):  # 从底部开始找
        op_id = op['id'].strip()
        if 'TableFullScan' in op_id and 'estRows' in op:
            try:
                features['row_count'] = float(op['estRows'])
                break
            except (ValueError, TypeError):
                pass

    return features

def get_query_plan_features(sql: str) -> Dict[str, float]:
    """执行EXPLAIN VERBOSE并提取特征"""
    with get_connection(autocommit=False) as connection:
        with connection.cursor(dictionary=True) as cur:
            cur.execute(f"EXPLAIN FORMAT = 'verbose' {sql}")
            plan = cur.fetchall()
    return extract_plan_features(plan)

def batch_collect_features(sql_queries: List[str]) -> List[Dict[str, float]]:
    """批量收集多个SQL查询的特征"""
    features_list = []
    for sql in sql_queries:
        try:
            with get_connection(autocommit=False) as connection:
                with connection.cursor(dictionary=True) as cur:
                    cur.execute(f"EXPLAIN FORMAT = 'verbose' {sql}")
                    plan = cur.fetchall()
                    features = extract_plan_features(plan)
                    features_list.append(features)
        except Exception as e:
            print(f"Error processing SQL: {sql}\nError: {str(e)}")
            features_list.append({})  # 添加空字典作为占位符
    return features_list

def get_query_execution_time(sql: str) -> float:
    """执行EXPLAIN ANALYZE并提取总执行时间（单位：ms，取第一个算子的time，来自execution info字段）"""
    with get_connection(autocommit=False) as connection:
        with connection.cursor(dictionary=True) as cur:
            cur.execute(f"EXPLAIN ANALYZE {sql}")
            analyze_result = cur.fetchall()
    import re
    if not analyze_result:
        return 0.0
    exec_info = analyze_result[0].get('execution info', '')
    match = re.search(r'time:(\d+\.?\d*)\s*(ms|s)', exec_info)
    if match:
        value, unit = match.groups()
        value = float(value)
        if unit == 's':
            value *= 1000
        return value
    return 0.0

# 示例用法
if __name__ == "__main__":
    # # 单个sql的测试
    # test_sql = """
    # select ol_number, sum(ol_quantity) as sum_qty, 
    # sum(ol_amount) as sum_amount, avg(ol_quantity) as avg_qty, 
    # avg(ol_amount) as avg_amount, count(*) as count_order 
    # from order_line group by ol_number order by ol_number
    # """
    
    # features = get_query_plan_features(test_sql)
    # exec_time = get_query_execution_time(test_sql)
    # print("Extracted Features:")
    # for k, v in features.items():
    #     print(f"{k}: {v}")    
    # print(f"Execution Time: {exec_time} ms")

	# 多个sql的测试并写入csv
    sql_file = "../../workload/workloadd_train.sql"
    csv_file = "sql_features_with_time_train.csv"
    with open(sql_file, "r", encoding="utf-8") as f:
        sql_content = f.read()
    sql_list = [s.strip() for s in sql_content.split(';') if s.strip()]

    # 先用一条SQL提取特征，获得所有特征名
    sample_features = get_query_plan_features(sql_list[0])
    fieldnames = list(sample_features.keys()) + ['label']  # label为执行时间

    with open(csv_file, 'w', newline='', encoding='utf-8') as csvf:
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()
        for idx, sql in enumerate(sql_list, 1):
            print(f"\nSQL #{idx}:")
            print(sql)
            try:
                features = get_query_plan_features(sql)
                exec_time = get_query_execution_time(sql)
                features['label'] = exec_time
                writer.writerow(features)
                print("Extracted Features:")
                for k, v in features.items():
                    print(f"{k}: {v}")
            except Exception as e:
                print(f"Error extracting features or execution time: {e}")
