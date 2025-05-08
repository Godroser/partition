import re
from collections import defaultdict
import sys
import os
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
import random

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from estimator.ch_columns_ranges_meta import Customer_columns, Warehouse_columns, Supplier_columns, Stock_columns, Region_columns, Orders_columns, Order_line_columns, New_order_columns, Nation_columns, Item_columns, History_columns, District_columns
from oracle_redshift_join_conditions import Join_Conditions

def parse_workload_joins(file_path="/data3/dzh/project/grep/dev/workload/workloadd.sql"):
    """
    解析SQL文件中的join条件, 提取涉及的表名和列名。

    :param file_path: SQL文件路径
    :return: 包含每条SQL的join条件信息的列表
    """
    join_conditions = []

    # 正则表达式匹配join条件
    join_pattern = re.compile(r"JOIN\s+(\w+)\s+ON\s+([\w\.]+)\s*=\s*([\w\.]+)", re.IGNORECASE)

    with open(file_path, "r") as file:
        for line in file:
            line = line.strip()
            if not line:  # 跳过空行
                continue

            matches = join_pattern.findall(line)
            if matches:
                for match in matches:
                    table_name = match[0]
                    column1 = match[1]
                    column2 = match[2]
                    join_conditions.append({
                        "table": table_name,
                        "columns": [column1, column2]
                    })

    return join_conditions

def construct_graph(join_conditions):
    """
    根据Join_Conditions构造无向图, 计算边的重复次数和节点的度数。

    :param join_conditions: Join_Conditions列表
    :return: 图结构(edges, degrees)
    """
    edges = defaultdict(int)  # 存储边及其重复次数
    degrees = defaultdict(int)  # 存储节点的度数

    for sql_joins in join_conditions:
        for join in sql_joins:
            left_table = join["tables"]["left"]
            right_table = join["tables"]["right"]

            for condition in join["conditions"]:
                left_column = condition["left_column"]
                right_column = condition["right_column"]

                # 构造边，确保边的顺序不影响结果
                edge = tuple(sorted([(left_table, left_column), (right_table, right_column)]))
                edges[edge] += 1

                # 更新节点的度数
                degrees[left_table] += 1
                degrees[right_table] += 1

    return edges, degrees

def calculate_edge_weights(edges):
    """
    计算每条边的权重，权重=cost*次数, cost由边包含的两个表的列的size相加得到。

    :param edges: 边及其重复次数的字典
    :return: 边及其权重的字典
    """
    # 表名到列大小的映射
    table_columns_map = {
        "customer": Customer_columns(),
        "district": District_columns(),
        "item": Item_columns(),
        "new_order": New_order_columns(),
        "orders": Orders_columns(),
        "order_line": Order_line_columns(),
        "stock": Stock_columns(),
        "warehouse": Warehouse_columns(),
        "history": History_columns(),
        "nation": Nation_columns(),
        "supplier": Supplier_columns(),
        "region": Region_columns(),
    }

    edge_weights = {}

    for edge, count in edges.items():
        # 获取边的两个节点
        (table1, column1), (table2, column2) = edge

        # 获取表的列大小
        table1_obj = table_columns_map[table1]
        table2_obj = table_columns_map[table2]

        # 找到列的大小
        try:
            size1 = table1_obj.columns_size[table1_obj.columns.index(column1)]
        except ValueError:  # 如果 column1 不在 table1_obj.columns 中
            size1 = table2_obj.columns_size[table2_obj.columns.index(column1)]
        try:
            size2 = table1_obj.columns_size[table1_obj.columns.index(column2)]
        except ValueError:  # 如果 column1 不在 table1_obj.columns 中
            size2 = table2_obj.columns_size[table2_obj.columns.index(column2)]

        # 计算cost和权重
        cost = size1 + size2
        weight = cost * count

        edge_weights[edge] = weight

    return edge_weights

def select_edges(edges, degrees):
    """
    基于边和权重的输出，迭代选择候选边集。

    :param edges: 边及其权重的字典
    :param degrees: 节点的度数字典
    :return: R中包含的边, 以及每个表对应的边的列
    """
    edges = dict(edges)  # 确保可修改
    initial_edges = edges
    R1, R2 = set(), set()
    used_nodes = set()
    iteration = 1

    while edges:
        # 找到度数大于1的节点
        valid_nodes = {node for node, degree in degrees.items() if degree > 1 and node not in used_nodes}
        if not valid_nodes:
            break

        # 随机选择一个度数大于1的节点
        current_node = random.choice(list(valid_nodes))

        # 找到与该节点相连的权重最高的边
        candidate_edges = [(edge, weight) for edge, weight in edges.items() if current_node in {edge[0][0], edge[1][0]}]
        if not candidate_edges:
            continue
        best_edge = max(candidate_edges, key=lambda x: x[1])[0]

        # 将边加入候选集
        if iteration % 2 == 1:
            R1.add(best_edge)
        else:
            R2.add(best_edge)

        # 标记节点为已使用，并移除该节点的所有边
        used_nodes.add(current_node)
        edges = {edge: weight for edge, weight in edges.items() if current_node not in {edge[0][0], edge[1][0]}}

        iteration += 1

    # 比较R1和R2的权重之和，选择较大的作为R
    R1_weight = sum(initial_edges[edge] for edge in R1)
    R2_weight = sum(initial_edges[edge] for edge in R2)
    R = R1 if R1_weight >= R2_weight else R2

    print("print R:")
    for edge in R:
        print(edge)

    # 生成R的表的集合
    R_tables = set()
    for edge in R:
        R_tables.add(edge[0][0])
        R_tables.add(edge[1][0])

    # 生成R的列的集合
    R_columns = set()
    for edge in R:
        R_columns.add(edge[0][1])
        R_columns.add(edge[1][1])

    # 对于不在R中的节点，找到与R中节点相连(并且边对应的列在R_columns中)的权重最大的边，加入R
    remaining_tables = {table for table in degrees.keys() if table not in R_tables}
    for table in remaining_tables:
        candidate_edges = [
            (edge, weight) for edge, weight in initial_edges.items()
            if (table in {edge[0][0], edge[1][0]}) and
               (edge[0][1] in R_columns or edge[1][1] in R_columns)
        ]
        if candidate_edges:
            best_edge = max(candidate_edges, key=lambda x: x[1])[0]
            R.add(best_edge)
            R_tables.add(best_edge[0][0])
            R_tables.add(best_edge[1][0])
            R_columns.add(best_edge[0][1])
            R_columns.add(best_edge[1][1])

    # 输出R中包含的边，以及每个表对应的边的列
    table_columns = defaultdict(list)
    for edge in R:
        table_columns[edge[0][0]].append(edge[0][1])
        table_columns[edge[1][0]].append(edge[1][1])

    return R, table_columns


# 示例调用
if __name__ == "__main__":
    result = parse_workload_joins()
    for item in result:
        print(item)
    edges, degrees = construct_graph(Join_Conditions)
    print("Edges with counts:")
    for edge, count in edges.items():
        print(f"{edge}: {count}")
    print("\nDegrees of nodes:")
    for node, degree in degrees.items():
        print(f"{node}: {degree}")


    edge_weights = calculate_edge_weights(edges)
    print("Edge weights:")
    for edge, weight in edge_weights.items():
        print(f"{edge}: {weight}")

    R, table_columns = select_edges(edge_weights, degrees)

    print("Selected edges in R:")
    for edge in R:
        print(edge)

    print("\nTable columns in R:")
    for table, columns in table_columns.items():
        print(f"{table}: {columns}")

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