import sys
import os
import numpy as np
from sklearn.cluster import KMeans

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

# 导入ch_columns_ranges_meta.py中的所有表类
from estimator.ch_columns_ranges_meta import (
    Customer_columns, District_columns, Item_columns, New_order_columns,
    Orders_columns, Order_line_columns, Stock_columns, Warehouse_columns,
    History_columns, Nation_columns, Supplier_columns, Region_columns
)

## 预处理table_column_accessed_by_query, 变成table:[1,0,0,0,0]的格式
def process_table_column_accessed_by_query(table_column_accessed_by_query):
    # 创建表类的实例
    table_instances = {
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
        "region": Region_columns()
    }

    # 处理table_column_accessed_by_query
    processed_table_column_accessed_by_query = []

    for query_access in table_column_accessed_by_query:
        processed_query_access = {}
        for table, columns in query_access.items():
            if table in table_instances:
                table_columns = table_instances[table].columns
                column_access = [1 if col in columns else 0 for col in table_columns]
                processed_query_access[table] = column_access
        processed_table_column_accessed_by_query.append(processed_query_access)

    return processed_table_column_accessed_by_query

# 定义距离计算方法
def calculate_distance(list1, list2):
    if len(list1) != len(list2):
        raise ValueError("Lists must be of the same length")
    numerator = sum(1 for a, b in zip(list1, list2) if a != b)
    denominator = len(list1)
    return numerator / denominator

# 对每个表进行K-means聚类
def kmeans_clustering(processed_table_column_accessed_by_query):
    table_clusters = {}
    # 获取所有表类的实例
    table_instances = {
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
        "region": Region_columns()
    }

    for table, table_instance in table_instances.items():
        data = [query_access[table] for query_access in processed_table_column_accessed_by_query if table in query_access]
        if len(data) < 2:
            continue
        data = np.array(data)
        kmeans = KMeans(n_clusters=2, random_state=0).fit(data)
        clusters = kmeans.labels_
        centers = kmeans.cluster_centers_
        table_clusters[table] = {
            "clusters": clusters,
            "centers": centers
        }
    return table_clusters

# 根据聚类中心将每个表的列分成两部分
def split_columns_by_cluster_centers(table_clusters):
    table_instances = {
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
        "region": Region_columns()
    }

    split_columns = {}
    for table, cluster_info in table_clusters.items():
        centers = cluster_info["centers"]
        if len(centers) == 0:
            continue
        center = centers[0]  # 使用第一个聚类中心
        table_instance = table_instances[table]
        table_columns = table_instance.columns
        part1 = [col for col, val in zip(table_columns, center) if val != 0]
        part2 = [col for col, val in zip(table_columns, center) if val == 0]
        split_columns[table] = {
            "part1": part1,
            "part2": part2
        }
    return split_columns

if __name__ == '__main__':
    table_column_accessed_by_query = [
        {
            "order_line": ["ol_number", "ol_quantity", "ol_amount", "ol_delivery_d"]
        },
        {
            "item": ["i_id", "i_name", "i_data"],
            "stock": ["s_i_id", "s_w_id", "s_quantity"],
            "supplier": ["s_suppkey", "s_name", "s_address", "s_phone", "s_comment", "s_nationkey"],
            "nation": ["n_name", "n_nationkey", "n_regionkey"],
            "region": ["r_name", "r_regionkey"]
        },
        {
            "customer": ["c_state", "c_id", "c_w_id", "c_d_id"],
            "new_order": ["no_w_id", "no_d_id", "no_o_id"],
            "orders": ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            "order_line": ["ol_o_id", "ol_w_id", "ol_d_id", "ol_amount"]
        },
        {
            "orders": ["o_ol_cnt", "o_id", "o_w_id", "o_d_id", "o_entry_d"],
            "order_line": ["ol_o_id", "ol_w_id", "ol_d_id", "ol_delivery_d"]
        },
        {
            "customer": ["c_id", "c_w_id", "c_d_id", "c_state"],
            "orders": ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            "order_line": ["ol_o_id", "ol_w_id", "ol_d_id", "ol_amount", "ol_i_id"],
            "stock": ["s_w_id", "s_i_id", "s_suppkey", "s_nationkey"],
            "supplier": ["s_suppkey"],
            "nation": ["n_name", "n_nationkey", "n_regionkey"],
            "region": ["r_name", "r_regionkey"]
        },
        {
            "order_line": ["ol_amount", "ol_delivery_d", "ol_quantity"]
        },
        {
            "order_line": ["ol_supply_w_id", "ol_i_id", "ol_w_id", "ol_d_id", "ol_o_id", "ol_amount"],
            "stock": ["s_w_id", "s_i_id"],
            "orders": ["o_w_id", "o_d_id", "o_id", "o_entry_d", "o_c_id"],
            "customer": ["c_id", "c_w_id", "c_d_id", "c_state"],
            "supplier": ["s_suppkey", "s_nationkey"],
            "nation": ["n_nationkey", "n_name"]
        },
        {
            "item": ["i_id", "i_data"],
            "supplier": ["s_suppkey"],
            "stock": ["s_w_id", "s_i_id", "s_nationkey"],
            "order_line": ["ol_i_id", "ol_supply_w_id", "ol_w_id", "ol_d_id", "ol_o_id", "ol_amount"],
            "orders": ["o_w_id", "o_d_id", "o_id", "o_entry_d", "o_c_id"],
            "customer": ["c_id", "c_w_id", "c_d_id", "c_state"],
            "nation": ["n_nationkey", "n_regionkey", "n_name"],
            "region": ["r_regionkey", "r_name"]
        },
        {
            "item": ["i_id", "i_data"],
            "stock": ["s_i_id", "s_w_id", "s_nationkey"],
            "supplier": ["s_suppkey"],
            "order_line": ["ol_i_id", "ol_supply_w_id", "ol_w_id", "ol_d_id", "ol_o_id", "ol_amount"],
            "orders": ["o_w_id", "o_d_id", "o_id"],
            "nation": ["n_nationkey", "n_name"]
        },
        {
            "customer": ["c_id", "c_last", "c_w_id", "c_d_id", "c_city", "c_phone", "c_state"],
            "orders": ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            "order_line": ["ol_w_id", "ol_d_id", "ol_o_id", "ol_amount", "ol_delivery_d"],
            "nation": ["n_nationkey", "n_name"]
        },
        {
            "stock": ["s_i_id", "s_w_id", "s_order_cnt", "s_nationkey"],
            "supplier": ["s_suppkey"],
            "nation": ["n_nationkey", "n_name"]
        },
        {
            "orders": ["o_ol_cnt", "o_carrier_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            "order_line": ["ol_w_id", "ol_d_id", "ol_o_id", "ol_delivery_d"]
        },
        {
            "customer": ["c_id", "c_w_id", "c_d_id"],
            "orders": ["o_w_id", "o_d_id", "o_id", "o_carrier_id"]
        },
        {
            "order_line": ["ol_i_id", "ol_amount", "ol_delivery_d"],
            "item": ["i_id", "i_data"]
        },
        {
            "order_line": ["ol_i_id", "ol_supply_w_id", "ol_amount", "ol_delivery_d"],
            "stock": ["s_w_id", "s_i_id"],
            "supplier": ["s_suppkey", "s_name", "s_address", "s_phone"]
        },
        {
            "stock": ["s_w_id", "s_i_id"],
            "item": ["i_name", "i_data", "i_price", "i_id"],
            "supplier": ["s_suppkey", "s_comment"]
        },
        {
            "order_line": ["ol_amount", "ol_quantity", "ol_i_id"],
            "item": ["i_id"]
        },
        {
            "customer": ["c_last", "c_id", "c_w_id", "c_d_id"],
            "orders": ["o_id", "o_entry_d", "o_ol_cnt", "o_w_id", "o_d_id", "o_c_id"],
            "order_line": ["ol_amount", "ol_w_id", "ol_d_id", "ol_o_id"]
        },
        {
            "order_line": ["ol_amount", "ol_quantity", "ol_i_id", "ol_w_id"],
            "item": ["i_id", "i_price", "i_data"]
        },
        {
            "supplier": ["s_name", "s_address", "s_suppkey", "s_nationkey"],
            "nation": ["n_nationkey", "n_name"],
            "stock": ["s_i_id", "s_w_id", "s_quantity"],
            "order_line": ["ol_i_id", "ol_delivery_d"],
            "item": ["i_id", "i_data"]
        },
        {
            "supplier": ["s_name", "s_suppkey", "s_nationkey"],
            "stock": ["s_w_id", "s_i_id"],
            "order_line": ["ol_i_id", "ol_w_id", "ol_o_id", "ol_d_id", "ol_delivery_d"],
            "orders": ["o_id", "o_entry_d"],
            "nation": ["n_nationkey", "n_name"]
        },
        {
            "customer": ["c_state", "c_phone", "c_balance", "c_id", "c_w_id", "c_d_id"],
            "orders": ["o_c_id", "o_w_id", "o_d_id"]
        }  
    ]

    processed_table_column_accessed_by_query = process_table_column_accessed_by_query(table_column_accessed_by_query)
    table_clusters = kmeans_clustering(processed_table_column_accessed_by_query)
    split_columns = split_columns_by_cluster_centers(table_clusters)

    # 输出每个表的part1和part2包含的列
    for table, parts in split_columns.items():
        print(f"Table: {table}")
        print(f"Part 1: {parts['part1']}")
        print(f"Part 2: {parts['part2']}")
        print("\n")