import sys
import os

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from estimator.ch_query_card import *
from estimator.ch_columns_ranges_meta import *
from estimator.ch_columns_ranges_meta import Customer_columns, District_columns, Item_columns, New_order_columns, Orders_columns, Order_line_columns, Stock_columns, Warehouse_columns, History_columns, Nation_columns, Supplier_columns, Region_columns

# 对于列的重要性,
# 根据qcard_list里面的qcard.columns和qcard.tables, 分析出每条query读取的列, 为每个表的列维护一个访问的频率直方图, 在table_columns类中. 对于频率低于一定阈值的列, 降低设置其为replicas的action的选择频率.
# 分析TP负载里update语句涉及的列, 这些列也维护一个更新频率在table_columns类中, 同时降低这些列设置成replicas的action的选择频率.

# 对于列的关联程度, 识别一个sql同时访问的column, 包括join投影操作, 将join操作涉及的列, 投影操作的列之间连边, 次数越多代表关联程度越高. 对于关连高的列, 在action中, 将这两个列设置replicas的action绑定到一起.

# 分析ap负载, 获取每个列的查询频率
def analyze_column_usage(qcard_list):
    column_usage = {}

    for qcard in qcard_list:
        for table_idx, columns in enumerate(qcard.columns):
            # print(table_idx)
            table_name = qcard.tables[table_idx]
            if table_name not in column_usage:
                column_usage[table_name] = {}
            for column in columns:
                if column not in column_usage[table_name]:
                    column_usage[table_name][column] = 0
                column_usage[table_name][column] += 1

    return column_usage

# 分析tp负载, 获取每个列的更新频率
tp_column_usage = {'district': {'d_next_o_id': 1, 'd_ytd': 1}, 'stock': {'s_quantity': 1, 's_ytd': 1, 's_order_cnt': 1, 's_remote_cnt': 1}, 'customer': {'c_balance': 2, 'c_ytd_payment': 1, 'c_payment_cnt': 1}, 'warehouse': {'w_ytd': 1}, 'orders': {'o_carrier_id': 1}, 'order_line': {'ol_delivery_d': 1}}

# 将每个列的查询频率和更新频率结合在一起
def generate_column_usage(qcard_list, tp_column_usage):
    # 初始化 columns_usage
    tables = [Customer_columns(), District_columns(), Item_columns(), New_order_columns(), Orders_columns(), Order_line_columns(), Stock_columns(), Warehouse_columns(), History_columns(), Nation_columns(), Supplier_columns(), Region_columns()]
    columns_usage = {}
    for table in tables:
        columns_usage[table.name] = {column: 0 for column in table.columns}

    # 获取 ap_column_usage
    ap_column_usage = analyze_column_usage(qcard_list)

    # 将 ap_column_usage 的数值加到 columns_usage 中
    for table, columns in ap_column_usage.items():
        for column, ap_usage in columns.items():
            if table in columns_usage and column in columns_usage[table]:
                columns_usage[table][column] += ap_usage

    # 将 tp_column_usage 的数值加到 columns_usage 中
    for table, columns in tp_column_usage.items():
        for column, tp_usage in columns.items():
            if table in columns_usage and column in columns_usage[table]:
                columns_usage[table][column] -= tp_usage

    return columns_usage

# 归一化数值
def normalize_column_usage(column_usage):
    all_values = [usage for columns in column_usage.values() for usage in columns.values()]
    min_value = min(all_values)
    max_value = max(all_values)
    
    normalized_usage = {}
    zero_values = 0

    for table, columns in column_usage.items():
        if table not in normalized_usage:
            normalized_usage[table] = {}
        
        for column, usage in columns.items():
            if max_value != min_value:
                normalized_value = (usage - min_value) / (max_value - min_value)
                normalized_usage[table][column] = normalized_value
                if usage == 0:
                    zero_values = normalized_value
                    # zero_values[f"{table}.{column}"] = normalized_value
            else:
                normalized_usage[table][column] = 0

    return normalized_usage, zero_values

def get_normalized_column_usage(qcard_list, tp_column_usage):
    final_usage = generate_column_usage(qcard_list, tp_column_usage)
    normalized_usage, zero_values = normalize_column_usage(final_usage)
    return normalized_usage, zero_values

if __name__ == "__main__":
    qcard_list = [Q1card(), Q2card(), Q3card(), Q4card(), Q5card(), Q6card(), Q7card(), Q8card(), Q9card(), Q10card(), Q11card(), Q12card(), Q13card(), Q14card(), Q15card(), Q16card(), Q17card(), Q18card(), Q19card(), Q20card(), Q21card(), Q22card()]
    for qcard in qcard_list:
        qcard.init()

    # column_usage = analyze_column_usage(qcard_list)
    # print(column_usage)
    # {'order_line': {'ol_number': 1, 'ol_quantity': 5, 'ol_amount': 13, 'ol_delivery_d': 9, 'ol_o_id': 10, 'ol_w_id': 11, 'ol_d_id': 10, 'ol_i_id': 10, 'ol_supply_w_id': 4}, 'item': {'i_id': 8, 'i_name': 2, 'i_data': 7, 'i_price': 2}, 'stock': {'s_i_id': 10, 's_w_id': 10, 's_quantity': 2, 's_order_cnt': 1}, 'supplier': {'s_suppkey': 10, 's_name': 4, 's_address': 3, 's_phone': 2, 's_comment': 2, 's_nationkey': 8}, 'nation': {'n_nationkey': 9, 'n_name': 9, 'n_regionkey': 3}, 'region': {'r_regionkey': 3, 'r_name': 3}, 'customer': {'c_state': 6, 'c_id': 8, 'c_w_id': 8, 'c_d_id': 8, 'c_last': 2, 'c_city': 1, 'c_phone': 2, 'c_balance': 1}, 'new_order': {'no_w_id': 1, 'no_d_id': 1, 'no_o_id': 1}, 'orders': {'o_c_id': 8, 'o_w_id': 11, 'o_d_id': 11, 'o_id': 11, 'o_entry_d': 10, 'o_ol_cnt': 3, 'o_carrier_id': 2}}
    final_usage = generate_column_usage(qcard_list, tp_column_usage)
    print(final_usage)
    
    normalized_usage, zero_values = normalize_column_usage(final_usage)
    print(normalized_usage)
    print("Zero values after normalization:", zero_values)
