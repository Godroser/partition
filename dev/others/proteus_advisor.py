import math
import sys
import os
import time
import copy
import random
import json
from datetime import datetime, timedelta
from decimal import Decimal
import logging

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from mcts.mcts import Node, State
from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.ch_query_cost import *
from estimator.ch_columns_ranges_meta import *


# update metadata given the partition and replica candidate
# candidate format:{'name': , 'columns':, 'partitionable_columns': , 'partition_keys': [], 'replicas': [], 'replica_partition_keys': []}
# 每一个candidate都是一个表的分区和副本设置
def update_meta(table_columns, table_meta, candidates):
    table_dict = {'customer': 0, 'district': 1, 'history': 2, 'item': 3, 'nation': 4, 'new_order': 5, 'order_line': 6, 'orders': 7, 'region': 8, 'stock': 9, 'supplier': 10, 'warehouse': 11}

    # 遍历表的分区键设置, 更新meta类和replica_meta类的分区元信息
    for candidate in candidates:
        for i in range(2):
            if i == 0:  # 更新meta类
                partition_keys = candidate['partition_keys']
            elif i == 1:  # 更新replica_meta类
                partition_keys = candidate['replica_partition_keys']

            #print('table_ranges: ', table_ranges)
            # update partition metadata
            # 没有选分区键
            if len(partition_keys) == 0:
                #print("Table {} no partition keys".format(table_name))
                continue

            # get tables key min and max value
            table_name = candidate['name']
            idx = table_dict.get(table_name)

            # 获取表的分区键的最大最小值
            # [[],[]] minmaxvalue[i][0]: min value, minmaxvalue[i][1]: max value
            minmaxvalues = table_columns[idx].get_keys_ranges(partition_keys)  

            # print('minmaxvalue: ', minmaxvalues)  
            # if len(minmaxvalues) > 0:
            #     print('minmaxvalue type: ', type(minmaxvalues[0][1]))

            # get tables keys ranges 划定每个分区的范围, 支持多个列
            # [[],[]] table_ranges[i]:第i个分区键的范围列表，默认分成4份
            table_ranges = [] 
            #print(minmaxvalues)
            for minmaxvalue in minmaxvalues:
                try:
                    # 检查是否为整数类型
                    if isinstance(minmaxvalue[0], int) and isinstance(minmaxvalue[1], int):
                        min_val = minmaxvalue[0]
                        max_val = minmaxvalue[1]
                        # 对于整型，直接均匀分成四份
                        step = (max_val - min_val) / 4
                        table_ranges.append([min_val + i * step for i in range(1, 5)])
                    # 检查是否为字符串类型并尝试转换为整数
                    elif isinstance(minmaxvalue[0], str) and isinstance(minmaxvalue[1], str):
                        min_val = int(minmaxvalue[0])
                        max_val = int(minmaxvalue[1])
                        # 对于整型，直接均匀分成四份
                        step = (max_val - min_val) / 4
                        table_ranges.append([min_val + i * step for i in range(1, 5)])
                    # 检查是否为Decimal类型
                    elif isinstance(minmaxvalue[0], Decimal) and isinstance(minmaxvalue[1], Decimal):
                        min_val = minmaxvalue[0]
                        max_val = minmaxvalue[1]
                        # 对于Decimal类型，直接均匀分成四份
                        step = (max_val - min_val) / 4
                        table_ranges.append([min_val + i * step for i in range(1, 5)])                    
                    else:
                        raise ValueError("Unsupported type for partition keys")
                except ValueError:
                    try:
                        # 检查是否为datetime类型
                        if isinstance(minmaxvalue[0], datetime) and isinstance(minmaxvalue[1], datetime):
                            min_val = minmaxvalue[0]
                            max_val = minmaxvalue[1]
                        else:
                            # 尝试将字符串转换为datetime
                            min_val = datetime.strptime(minmaxvalue[0], '%Y-%m-%d %H:%M:%S')
                            max_val = datetime.strptime(minmaxvalue[1], '%Y-%m-%d %H:%M:%S')
                        # 对于datetime类型，根据天均匀分成四份
                        total_days = (max_val - min_val).days
                        step = total_days / 4
                        table_ranges.append([min_val + timedelta(days=i * step) for i in range(1, 5)])
                    except ValueError:
                        raise ValueError(f"Unsupported type for partition keys: {minmaxvalue}")

            if i == 0: 
                # 更新meta类的分区元信息
                table_meta[idx].update_partition_metadata(partition_keys, table_ranges)
            elif i==1:
                # 更新replica_meta类的分区元信息
                table_meta[idx + 12].update_partition_metadata(partition_keys, table_ranges)

# 更新每个Qcard类的rowSize
def update_rowsize(table_columns, candidates):
    qcard = []
    # 遍历query, 更新每个Qcard类的rowSize
    q1card = Q1card()
    q1card.init()
    #print("Query 1")
    q1card.update_table_rowsize(table_columns, candidates)
    qcard.append(q1card)

    q2card = Q2card()
    q2card.init()
    #print("Query 2")
    q2card.update_table_rowsize(table_columns, candidates)
    qcard.append(q2card)

    q3card = Q3card()
    q3card.init()
    #print("Query 3")
    q3card.update_table_rowsize(table_columns, candidates)
    qcard.append(q3card)

    q4card = Q4card()
    q4card.init()
    #print("Query 4")
    q4card.update_table_rowsize(table_columns, candidates)
    qcard.append(q4card)

    q5card = Q5card()
    q5card.init()
    #print("Query 5")
    q5card.update_table_rowsize(table_columns, candidates)
    qcard.append(q5card)

    q6card = Q6card()
    q6card.init()
    #print("Query 6")
    q6card.update_table_rowsize(table_columns, candidates)
    qcard.append(q6card)

    q7card = Q7card()
    q7card.init()
    #print("Query 7")
    q7card.update_table_rowsize(table_columns, candidates)
    qcard.append(q7card)

    q8card = Q8card()
    q8card.init()
    #print("Query 8")
    q8card.update_table_rowsize(table_columns, candidates)
    qcard.append(q8card)

    q9card = Q9card()
    q9card.init()
    #print("Query 9")
    q9card.update_table_rowsize(table_columns, candidates)
    qcard.append(q9card)

    q10card = Q10card()
    q10card.init()
    #print("Query 10")
    q10card.update_table_rowsize(table_columns, candidates)
    qcard.append(q10card)

    q11card = Q11card()
    q11card.init()
    #print("Query 11")
    q11card.update_table_rowsize(table_columns, candidates)
    qcard.append(q11card)

    q12card = Q12card()
    q12card.init()
    #print("Query 12")
    q12card.update_table_rowsize(table_columns, candidates)
    qcard.append(q12card)

    q13card = Q13card()
    q13card.init()
    #print("Query 13")
    q13card.update_table_rowsize(table_columns, candidates)
    qcard.append(q13card)

    q14card = Q14card()
    q14card.init()
    #print("Query 14")
    q14card.update_table_rowsize(table_columns, candidates)
    qcard.append(q14card)

    q15card = Q15card()
    q15card.init()
    #print("Query 15")
    q15card.update_table_rowsize(table_columns, candidates)
    qcard.append(q15card)

    q16card = Q16card()
    q16card.init()
    #print("Query 16")
    q16card.update_table_rowsize(table_columns, candidates)
    qcard.append(q16card)

    q17card = Q17card()
    q17card.init()
    #print("Query 17")
    q17card.update_table_rowsize(table_columns, candidates)
    qcard.append(q17card)

    q18card = Q18card()
    q18card.init()
    #print("Query 18")
    q18card.update_table_rowsize(table_columns, candidates)
    qcard.append(q18card)

    q19card = Q19card()
    q19card.init()
    #print("Query 19")
    q19card.update_table_rowsize(table_columns, candidates)
    qcard.append(q19card)

    q20card = Q20card()
    q20card.init()
    #print("Query 20")
    q20card.update_table_rowsize(table_columns, candidates)
    qcard.append(q20card)

    q21card = Q21card()
    q21card.init()
    #print("Query 21")
    q21card.update_table_rowsize(table_columns, candidates)
    qcard.append(q21card)

    q22card = Q22card()
    q22card.init()
    #print("Query 22")
    q22card.update_table_rowsize(table_columns, candidates)
    qcard.append(q22card)

    return qcard

def reset_table_meta(table_meta):
    #reset keys, partition_cnt, partition_range in table_meta
    table_meta.clear()
    customer_meta = Customer_Meta()
    district_meta = District_Meta()
    history_meta = History_Meta()
    item_meta = Item_Meta()
    nation_meta = Nation_Meta()
    new_order_meta = New_Order_Meta()
    order_line_meta = Order_Line_Meta()
    orders_meta = Orders_Meta()
    region_meta = Region_Meta()
    stock_meta = Stock_Meta()
    supplier_meta = Supplier_Meta()
    warehouse_meta = Warehouse_Meta()
    table_meta.extend([customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta])

    customer_replica_meta = Customer_Meta()
    district_replica_meta = District_Meta()
    history_replica_meta = History_Meta()
    item_replica_meta = Item_Meta()
    nation_replica_meta = Nation_Meta()
    new_order_replica_meta = New_Order_Meta()
    order_line_replica_meta = Order_Line_Meta()
    orders_replica_meta = Orders_Meta()
    region_replica_meta = Region_Meta()
    stock_replica_meta = Stock_Meta()
    supplier_replica_meta = Supplier_Meta()
    warehouse_replica_meta = Warehouse_Meta()

    customer_replica_meta.isreplica = True
    district_replica_meta.isreplica = True
    history_replica_meta.isreplica = True
    item_replica_meta.isreplica = True
    nation_replica_meta.isreplica = True
    new_order_replica_meta.isreplica = True
    order_line_replica_meta.isreplica = True
    orders_replica_meta.isreplica = True
    region_replica_meta.isreplica = True
    stock_replica_meta.isreplica = True
    supplier_replica_meta.isreplica = True
    warehouse_replica_meta.isreplica = True

    table_meta.extend([customer_replica_meta, district_replica_meta, history_replica_meta, item_replica_meta, nation_replica_meta, new_order_replica_meta, order_line_replica_meta, orders_replica_meta, region_replica_meta, stock_replica_meta, supplier_replica_meta, warehouse_replica_meta])    

def init_table_columns_meta():
    # 1. 初始化表参数
    table_meta = []
    customer_meta = Customer_Meta()
    district_meta = District_Meta()
    history_meta = History_Meta()
    item_meta = Item_Meta()
    nation_meta = Nation_Meta()
    new_order_meta = New_Order_Meta()
    order_line_meta = Order_Line_Meta()
    orders_meta = Orders_Meta()
    region_meta = Region_Meta()
    stock_meta = Stock_Meta()
    supplier_meta = Supplier_Meta()
    warehouse_meta = Warehouse_Meta()
    # append的顺序要求一致
    table_meta.extend([customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta]) 

    # 初始化列存表的参数
    customer_replica_meta = Customer_Meta()
    district_replica_meta = District_Meta()
    history_replica_meta = History_Meta()
    item_replica_meta = Item_Meta()
    nation_replica_meta = Nation_Meta()
    new_order_replica_meta = New_Order_Meta()
    order_line_replica_meta = Order_Line_Meta()
    orders_replica_meta = Orders_Meta()
    region_replica_meta = Region_Meta()
    stock_replica_meta = Stock_Meta()
    supplier_replica_meta = Supplier_Meta()
    warehouse_replica_meta = Warehouse_Meta()

    customer_replica_meta.isreplica = True
    district_replica_meta.isreplica = True
    history_replica_meta.isreplica = True
    item_replica_meta.isreplica = True
    nation_replica_meta.isreplica = True
    new_order_replica_meta.isreplica = True
    order_line_replica_meta.isreplica = True
    orders_replica_meta.isreplica = True
    region_replica_meta.isreplica = True
    stock_replica_meta.isreplica = True
    supplier_replica_meta.isreplica = True
    warehouse_replica_meta.isreplica = True

    table_meta.extend([customer_replica_meta, district_replica_meta, history_replica_meta, item_replica_meta, nation_replica_meta, new_order_replica_meta, order_line_replica_meta, orders_replica_meta, region_replica_meta, stock_replica_meta, supplier_replica_meta, warehouse_replica_meta])

    # 2. 初始化表的列参数
    table_columns = []
    customer_columns = Customer_columns()
    district_columns = District_columns()
    history_columns = History_columns()    
    item_columns = Item_columns()
    nation_columns = Nation_columns()
    new_order_columns = New_order_columns()
    order_line_columns = Order_line_columns()    
    orders_columns = Orders_columns()
    region_columns = Region_columns()
    stock_columns = Stock_columns()
    supplier_columns = Supplier_columns()
    warehouse_columns = Warehouse_columns()
    table_columns.extend([customer_columns, district_columns, history_columns, item_columns, nation_columns, new_order_columns, order_line_columns, orders_columns, region_columns, stock_columns, supplier_columns, warehouse_columns])    

    return table_meta, table_columns

# 计算query前的准备, 根据candidate调整meta card qparams类
def pre_calculate_cost(table_columns, table_meta, candidates):
    # 根据分区副本情况更新元数据   
    update_meta(table_columns, table_meta, candidates)

    # 更新每个Qcard类的rowSize
    qcard_list = update_rowsize(table_columns, candidates)

    # get Qcard 判断是否要读表的replica, 获取每个query扫描对应表的card
    get_qcard(table_meta, qcard_list, candidates)    

    # update Qparams 将qcard复制到qparams
    qparams_list = update_qparams_with_qcard(qcard_list)

    return qparams_list

## 计算query包含的operator的cost, 返回query的每个operator的cost列表, cost列表和query_operators列表的对应
def calculate_query_operators_cost(qry_idx, qparams_list):
    local_query_operators = update_query_operators_with_replica(qry_idx, qparams_list, query_operators)
    query_info = local_query_operators[qry_idx]
    operators = query_info["operators"]
    tables = query_info["tables"]
    content = '1'
    cost = 0

    # 记录operator的cost列表
    query_operators_costs = []

    # print("Tables:", tables)
    # print("Operators:", operators)
    # print('qparams_list: ', qparams_list[qry_idx])

    for operator, table in zip(operators, tables):
        rows_attr = f"rows_tablescan_{table}"
        rowsize_attr = f"rowsize_tablescan_{table}"
        rows_selection_attr = f"rows_selection_{table}"

        cost = 0
        engine = 'Tikv'
        if table.endswith("_replica"):
            engine = 'Tiflash'           


        rows = getattr(qparams_list[qry_idx], rows_attr, None)
        rowsize = getattr(qparams_list[qry_idx], rowsize_attr, None)
        rows_selection = getattr(qparams_list[qry_idx], rows_selection_attr, None)

        # print("table:", table)
        # print('rows_attr: {} : {}'.format(rows_attr, rows))
        # print('rowsize_attr: {} : {}'.format(rowsize_attr, rowsize))
        logging.debug(f"rowsize_attr: {rowsize_attr} : {rowsize}")

        if operator == "TableScan":
            op_instance = TableScan(content, rows, rowsize)
        elif operator == "Selection":
            op_instance = Selection(content, rows_selection, 1)
        elif operator == "TableReader":
            op_instance = TableReader(content, rows, rowsize)
        else:
            continue

        op_instance.engine = engine
        cost = op_instance.calculate_cost()
        # print("operator: ", operator)
        # print("add op_instance.calculate_cost: ", op_instance.calculate_cost())

        # 对于读取了rpelica的情况, 要计算额外的算子开销
        if table.endswith("_replica"):
            engine = 'Tiflash'
            original_table = table.replace("_replica", "")
            original_rows_attr = f"rows_tablescan_{original_table}"
            original_rowsize_attr = f"rowsize_tablescan_{original_table}"

            buildRows = getattr(qparams_list[qry_idx], original_rows_attr, None)
            buildRowSize = getattr(qparams_list[qry_idx], original_rowsize_attr, None)
            probeRows = rows
            probeRowSize = rowsize

            # 获取原表对应的 primary_keys 数量
            table_columns_class = globals()[f"{original_table.capitalize()}_columns"]
            nKeys = len(table_columns_class().primary_keys)

            hash_join_instance = HashJoin(content, buildRows, 1, buildRowSize, nKeys, probeRows, 1, probeRowSize)
            hash_join_instance.engine = engine
            cost += hash_join_instance.calculate_cost()

            # print("add hash_join_instance.calculate_cost(): ",hash_join_instance.calculate_cost())
        
        query_operators_costs.append(cost)
            
    return query_operators_costs

# 计算每一个query在候选下的cost
# candidates是每个表的分区和副本设置
def calculate_cost(table_meta, qparams_list):
    # 记录每个query各个operators的cost
    query_operators_costs = []
    # 计算22条query的代价
    for i in range(0,22):
        cost = calculate_query_operators_cost(i, qparams_list)
        query_operators_costs.append([cost])
        # print(f"Query{i+1} operators cost: {cost}")
    
    reset_table_meta(table_meta)
    return query_operators_costs

# 新增函数：计算每个表涉及的算子的cost总和
def calculate_table_operator_costs(query_operators_costs, query_operators):
    table_costs = {table: 0 for table in ['customer', 'district', 'history', 'item', 'nation', 'new_order', 'order_line', 'orders', 'region', 'stock', 'supplier', 'warehouse']}
    
    for query_idx, costs in enumerate(query_operators_costs):
        operators = query_operators[query_idx]["operators"]
        tables = query_operators[query_idx]["tables"]
        for operator, table, cost in zip(operators, tables, costs[0]):
            if table.endswith("_replica"):
                table = table.replace("_replica", "")
            table_costs[table] += cost
    
    return table_costs

# 搜索每个表的最佳候选配置
def search_table_candidate(table_meta, table_columns, qparams_list, table_idx, candidates):
    table_candidate = candidates[table_idx]
    # table_name = table_candidate['name']
    columns = table_columns[table_idx].columns
    primary_keys = table_columns[table_idx].primary_keys

    # 垂直分区
    mid_point = len(columns) // 2
    vertical_partitions = [
        columns[:mid_point],
        columns[mid_point:]
    ]

    # 水平分区
    horizontal_partitions = {
        'partition_keys': primary_keys,
        'replica_partition_keys': primary_keys
    }

    # 生成所有候选配置
    candidates_list = []
    for replica in vertical_partitions:
        candidate = {
            "replicas": replica,
            "partition_keys": [],
            "replica_partition_keys": []
        }
        candidates_list.append(candidate)

        if horizontal_partitions['partition_keys']:
            candidate_with_partition = candidate.copy()
            candidate_with_partition['partition_keys'] = horizontal_partitions['partition_keys']
            candidates_list.append(candidate_with_partition)

        if horizontal_partitions['replica_partition_keys']:
            candidate_with_replica_partition = candidate.copy()
            candidate_with_replica_partition['replica_partition_keys'] = horizontal_partitions['replica_partition_keys']
            candidates_list.append(candidate_with_replica_partition)

        if horizontal_partitions['partition_keys'] and horizontal_partitions['replica_partition_keys']:
            candidate_with_both_partitions = candidate.copy()
            candidate_with_both_partitions['partition_keys'] = horizontal_partitions['partition_keys']
            candidate_with_both_partitions['replica_partition_keys'] = horizontal_partitions['replica_partition_keys']
            candidates_list.append(candidate_with_both_partitions)

    # 初始状态
    initial_candidate = {
        "replicas": [],
        "partition_keys": [],
        "replica_partition_keys": []
    }
    candidates_list.append(initial_candidate)

    # 评估每种候选配置的成本，选择成本最低的配置
    best_candidate = None
    lowest_cost = float('inf')

    for candidate in candidates_list:
        table_candidate.update(candidate)
        qparams_list = pre_calculate_cost(table_columns, table_meta, candidates)
        query_operators_costs = calculate_cost(table_meta, qparams_list)
        table_operator_costs = calculate_table_operator_costs(query_operators_costs, query_operators)
        total_cost = sum(table_operator_costs.values())

        if total_cost < lowest_cost:
            lowest_cost = total_cost
            best_candidate = candidate

    table_candidate.update(best_candidate)

    return table_candidate

if __name__ == "__main__":
    table_meta, table_columns = init_table_columns_meta()

    ## 构造初始配置
    tables = []
    for table_column in table_columns:
        dict_tmp = {}
        dict_tmp['name'] = table_column.name
        dict_tmp['columns'] = table_column.columns
        dict_tmp['partitionable_columns'] = table_column.partitionable_columns
        dict_tmp['partition_keys'] = table_column.partition_keys
        dict_tmp['replicas'] = table_column.replicas
        # dict_tmp['replicas'] = table_column.columns # 初始默认全表replica
        dict_tmp['replica_partition_keys'] = table_column.replica_partition_keys
        tables.append(dict_tmp)   

    candidates = State(tables).tables

    qparams_list = pre_calculate_cost(table_columns, table_meta, candidates)
    query_operators_costs = calculate_cost(table_meta, qparams_list)
    
    # 计算每个表的算子cost总和
    table_operator_costs = calculate_table_operator_costs(query_operators_costs, query_operators)
    print("Table operator costs:", table_operator_costs)

    table_candidates = []
    for table_idx in range(len(tables)):
        table_candidate = search_table_candidate(table_meta, table_columns,qparams_list, table_idx, candidates)
        table_candidates.append(table_candidate)
        print(f"Table {tables[table_idx]['name']}: {table_candidate}")

    # 使用 json.dumps 格式化输出
    formatted_output = json.dumps(table_candidates, indent=4, ensure_ascii=False)
    # 将格式化后的输出写入到文件
    with open('/data3/dzh/project/grep/dev/Output/proteus_advisor.txt', 'w', encoding='utf-8') as f:
        f.write(formatted_output)    