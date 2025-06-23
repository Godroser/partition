import math
import sys
import os
import time
import copy
import random
import json
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count
from decimal import Decimal
import logging

#sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from mcts.mcts import Node, State
from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.ch_query_cost import *
from estimator.ch_columns_ranges_meta import *
from config import Config
from log.logging_config import setup_logging
from workload.workload_analyzer import get_normalized_column_usage, tp_column_usage

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




# 计算候选的reward
# candidates是每个表的分区和副本设置
def calculate_reward(table_columns, table_meta, candidates):
    # 根据分区副本情况更新元数据    
    logging.info("Start Update meta data")
    update_meta(table_columns, table_meta, candidates)
    logging.info("Finish Update meta data")

    # 更新每个Qcard类的rowSize
    qcard_list = update_rowsize(table_columns, candidates)
    logging.info("Finish Update rowsize")

    # get Qcard 判断是否要读表的replica, 获取每个query扫描对应表的card
    get_qcard(table_meta, qcard_list, candidates)
    # qcard_list = get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    logging.info("Finish Get Qcard")

    # update Qparams 将qcard复制到qparams
    qparams_list = update_qparams_with_qcard(qcard_list)

    # calculate query cost
    engine = 'Tiflash'
    
    reward = 0.0
    # 计算22条query的代价
    for i in range(0,22):
        cost = 0
        cost = calculate_query_cost(i, qparams_list)
        reward += cost
        logging.info(f"Query{i+1}: {cost}")

    # reward += calculate_q1(engine, qparams_list[0])
    # reward += calculate_q2(engine, qparams_list[1])
    # reward += calculate_q3(engine, qparams_list[2])
    # reward += calculate_q4(engine, qparams_list[3])
    # reward += calculate_q5(engine, qparams_list[4])
    # reward += calculate_q6(engine, qparams_list[5])
    # reward += calculate_q7(engine, qparams_list[6])
    # reward += calculate_q8(engine, qparams_list[7])
    # reward += calculate_q9(engine, qparams_list[8])  
    # reward += calculate_q10(engine, qparams_list[9])
    # reward += calculate_q11(engine, qparams_list[10])
    # reward += calculate_q12(engine, qparams_list[11])
    # reward += calculate_q13(engine, qparams_list[12])
    # reward += calculate_q14(engine, qparams_list[13])
    # reward += calculate_q15(engine, qparams_list[14])
    # reward += calculate_q16(engine, qparams_list[15])
    # reward += calculate_q17(engine, qparams_list[16])    
    # reward += calculate_q18(engine, qparams_list[17])
    # reward += calculate_q19(engine, qparams_list[18])
    # reward += calculate_q20(engine, qparams_list[19])
    # reward += calculate_q21(engine, qparams_list[20])
    # reward += calculate_q22(engine, qparams_list[21]) 

    # print(table_meta[6].keys)
    # print(table_meta[6].partition_cnt)
    # print(table_meta[6].partition_range)
    # print(table_meta[6].count)  
    # print(table_meta[7].keys)
    # print(table_meta[7].partition_cnt)
    # print(table_meta[7].partition_range)
    # print(table_meta[7].count)     

    reset_table_meta(table_meta)
    reward = normalize_reward(reward)

    # 评估减少的replica的列带来的reward, size大小 + tp_column_usage频率
    removed_replcas_reward = 0
    columns_size = 0
    for candidate in candidates:
        table_name = candidate['name']
        replicas = candidate['replicas']
        table_column = next((tc for tc in table_columns if tc.name == table_name), None)

        if table_name not in tp_column_usage:
            continue

        if table_column:
            missing_columns = set(table_column.columns) - set(replicas)
            for missing_column in missing_columns:  # usage * size
                if missing_column in tp_column_usage[table_name]:
                    tp_usage = tp_column_usage[table_name][missing_column]
                    column_size = table_column.columns_size[table_column.columns.index(missing_column)]
                    removed_replcas_reward += column_size * tp_usage

            missing_columns_size = sum(table_column.columns_size[table_column.columns.index(col)] for col in missing_columns)
            columns_size += missing_columns_size
            # logging.info(f"Table: {table_name}, Missing Columns: {missing_columns}, Total Size: {missing_columns_size}")
    logging.info(f"Total Missing Column Size: {columns_size}")
    logging.info(f"Total Removed Replica Reward: {removed_replcas_reward}")
    reward += (removed_replcas_reward)

    return reward    

def simulate(state, depth, max_depth=10):
    # 随机模拟直到终止状态
    state_simu = copy.deepcopy(state)

    # 获取列的查询更新信息, 设置action优先级
    qcard_list = [Q1card(), Q2card(), Q3card(), Q4card(), Q5card(), Q6card(), Q7card(), Q8card(), Q9card(), Q10card(), Q11card(), Q12card(), Q13card(), Q14card(), Q15card(), Q16card(), Q17card(), Q18card(), Q19card(), Q20card(), Q21card(), Q22card()]
    for qcard in qcard_list:
        qcard.init()           
    normalized_usage, zero_values, _, _ = get_normalized_column_usage(qcard_list, tp_column_usage)    
    
    while depth < max_depth:
        possible_actions = state_simu.get_possible_actions()
        possible_actions = state_simu.sort_actions(possible_actions, normalized_usage, zero_values)

        if not possible_actions:
            break  # 如果没有可能的动作，退出循环
        action = random.choice(possible_actions)
        state_simu = state_simu.take_action(action)
        depth += 1      
        logging.info(action)
    return calculate_reward(table_columns, table_meta, state_simu.tables)

def normalize_reward(reward):
    # 归一化
    N = 30000000000.0
    return (N - reward) / 10000000.0

def monte_carlo_tree_search(root, iterations, max_depth):
    for i in range(iterations):
        print(i)
        node = root
        reward = 0
        # 选择. 对于完全扩展的节点，选择最佳子节点，直到达到最大深度
        while node.is_fully_expanded() and node.depth < max_depth:
            # 节点没有可能的动作，退出循环              
            if not node.state.get_possible_actions():
                break
            node = node.best_child()          
        if not node.state.get_possible_actions():
            continue

        # 扩展
        if node.depth < max_depth:
            node = node.expand()

        # 模拟
        reward = simulate(node.state, node.depth, max_depth)
        # print("Reward: ", reward)
        logging.info(f"Reward: {reward}")
        # print("*****************************")
        # print("*****************************")

        # 反向传播
        while node is not None:
            node.update(reward)
            node = node.parent

def expand_root(root, max_depth):
    # 扩展根节点
    actions = root.state.get_possible_actions()
    child_nodes = []
    while not root.is_fully_expanded():
        for action in actions:
            if action not in [child.state.action for child in root.children]:
                new_state = root.state.take_action(action)
                child_node = Node(new_state, root, root.depth + 1)  # 更新子节点的深度
                root.children.append(child_node)
                logging.info(f"take action: {action}")
                logging.info(f"append child to node depth: {root.depth}")
                child_nodes.append(child_node)   

    # 计算reward
    for child_node in child_nodes:
        reward = simulate(child_node.state, child_node.depth, max_depth)
    
        # 反向传播
        while child_node is not None:
            child_node.update(reward)
            child_node = child_node.parent    
    return child_nodes

def worker_process(root, iterations, max_depth):
    local_root = copy.deepcopy(root)
    for _ in range(iterations):
        node = local_root
        # 选择. 对于完全扩展的节点，选择最佳子节点，直到达到最大深度
        while node.is_fully_expanded() and node.depth < max_depth:
            if not node.state.get_possible_actions():
                break
            node = node.best_child()
        if not node.state.get_possible_actions():
            continue

        # 扩展
        if node.depth < max_depth:
            node = node.expand()

        # 模拟
        reward = simulate(node.state, node.depth, max_depth)

        # 反向传播
        while node is not None:
            node.update(reward)
            node = node.parent

    return local_root

def parallel_monte_carlo_tree_search(root, iterations, max_depth, num_processes):
    #扩展根节点全部的一层子节点
    nodes = expand_root(root, max_depth) # [node in root.children]

    if num_processes is None:
        num_processes = cpu_count()

    iterations_per_process = iterations // num_processes
    #iterations_per_process = iterations
    nodes_per_process = len(nodes) // num_processes
    node_chunks = [nodes[i:i + nodes_per_process] for i in range(0, len(nodes), nodes_per_process)]

    with Pool(processes=num_processes) as pool:
        # results = pool.starmap(worker_process, [(root, iterations_per_process, max_depth)] * num_processes)
        pool.starmap(worker_process, [(node, iterations_per_process, max_depth) for node in nodes])

    # # 合并结果
    # for result in results:
    #     for child in result.children:
    #         if child not in root.children:
    #             root.children.append(child)
    #         else:
    #             existing_child = root.children[root.children.index(child)]
    #             existing_child.visits += child.visits
    #             existing_child.reward += child.reward

    return root


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

if __name__ == "__main__":
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

    
    # 3.构建mcts树的根节点
    tables = []
    for table_column in table_columns:
        dict_tmp = {}
        dict_tmp['name'] = table_column.name
        dict_tmp['columns'] = table_column.columns
        dict_tmp['partitionable_columns'] = table_column.partitionable_columns
        dict_tmp['partition_keys'] = table_column.partition_keys
        #dict_tmp['replicas'] = table_column.replicas
        dict_tmp['replicas'] = table_column.columns # 初始默认全表replica
        dict_tmp['replica_partition_keys'] = table_column.replica_partition_keys
        tables.append(dict_tmp)   

    # 设置日志
    setup_logging()


    # #*************************独立测试时用的代码*************************
    # initial_state = State(tables)
    # root = Node(initial_state)    
    # reward = calculate_reward(table_columns, table_meta,tables)
    # reward = normalize_reward(reward)
    # print("initial reward: ", reward)

    # #print(customer_meta.keys)
    # tables[0]['partition_keys'] = ['c_id']
    # tables[1]['partition_keys'] = ['d_id']
    # tables[2]['partition_keys'] = ['h_c_id']
    # tables[3]['partition_keys'] = ['i_id']
    # tables[4]['partition_keys'] = ['n_nationkey']
    # tables[5]['partition_keys'] = ['no_o_id']
    # tables[6]['partition_keys'] = ['ol_delivery_d']
    # tables[7]['partition_keys'] = ['o_entry_d']
    # reward = calculate_reward(table_columns, table_meta,tables)
    # reward = normalize_reward(reward)
    # print("initial reward1: ", reward)

    # #tables[0]['partition_keys'] = ['c_w_id']
    # tables[1]['partition_keys'] = ['d_id']
    # tables[2]['partition_keys'] = ['h_c_w_id']
    # tables[3]['partition_keys'] = ['i_id', 'i_im_id']
    # tables[4]['partition_keys'] = ['n_regionkey']
    # tables[5]['partition_keys'] = ['no_w_id']
    # #tables[6]['partition_keys'] = ['ol_w_id']
    # #tables[6]['partition_keys'] = ['ol_delivery_d']
    # tables[7]['partition_keys'] = ['o_all_local']
    # tables[8]['partition_keys'] = []
    # tables[9]['partition_keys'] = []
    # tables[10]['partition_keys'] = []
    # reward = calculate_reward(table_columns, table_meta,tables)
    # reward = normalize_reward(reward)
    # print("initial reward2: ", reward)    




    # 4. 进行并行化的mcts搜索
    initial_state = State(tables)
    root = Node(initial_state) 

    print('cpu_count: ', cpu_count())

    start_time = time.time()
    #parallel_monte_carlo_tree_search(root, iterations=1000, max_depth=10, num_processes=3)
    monte_carlo_tree_search(root, iterations=5000, max_depth=25)
    mcts_time = time.time() - start_time

    start_time = time.time()
    # 从根节点开始，选择最佳子节点，直到叶子节点
    node = root.best_child(c_param=0)
    node1 = copy.deepcopy(node)
    while True:
        if len(node.children) == 0:
            break        
        node = node.best_child(c_param=0)
        if (node.reward / node.visits) > (node1.reward / node1.visits):
            print("node1 copy")
            node1 = copy.deepcopy(node)         
    selection_time = time.time() - start_time

    #print("最佳分区键和副本设置:", node1.state.tables)
    #print("最佳分区键和副本设置:")
    # 使用 json.dumps 格式化输出
    formatted_output = json.dumps(node1.state.tables, indent=4, ensure_ascii=False)
    # 将格式化后的输出写入到文件
    with open('Output/best_advisor_separated1.txt', 'w', encoding='utf-8') as f:
        f.write(formatted_output)

    print("最佳收益:", node1.reward / node1.visits)
    print("访问次数:", node1.visits)    
    print("层数:", node1.depth)

    print(f"蒙特卡洛树搜索时间: {mcts_time:.2f}秒")
    print(f"选择最佳子节点时间: {selection_time:.2f}秒")


    logging.info("最佳收益:", node1.reward / node1.visits)
    logging.info("访问次数:", node1.visits)    
    logging.info("层数:", node1.depth)
    logging.info("最佳配置:")
    logging.info(formatted_output)


    
    for table, initial_table in zip(node1.state.tables, initial_state.tables):
        logging.info(f"{table['name']}:")
        action_partition_keys = set(table['partition_keys'])
        action_replicas = set(initial_table['replicas']) - set(table['replicas'])
        action_replica_partition_keys = set(table['replica_partition_keys'])

        logging.info(f"partition_keys: {action_partition_keys}")
        logging.info(f"remove replicas: {action_replicas}")
        logging.info(f"replica_partition_keys: {action_replica_partition_keys}")


    # #*************************独立测试时用的代码*************************
    # # 3. 根据分区副本情况更新元数据
    # candidates = [{'name': 'customer', 'partition_keys': ['c_id', 'c_w_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}, {'name': 'order_line', 'partition_keys': ['ol_i_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}]

    # tables[0]['partition_keys'] = ['c_w_id']
    # tables[1]['partition_keys'] = ['d_id']
    # tables[2]['partition_keys'] = ['h_c_w_id']
    # tables[3]['partition_keys'] = ['i_id', 'i_im_id']
    # tables[4]['partition_keys'] = ['n_regionkey']
    # tables[5]['partition_keys'] = ['no_w_id']
    # #tables[6]['partition_keys'] = ['ol_w_id']
    # tables[6]['partition_keys'] = ['ol_delivery_d']
    # tables[7]['partition_keys'] = ['o_all_local']    

    # update_meta(table_columns, table_meta, tables)

    # # get Qcard
    # qcard_list = get_qcard(table_meta)

    # # update Qparams
    # qparams_list = update_qparams_with_qcard(qcard_list)

    # # calculate query cost
    # engine = 'Tiflash'
    
    # reward = calculate_reward(table_columns, table_meta,tables)
    # reward = normalize_reward(reward)
    # print("initial reward: ", reward)

    # print(calculate_q1(engine, qparams_list[0]))
    # print(calculate_q2(engine, qparams_list[1]))
    # print(calculate_q3(engine, qparams_list[2]))
    # print(calculate_q4(engine, qparams_list[3]))
    # print(calculate_q5(engine, qparams_list[4]))
    # print(calculate_q6(engine, qparams_list[5]))
    # print(calculate_q7(engine, qparams_list[6]))
    # print(calculate_q8(engine, qparams_list[7]))
    # print(calculate_q9(engine, qparams_list[8]))  
    # print(calculate_q10(engine, qparams_list[9]))
    # print(calculate_q11(engine, qparams_list[10]))
    # print(calculate_q12(engine, qparams_list[11]))
    # print(calculate_q13(engine, qparams_list[12]))
    # print(calculate_q14(engine, qparams_list[13]))
    # print(calculate_q15(engine, qparams_list[14]))
    # print(calculate_q16(engine, qparams_list[15]))
    # print(calculate_q17(engine, qparams_list[16]))    
    # print(calculate_q18(engine, qparams_list[17]))
    # print(calculate_q19(engine, qparams_list[18]))
    # print(calculate_q20(engine, qparams_list[19]))
    # print(calculate_q21(engine, qparams_list[20]))
    # print(calculate_q22(engine, qparams_list[21]))