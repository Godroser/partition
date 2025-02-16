import math
import sys
import os
import time
import copy
import random
import json
from datetime import datetime, timedelta
from multiprocessing import Pool, cpu_count

#sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from mcts.mcts import Node, State
from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.ch_query_cost import *
from estimator.ch_columns_ranges_meta import *
from config import Config

# update metadata given the partition and replica candidate
# candidate format:{'name': , 'columns':, 'partitionable_columns': , 'partition_keys': [], 'replicas': [], 'replica_partition_keys': []}
def update_meta(table_columns, table_meta, candidates):
    table_dict = {'customer': 0, 'district': 1, 'history': 2, 'item': 3, 'nation': 4, 'new_order': 5, 'order_line': 6, 'orders': 7, 'region': 8, 'stock': 9, 'supplier': 10, 'warehouse': 11}

    # 每一个candidate都是一个表的分区和副本设置
    for candidate in candidates:
        # get tables key min and max value
        table_name = candidate['name']
        idx = table_dict.get(table_name)

        # 获取表的分区键的最大最小值
        # [[],[]] minmaxvalue[i][0]: min value, minmaxvalue[i][1]: max value
        minmaxvalues = table_columns[idx].get_keys_ranges(candidate['partition_keys'])  

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
                    step = (max_val - min_val) // 4
                    table_ranges.append([min_val + i * step for i in range(1, 5)])
                # 检查是否为字符串类型并尝试转换为整数
                elif isinstance(minmaxvalue[0], str) and isinstance(minmaxvalue[1], str):
                    min_val = int(minmaxvalue[0])
                    max_val = int(minmaxvalue[1])
                    # 对于整型，直接均匀分成四份
                    step = (max_val - min_val) // 4
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
                    step = total_days // 4
                    table_ranges.append([min_val + timedelta(days=i * step) for i in range(1, 5)])
                except ValueError:
                    raise ValueError(f"Unsupported type for partition keys: {minmaxvalue}")

        #print('table_ranges: ', table_ranges)
        # update partition metadata
        if len(table_ranges) == 0: # 没有选分区键
            #print("Table {} no partition keys".format(table_name))
            return
        
        # 更新meta类的分区元信息
        table_meta[idx].update_partition_metadata(candidate['partition_keys'], table_ranges)

# 计算候选的reward
# candidates是每个表的分区和副本设置
def calculate_reward(table_columns, table_meta, candidates):
    # 根据分区副本情况更新元数据    
    update_meta(table_columns, table_meta, candidates)

    # get Qcard 获取每个query扫描对应表的card
    qcard_list = get_qcard(table_meta)
    # qcard_list = get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    # update Qparams 将qcard复制到qparams
    qparams_list = update_qparams_with_qcard(qcard_list)

    # calculate query cost
    engine = 'Tiflash'
    
    reward = 0.0
    reward += calculate_q1(engine, qparams_list[0])
    reward += calculate_q2(engine, qparams_list[1])
    reward += calculate_q3(engine, qparams_list[2])
    reward += calculate_q4(engine, qparams_list[3])
    reward += calculate_q5(engine, qparams_list[4])
    reward += calculate_q6(engine, qparams_list[5])
    reward += calculate_q7(engine, qparams_list[6])
    reward += calculate_q8(engine, qparams_list[7])
    reward += calculate_q9(engine, qparams_list[8])  
    reward += calculate_q10(engine, qparams_list[9])
    reward += calculate_q11(engine, qparams_list[10])
    reward += calculate_q12(engine, qparams_list[11])
    reward += calculate_q13(engine, qparams_list[12])
    reward += calculate_q14(engine, qparams_list[13])
    reward += calculate_q15(engine, qparams_list[14])
    reward += calculate_q16(engine, qparams_list[15])
    reward += calculate_q17(engine, qparams_list[16])    
    reward += calculate_q18(engine, qparams_list[17])
    reward += calculate_q19(engine, qparams_list[18])
    reward += calculate_q20(engine, qparams_list[19])
    reward += calculate_q21(engine, qparams_list[20])
    reward += calculate_q22(engine, qparams_list[21]) 

    # print(table_meta[6].keys)
    # print(table_meta[6].partition_cnt)
    # print(table_meta[6].partition_range)
    # print(table_meta[6].count)  
    # print(table_meta[7].keys)
    # print(table_meta[7].partition_cnt)
    # print(table_meta[7].partition_range)
    # print(table_meta[7].count)     
    reset_table_meta(table_meta)
    # print(table_meta[0].keys)
    # print(table_meta[0].partition_cnt)
    # print(table_meta[0].partition_range)
    #print("reward: ", reward)
    return reward    

def simulate(state, depth, max_depth=10):
    # 随机模拟直到终止状态
    state_simu = copy.deepcopy(state)
    while depth < max_depth:
        possible_actions = state_simu.get_possible_actions()
        if not possible_actions:
            break  # 如果没有可能的动作，退出循环
        action = random.choice(possible_actions)
        state_simu = state_simu.take_action(action)
        depth += 1      
        #print(action)
    return calculate_reward(table_columns, table_meta, state_simu.tables)

def normalize_reward(reward):
    # 归一化
    N = 3000000000.0
    return (N - reward) / 1000000.0

def monte_carlo_tree_search(root, iterations, max_depth):
    for i in range(iterations):
        print(i)
        node = root
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
        #print("Reward: ", reward)       
        reward = normalize_reward(reward)
        print("Reward: ", reward)
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
                print("take action:", action)
                print("append child to node depth:", root.depth)
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
        dict_tmp['replicas'] = table_column.replicas
        dict_tmp['replica_partition_keys'] = table_column.replica_partition_keys
        tables.append(dict_tmp)

    # ##独立测试的代码
    # initial_state = State(tables)
    # root = Node(initial_state)    
    # print("initial reward: ", calculate_reward(table_columns, table_meta, root.state.tables))

    # print(customer_meta.keys)
    # tables[0]['partition_keys'] = ['c_id']
    # tables[1]['partition_keys'] = ['d_id']
    # tables[2]['partition_keys'] = ['h_c_id']
    # tables[3]['partition_keys'] = ['i_id']
    # tables[4]['partition_keys'] = ['n_nationkey']
    # tables[5]['partition_keys'] = ['no_o_id']
    # tables[6]['partition_keys'] = ['ol_delivery_d']
    # tables[7]['partition_keys'] = ['o_entry_d']
    # print("initial reward1: ", calculate_reward(table_columns, table_meta,tables))
    # print(customer_meta.keys)
   




    # 4. 进行并行化的mcts搜索
    initial_state = State(tables)
    root = Node(initial_state) 

    print('cpu_count: ', cpu_count())

    start_time = time.time()
    #parallel_monte_carlo_tree_search(root, iterations=1000, max_depth=10, num_processes=3)
    monte_carlo_tree_search(root, iterations=300, max_depth=10)
    mcts_time = time.time() - start_time

    start_time = time.time()
    # 从根节点开始，选择最佳子节点，直到叶子节点
    node = root.best_child(c_param=0)
    node1 = copy.deepcopy(node)
    while True:
        if len(node.children) == 0:
            break        
        node = node.best_child(c_param=0)
        if node.state.get_reward() > node1.state.get_reward():
            node1 = copy.deepcopy(node)
    selection_time = time.time() - start_time

    #print("最佳分区键和副本设置:", node1.state.tables)
    print("最佳分区键和副本设置:")
    # 使用 json.dumps 格式化输出
    formatted_output = json.dumps(node1.state.tables, indent=4, ensure_ascii=False)
    # 将格式化后的输出写入到文件
    with open('Output/best_advisor.txt', 'w', encoding='utf-8') as f:
        f.write(formatted_output)

    print("最佳收益:", node1.reward)
    print("最佳收益:", node1.state.get_reward())    

    print(f"蒙特卡洛树搜索时间: {mcts_time:.2f}秒")
    print(f"选择最佳子节点时间: {selection_time:.2f}秒")
    


    # #独立测试时用的代码
    # # 3. 根据分区副本情况更新元数据
    # candidates = [{'name': 'customer', 'partition_keys': ['c_id', 'c_w_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}, {'name': 'order_line', 'partition_keys': ['ol_i_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}]

    # update_meta(table_columns, table_meta, candidates)

    # # get Qcard
    # qcard_list = get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    # # update Qparams
    # qparams_list = update_qparams_with_qcard(qcard_list)

    # # calculate query cost
    # engine = 'Tiflash'
    
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