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

from mcts.mcts import Node, State
from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.ch_query_cost import *
from estimator.ch_columns_ranges_meta import *
from advisor import update_meta, update_rowsize, reset_table_meta, normalize_reward
from config import Config
from log.logging_config import setup_logging
from workload.workload_analyzer import get_normalized_column_usage, tp_column_usage

class IncrementalState(State):
    def __init__(self, tables, action=None, possible_actions=None):
        super().__init__(tables, action)
        self.possible_actions = possible_actions  # 预定义的可能的actions集合

    def get_possible_actions(self):
        # 获取可能的动作 (add/remove partition key应该确保不是全量replica, add/remove replica应该是否有相应的partition可以设置/移除副本)
        actions = []
        for action in self.possible_actions:
            table = next((t for t in self.tables if t['name'] == action[1]), None)
            if table:
                if action[0] == 'add partition key':
                    if table['replicas'] == table['columns']: ##说明全量replica了
                        continue
                    else:                  
                        actions.append(action)
                if action[0] == 'remove partition key':
                    if table['replicas'] == table['columns']: ##说明全量replica了
                        continue
                    else:
                        if table['partition_keys']:
                            actions.append(action)
                elif action[0] == 'add replica':
                    if table['replicas'] == []: ##说明没有列存副本
                        actions.append(action)
                elif action[0] == 'remove replica':
                    if table['replicas'] != []: ##说明有列存副本
                        actions.append(action)
        logging.info(f"possible actions: {actions}")
        return actions

    def take_action(self, action):
        # 返回新的状态给新节点
        new_tables = [copy.deepcopy(table) for table in self.tables]
        for table in new_tables:
            if table['name'] == action[1]:
                if action[0] == 'add partition key':
                    table['partition_keys'].append(action[2])
                elif action[0] == 'remove partition key':
                    print(f"remove partition key: {action[2]} {table['partition_keys']}")
                    table['partition_keys'].remove(action[2])
                elif action[0] == 'add replica': # 将整个表的列都设置成replicas
                    table['replicas'] = table['columns']
                    table['partition_keys'] = []
                elif action[0] == 'remove replica': # 将整个表的replicas都清空
                    table['replicas'] = []
                    table['replica_partition_keys'] = []
        logging.info(f"take action: {action}")
        return State(new_tables, action)

    def get_reward(self):
        # 计算当前状态的收益
        return calculate_reward(self.tables)
    
class IncrementalNode(Node):
    def __init__(self, state, parent=None, depth=0):
        self.state = state
        self.parent = parent
        self.children = []
        self.visits = 0
        self.reward = 0
        self.depth = depth  # 添加深度属性

    def is_fully_expanded(self):
        # 判断节点是否已经完全扩展. 即是否所有可能的动作都已经尝试过
        return len(self.children) == len(self.state.get_possible_actions())        

    def expand(self):
        # 扩展节点
        actions = self.state.get_possible_actions()

        #print("expand node depth:", self.depth)
        #print("actions:", actions)
        for action in actions:
            if action not in [child.state.action for child in self.children]:
                new_state = self.state.take_action(action)
                child_node = IncrementalNode(new_state, self, self.depth + 1)  # 更新子节点的深度
                self.children.append(child_node)
                #print("take action:", action)
                # print("append child to node depth:", self.depth)
                # print("child action:", action)
                logging.info(f"append child to node depth: {self.depth}")
                logging.info(f"child action: {action}")
                return child_node
        logging.info(f"actions: {actions}")
        raise Exception("Should never reach here")

    
Table_Size = {
    'customer': 3608139745,
    'district': 223071,
    'history': 406510068,
    'item': 8269834,
    'nation': 2468,
    'new_order': 24408907,
    'order_line': 4741652802,
    'orders': 284034108,
    'region': 513,
    'stock': 6647360990,
    'supplier': 1509310,
    'warehouse': 21167,
}
    
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

    # 计算数据重分布所需要的代价
    data_reorg_cost = 0
    coefficient_load = 10.0  # 系数
    coefficient_replica = 0.001  # 系数

    # 获取初始状态的表配置
    initial_tables = []
    file_path = "/data3/dzh/project/grep/dev/Output/manual_advisor.txt"
    with open(file_path, 'r') as file:
        candidate_file = json.load(file)
    
    for table_info in candidate_file:
        initial_tables.append(table_info)          

    # 遍历candidates的每一个表，与初始状态对比
    for candidate, initial_table in zip(candidates, initial_tables):
        table_name = candidate['name']
        table_column = next((tc for tc in table_columns if tc.name == table_name), None)
        
        if not table_column:
            continue

        # 计算表的大小
        table_size = Table_Size[table_name]

        # 检查replicas是否发生变化
        if set(candidate['replicas']) != set(initial_table['replicas']):
            # 增加/移除列存副本的代价
            data_reorg_cost += coefficient_replica * table_size
            logging.info(f"Table {table_name} replicas changed, cost: {coefficient_replica * table_size}")
        # 如果replicas没变，检查partition_keys是否变化
        elif set(candidate['partition_keys']) != set(initial_table['partition_keys']):
            # 重新导入数据的代价
            data_reorg_cost += coefficient_load  * table_size
            logging.info(f"Table {table_name} partition keys changed, cost: {coefficient_load * table_size}")

    logging.info(f"Total data reorganization cost: {data_reorg_cost}")
    reward += data_reorg_cost

    return reward

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
        logging.info(f"Reward: {reward}")

        # 反向传播
        while node is not None:
            node.update(reward)
            node = node.parent

def simulate(state, depth, max_depth=3):
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

def expand_root(root, max_depth):
    # 扩展根节点
    actions = root.state.get_possible_actions()
    child_nodes = []
    while not root.is_fully_expanded():
        for action in actions:
            if action not in [child.state.action for child in root.children]:
                new_state = root.state.take_action(action)
                child_node = IncrementalNode(new_state, root, root.depth + 1)  # 更新子节点的深度
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

    # 设置日志
    # setup_logging()

    # 3. 构建mcts树的根节点，使用预定义的配置
    tables = []
    file_path = "/data3/dzh/project/grep/dev/Output/manual_advisor.txt"
    with open(file_path, 'r') as file:
        candidate_file = json.load(file)
    
    for table_info in candidate_file:
        tables.append(table_info)   

    # 4. 预定义可能的actions集合
    # 受影响的表和列
    involved_columns = ['c_id', 'c_name', 'c_phone']
    involved_tables = ['customer']

    possible_actions = [
        # 示例actions，实际使用时需要根据具体需求定义
        ('add partition key', 'customer', 'c_id'), # 分区
        ('remove partition key', 'customer', 'c_id'), # 移除分区
        ('add replica', 'customer'), # 分区副本
        ('remove replica', 'customer'), # 移除分区副本
        ('add replica', 'district'),
        ('remove replica', 'district'),
        ('add replica', 'history'),
        ('remove replica', 'history'),
        ('add replica', 'item'),
        ('remove replica', 'item'),
        ('add replica', 'nation'),
        ('remove replica', 'nation'),
        ('add replica', 'new_order'),
        ('remove replica', 'new_order'),
        ('add replica', 'order_line'),
        ('remove replica', 'order_line'),
        ('add replica', 'orders'),
        ('remove replica', 'orders'),
        ('add replica', 'region'),
        ('remove replica', 'region'),
        ('add replica', 'stock'),
        ('remove replica', 'stock'),
        ('add replica', 'supplier'),
        ('remove replica', 'supplier'),
        ('add replica', 'warehouse'),
        ('remove replica', 'warehouse'),
    ]

    # 5. 创建增量版本的State
    initial_state = IncrementalState(tables, possible_actions=possible_actions)
    root = IncrementalNode(initial_state)

    # 6. 进行MCTS搜索
    start_time = time.time()
    monte_carlo_tree_search(root, iterations=20, max_depth=5)
    mcts_time = time.time() - start_time

    # 7. 选择最佳配置
    start_time = time.time()
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

    # 8. 输出结果
    formatted_output = json.dumps(node1.state.tables, indent=4, ensure_ascii=False)
    with open('Output/best_advisor_incremental.txt', 'w', encoding='utf-8') as f:
        f.write(formatted_output)

    # 9. 比较节点, 获取状态变化的action
    required_actions = []
    
    # 获取初始状态和最终状态
    initial_tables = []
    file_path = "/data3/dzh/project/grep/dev/Output/manual_advisor.txt"
    with open(file_path, 'r') as file:
        initial_tables = json.load(file)
    
    final_tables = node1.state.tables
    
    # 遍历每个表，比较状态变化
    for initial_table, final_table in zip(initial_tables, final_tables):
        table_name = initial_table['name']
        
        # 比较replicas的变化
        initial_replicas = set(initial_table['replicas'])
        final_replicas = set(final_table['replicas'])
        
        if initial_replicas != final_replicas:
            if len(final_replicas) > len(initial_replicas):
                # replicas增加了，说明是add replica
                required_actions.append(('add replica', table_name))
            else:
                # replicas减少了，说明是remove replica
                required_actions.append(('remove replica', table_name))
        else:
            # replicas没变，比较partition_keys
            initial_partition_keys = set(initial_table['partition_keys'])
            final_partition_keys = set(final_table['partition_keys'])
            
            # 找出新增的分区键
            added_keys = final_partition_keys - initial_partition_keys
            for key in added_keys:
                required_actions.append(('add partition key', table_name, key))
            
            # 找出移除的分区键
            removed_keys = initial_partition_keys - final_partition_keys
            for key in removed_keys:
                required_actions.append(('remove partition key', table_name, key))
    
    # 输出所需的actions
    print("\nRequired actions to transform from initial state to final state:")
    for action in required_actions:
        print(f"Action: {action}")
    
    # # 将actions写入文件
    # with open('Output/required_actions.txt', 'w') as f:
    #     for action in required_actions:
    #         f.write(f"{action}\n")

    print("最佳收益:", node1.reward / node1.visits)
    print("访问次数:", node1.visits)    
    print("层数:", node1.depth)
    print(f"蒙特卡洛树搜索时间: {mcts_time:.2f}秒")
    print(f"选择最佳子节点时间: {selection_time:.2f}秒")
