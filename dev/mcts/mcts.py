import math
import random
import copy
from fpdf import FPDF
import time

class State:
    def __init__(self, tables, action=None):
        self.tables = tables # 记录每个表的分区键和副本情况
        self.action = action # 即将执行的三元组(action_type, table_name, column_name)

    def get_possible_actions(self):
        # 获取可能的动作（选择分区键或设置副本）
        actions = []
        for table in self.tables:
            partition_candidates = set(table['partitionable_columns']) - set(table['replicas'])
            replica_candidates = set(table['columns']) - set(table['partition_keys'])
            replica_partition_candidates = set(table['partitionable_columns']) & set(table['replicas'])
            #print("partition_candidates:", partition_candidates)
            #print("replica_partition_candidates:", replica_partition_candidates)
            for column in partition_candidates:
                if column not in table['partition_keys']:
                    actions.append(('partition', table['name'], column))
            for column in replica_partition_candidates:
                if column not in table['replica_partition_keys']:
                    actions.append(('replica_partition', table['name'], column))
            for column in replica_candidates:
                if column not in table['replicas']:
                    actions.append(('replica', table['name'], column))
        random.shuffle(actions)  # 打乱actions的顺序
        return actions

    def take_action(self, action):
        # 返回新的状态给新节点
        new_tables = [copy.deepcopy(table) for table in self.tables]
        for table in new_tables:
            if table['name'] == action[1]:
                if action[0] == 'partition':
                    table['partition_keys'].append(action[2])
                elif action[0] == 'replica':
                    table['replicas'].append(action[2])
                elif action[0] == 'replica_partition':
                    table['replica_partition_keys'].append(action[2])
        return State(new_tables, action)

    # def is_terminal(self):
    #     # 判断是否为终止状态
    #     for table in self.tables:
    #         if not table['partition_keys'] or not table['replicas']:
    #             return False
    #     return True

    def get_reward(self):
        # 计算当前状态的收益
        return calculate_reward(self.tables)

class Node:
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

    def best_child(self, c_param=1):
        # 使用UCB1策略选择最佳子节点
        if not self.children:
            print(self.state.tables)
            raise ValueError("No children to select from")
        choices_weights = [
            (child.reward / child.visits) + c_param * math.sqrt((2 * math.log(self.visits) / child.visits))
            for child in self.children
        ]
        return self.children[choices_weights.index(max(choices_weights))]

    def expand(self):
        # 扩展节点
        actions = self.state.get_possible_actions()
        print("expand node depth:", self.depth)
        #print("actions:", actions)
        for action in actions:
            if action not in [child.state.action for child in self.children]:
                new_state = self.state.take_action(action)
                child_node = Node(new_state, self, self.depth + 1)  # 更新子节点的深度
                self.children.append(child_node)
                print("take action:", action)
                print("append child to node depth:", self.depth)
                return child_node
        raise Exception("Should never reach here")

    def update(self, reward):
        # 更新节点的访问次数和奖励
        self.visits += 1
        self.reward += reward

def calculate_reward(tables):
    # 假设的收益计算模型
    reward = 0
    for table in tables:
        if 'col1' in table['partition_keys']:
            reward += 10
        if 'col2' in table['partition_keys']:
            reward += 5
        if 'col3' in table['partition_keys']:
            reward += -2
        if 'col1' in table['replica_partition_keys']:
            reward += 0
        if 'col2' in table['replica_partition_keys']:
            reward += 3
        if 'col3' in table['replica_partition_keys']:
            reward += 10            
        if 'col1' in table['replicas']:
            reward += 5
        if 'col2' in table['replicas']:
            reward += 5
        if 'col3' in table['replicas']:
            reward += 10
        #reward += len(table['partition_keys']) * 10 + len(table['replicas']) * 5
    return reward

def monte_carlo_tree_search(root, iterations=1000, max_depth=10):
    best_node = None
    best_reward = 0
    for i in range(iterations):
        node = root
        # 选择. 对于完全扩展的节点，选择最佳子节点，直到达到最大深度
        while node.is_fully_expanded() and node.depth < max_depth:
            # print("node fully expanded:", node.depth)
            # 判断节点是否是叶结点
            if len(node.state.get_possible_actions()) == 0:
                break
            node = node.best_child()
        if len(node.state.get_possible_actions()) == 0:
            continue          

        # 扩展
        if node.depth < max_depth:
            node = node.expand()
            if node.state.get_reward() >= best_reward:
                best_node = copy.deepcopy(node)
                best_reward = node.state.get_reward()
        
        # 模拟
        #print("node partition keys: ", node.state.tables[0]['partition_keys'])
        reward = simulate(node.state, node.depth, max_depth)
        #print("node partition keys: ", node.state.tables[0]['partition_keys'])
        
        # 反向传播
        while node is not None:
            node.update(reward)
            node = node.parent        
    
    return best_node, best_reward

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
    #print("simulate +1")
    #print(possible_actions)        
    return state.get_reward()

def print_tree(node, indent="", output=[]):
    output.append(f"{indent}Node: Visits={node.visits}, Reward={node.reward}, State={node.state.tables}, Depth={node.depth}\n")
    for child in node.children:
        print_tree(child, indent + "  ", output)
    return output

def save_tree_to_pdf(tree_output, filename="tree_structure.pdf"):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_font("Arial", size=12)
    for line in tree_output:
        pdf.multi_cell(0, 10, line)
    pdf.output(filename)

def save_tree_to_txt(tree_output, filename="tree_structure.txt"):
    with open(filename, "w") as file:
        file.writelines(tree_output)

if __name__ == "__main__":
    # 示例表结构
    tables = [
        {'name': 'table1', 'columns': ['col1', 'col2', 'col3'], 'partitionable_columns': ['col1', 'col2', 'col3'], 'partition_keys': [], 'replicas': [], 'replica_partition_keys': []},
        {'name': 'table2', 'columns': ['col1', 'col2', 'col3'], 'partitionable_columns': ['col1', 'col2', 'col3'], 'partition_keys': [], 'replicas': [], 'replica_partition_keys': []},
        # 其他表...
    ]

    initial_state = State(tables)
    root = Node(initial_state)

    start_time = time.time()
    best_node, best_reward = monte_carlo_tree_search(root, iterations=40000)
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

    print("最佳分区键和副本设置:", node1.state.tables)
    print("最佳收益:", node1.reward)
    print("最佳收益:", node1.state.get_reward())

    # # 搜索过程中维护的最佳节点
    # print("最佳分区键和副本设置:", best_node.state.tables)
    # print("最佳收益:", best_node.reward)
    # print("最佳收益:", best_node.state.get_reward())

    start_time = time.time()
    print("\n蒙特卡洛树结构:")
    tree_output = print_tree(root)
    save_tree_to_txt(tree_output)
    output_time = time.time() - start_time

    print(f"蒙特卡洛树搜索时间: {mcts_time:.2f}秒")
    print(f"选择最佳子节点时间: {selection_time:.2f}秒")
    print(f"输出树结构时间: {output_time:.2f}秒")