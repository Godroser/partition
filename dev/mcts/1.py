import math
import random
import copy

class State:
    def __init__(self, tables, action=None):
        self.tables = tables # 记录每个表的分区键和副本情况
        self.action = action # 即将执行的三元组(action_type, table_name, column_name)

    def get_possible_actions(self):
        # 获取可能的动作（选择分区键或设置副本）
        actions = []
        for table in self.tables:
            for column in table['columns']:
                if column not in table['partition_keys']:
                    actions.append(('partition', table['name'], column))
                if column not in table['replicas']:
                    actions.append(('replica', table['name'], column))
        return actions

    def take_action(self, action):
        # 返回新的状态给新节点，但是不修改self.tables
        new_tables = [copy.deepcopy(table) for table in self.tables]
        for table in new_tables:
            if table['name'] == action[1]:
                if action[0] == 'partition':
                    table['partition_keys'].append(action[2])
                elif action[0] == 'replica':
                    table['replicas'].append(action[2])
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

    def best_child(self, c_param=1.4):
        # 使用UCB1策略选择最佳子节点
        choices_weights = [
            (child.reward / child.visits) + c_param * math.sqrt((2 * math.log(self.visits) / child.visits))
            for child in self.children
        ]
        return self.children[choices_weights.index(max(choices_weights))]

    def expand(self):
        # 扩展节点
        actions = self.state.get_possible_actions()
        print("expand node depth:", self.depth)
        print("actions:", actions)
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
        self.reward = reward

def calculate_reward(tables):
    # 假设的收益计算模型
    reward = 0
    for table in tables:
        reward += len(table['partition_keys']) * 10 + len(table['replicas']) * 5
    return reward

def monte_carlo_tree_search(root, iterations=1000, max_depth=10):
    best_node = None
    best_reward = 0
    for _ in range(iterations):
        node = root
        # 选择
        # 选择最佳子节点，直到达到最大深度或节点完全扩展
        while node.is_fully_expanded() and node.depth < max_depth:
            print("node fully expanded:", node.depth)
            node = node.best_child()
        # 扩展
        if node.depth < max_depth:
            node = node.expand()
            if node.state.get_reward() >= best_reward:
                best_node = node
                best_reward = node.state.get_reward()
        # 模拟
        #print("node partition keys: ", node.state.tables[0]['partition_keys'])
        reward = simulate(node.state, node.depth, max_depth)
        #print("node partition keys: ", node.state.tables[0]['partition_keys'])
        # 反向传播
        while node is not None and reward >= node.reward:
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

def print_tree(node, indent=""):
    print(f"{indent}Node: Visits={node.visits}, Reward={node.reward}, Action={node.state.action}")
    for child in node.children:
        print_tree(child, indent + "  ")

if __name__ == "__main__":
    # 示例表结构
    tables = [
        {'name': 'table1', 'columns': ['col1', 'col2', 'col3'], 'partition_keys': [], 'replicas': []},
        {'name': 'table2', 'columns': ['col1', 'col2', 'col3'], 'partition_keys': [], 'replicas': []},
        # 其他表...
    ]

    initial_state = State(tables)
    root = Node(initial_state)
    best_node, best_reward = monte_carlo_tree_search(root, iterations=10000)

    print("最佳分区键和副本设置:", best_node.state.tables)
    print("最佳收益:", best_node.reward)
    print("最佳收益:", best_node.state.get_reward())
    # print("\n蒙特卡洛树结构:")
    # print_tree(root)