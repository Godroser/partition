import math
import random

class PartitionReplicaState:
    def __init__(self, tables, partition_keys=None, replicas=None):
        # 初始化分区副本状态
        self.tables = tables
        self.partition_keys = partition_keys if partition_keys else [None] * len(tables)
        self.replicas = replicas if replicas else [False] * len(tables)

    def get_legal_actions(self):
        # 获取合法的操作
        actions = []
        for i, table in enumerate(self.tables):
            if self.partition_keys[i] is None:
                actions.extend([(i, 'partition', col) for col in table['columns'] if col in table['partitionable_columns']])
            if not self.replicas[i]:
                actions.extend([(i, 'replica', col) for col in table['columns']])
        return actions

    def next_state(self, action):
        # 根据操作生成下一个状态
        table_idx, action_type, column = action
        new_partition_keys = self.partition_keys[:]
        new_replicas = self.replicas[:]
        if action_type == 'partition':
            new_partition_keys[table_idx] = column
        elif action_type == 'replica':
            new_replicas[table_idx] = True
        return PartitionReplicaState(self.tables, new_partition_keys, new_replicas)

    def is_terminal(self):
        # 判断是否为终止状态
        return all(key is not None for key in self.partition_keys) and all(self.replicas)

    def reward(self):
        # 模拟计算收益的模型
        res = 0
        for i in range(len(self.partition_keys)):
            if self.partition_keys[i] is not None:
                res += 0
            if self.replicas[i]:
                res += 1
        return res
        #return random.random()

class MCTSNode:
    def __init__(self, state, parent=None):
        # 初始化MCTS节点
        self.state = state
        self.parent = parent
        self.children = []
        self.visits = 0
        self.value = 0

    def is_fully_expanded(self):
        # 判断节点是否完全扩展
        return len(self.children) == len(self.state.get_legal_actions())

    def best_child(self, c_param=1.4):
        # 选择最佳子节点
        choices_weights = [
            (child.value / child.visits) + c_param * math.sqrt((2 * math.log(self.visits) / child.visits))
            for child in self.children
        ]
        return self.children[choices_weights.index(max(choices_weights))]

    def expand(self):
        # 扩展节点
        actions = self.state.get_legal_actions()
        for action in actions:
            if action not in [child.state for child in self.children]:
                next_state = self.state.next_state(action)
                child_node = MCTSNode(next_state, parent=self)
                self.children.append(child_node)
                return child_node
        raise Exception("Should not reach here")

    def rollout(self):
        # 模拟随机游戏直到终止状态
        current_rollout_state = self.state
        while not current_rollout_state.is_terminal():
            possible_moves = current_rollout_state.get_legal_actions()
            action = self.rollout_policy(possible_moves)
            print("Action: ", action)
            current_rollout_state = current_rollout_state.next_state(action)
        return current_rollout_state.reward()

    def rollout_policy(self, possible_moves):
        # 随机选择一个操作
        return random.choice(possible_moves)

    def backpropagate(self, result):
        # 反向传播结果
        self.visits += 1
        self.value = result  # 将收益赋予当前节点的value
        if self.parent:
            if self.value > self.parent.value:  # 如果当前节点的value大于父节点的value
                self.parent.value = self.value  # 将当前节点的value赋予父节点的value
            self.parent.backpropagate(result)

def mcts(root, iterations):
    # 蒙特卡洛树搜索主循环
    for i in range(iterations):
        node = tree_policy(root)
        reward = node.rollout()
        print("Iteration: ", i, "Reward: ", reward)
        node.backpropagate(reward)
    
    # 输出得分最高的一个节点
    best_node, result_node = find_best_node(root, root)
    return best_node

def find_best_node(node, result_node):
    # 递归寻找得分最高的节点
    if not node.children:
        return node, result_node
    best_child = max(node.children, key=lambda child: child.value)
    if best_child.value > result_node.value:
        result_node = best_child
    return find_best_node(best_child, result_node)

def tree_policy(node):
    # 树策略：选择节点进行扩展或模拟
    while not node.state.is_terminal():
        if not node.is_fully_expanded():
            return node.expand()
        else:
            node = node.best_child()
    return node

if __name__ == "__main__":
    tables = [
        {'columns': ['col1', 'col2', 'col3'], 'partitionable_columns': ['col1', 'col2']},
        # 假设有12个表，每个表有若干列
        # ...
    ] * 12

    initial_state = PartitionReplicaState(tables)
    root = MCTSNode(initial_state)
    best_node = mcts(root, 10)

    print("Best partition keys:", best_node.state.partition_keys)
    print("Best replicas:", best_node.state.replicas)