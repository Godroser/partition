class TreeNode:
    def __init__(self, content):
        self.content = content
        self.children = []
        self.cost_formula = None  # 执行代价公式，可以根据名称来定义
        self.cost = None  # 代价值       

    def add_child(self, child_node):
        self.children.append(child_node)
 
    def set_cost_formula(self, cost_formula):
        self.cost_formula = cost_formula

    def calculate_cost(self):
        if self.cost_formula:
            # 执行代价公式的计算，可以根据具体公式来实现
            self.cost = self.cost_formula(self)
        else:
            self.cost = 0  # 如果没有代价公式，则代价为0

    def __repr__(self):
        return f"TreeNode(content={self.content})"


def parse_query_tree(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = file.readlines()

    queries = []
    current_root = None
    stack = []  # Stack to maintain nodes at each level

    for line in lines:
        line = line.rstrip()  # Strip newline and trailing spaces
        if not line:  # Empty line indicates end of a query
            if current_root:
                queries.append(current_root)
            current_root = None
            stack = []
            continue

        if line[0].isalpha():  # Start of a new query
            current_root = TreeNode(line)
            stack = [current_root]  # Reset the stack with the new root
        else:
            # Find the depth based on the position of └─ or ├─
            index = line.find("└─") if "└─" in line else line.find("├─")
            #print(index)
            if index != -1:
                depth = index // 2 + 1
                while len(stack) <= depth:
                    stack.append(None)  # Ensure stack has enough depth
                current_node = TreeNode(line)
                parent_node = stack[depth - 1] if depth > 0 else None
                #print(parent_node.children)
                if parent_node:
                    parent_node.add_child(current_node)
                stack[depth] = current_node

    if current_root:  # Add the last query if the file doesn't end with a blank line
        queries.append(current_root)

    return queries


def print_tree(node, indent=0):
    """Helper function to print the tree structure."""
    print("  " * indent + node.content)
    for child in node.children:
        print_tree(child, indent + 1)

# 定义不同算子的代价公式
def default_cost_formula(node):
    # 默认代价公式，可以根据算子名称或其他参数来定义
    if "TableFullScan" in node.content:
        return 10  # 假设全表扫描的代价为10
    elif "HashJoin" in node.content:
        return 20  # 假设哈希连接的代价为20
    elif "Selection" in node.content:
        return 5  # 假设选择算子的代价为5
    elif "Projection" in node.content:
        return 3  # 假设投影算子的代价为3
    elif "HashAgg" in node.content:
        return 15  # 假设哈希聚合的代价为15
    else:
        return 1  # 默认代价为1


# 计算并显示执行计划的代价
def calculate_plan_cost(node):
    if node == None:
        return 0
    total_cost = 0
    for child in node.children:
        total_cost += calculate_plan_cost(child)
    node.set_cost_formula(default_cost_formula)
    node.calculate_cost()
    total_cost += node.cost
    return total_cost        


if __name__ == "__main__":
    # Parse the query trees from the file
    file_path = "ch_operator.txt"
    query_trees = parse_query_tree(file_path)

    # Print each tree for verification
    # for i, root in enumerate(query_trees):
    #     print(f"Query Tree {i + 1}:")
    #     print_tree(root)
    #     print()

    # calculate the cost of each query
    # 计算每条查询的总代价
    for i, root in enumerate(query_trees):
        print(f"Query Tree {i + 1}:")        
        query_cost = calculate_plan_cost(root)
        print(f"Total cost of the execution plan: {query_cost}")
