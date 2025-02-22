import math

#from ch_query_params import Q1params,Q2params,Q3params,Q4params,Q5params,Q6params,Q7params,Q8params,Q9params,Q10params,Q11params,Q12params,Q13params,Q14params,Q15params,Q16params,Q17params,Q18params,Q19params,Q20params,Q21params,Q22params

class TreeNode:
    def __init__(self, content):
        self.content = content
        self.children = []
        self.cost_formula = None  # 执行代价公式，可以根据名称来定义
        self.cost = None  # 代价值       
        self.engine = None # tidb or tikv or tiflash


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
    

class Global_Params:
    def __init__(self):
        # 全局参数，例如扫描代价因子、哈希连接因子等
        self.tidb_temp_table_factor = 0.0  # TiDBTemp
        self.tikv_scan_factor = 40.7  # TiKVScan
        self.tikv_desc_scan_factor = 61.05  # TiKVDescScan
        self.tiflash_scan_factor = 11.6  # TiFlashScan
        self.tidb_cpu_factor = 49.9  # TiDBCPU
        self.tikv_cpu_factor = 49.9  # TiKVCPU
        self.tiflash_cpu_factor = 2.4  # TiFlashCPU
        self.tidb_kv_net_factor = 3.96  # TiDB2KVNet
        self.tidb_flash_net_factor = 2.2  # TiDB2FlashNet
        self.tiflash_mpp_net_factor = 1.0  # TiFlashMPPNet
        self.tidb_mem_factor = 0.2  # TiDBMem
        self.tikv_mem_factor = 0.2  # TiKVMem
        self.tiflash_mem_factor = 0.05  # TiFlashMem
        self.tidb_disk_factor = 200.0  # TiDBDisk
        self.tidb_request_factor = 6000000.0  # TiDBRequest   
        self.memQuota = 1024*1024  #memory quota 1GB

class Operator_Params:
    def __init__(self, rows, rowSize, buildRows, buildRowSize, probeRows, probeRowSize, aggFuncs, numFuncs, nKeys, sortitems, leftRows, leftRowSize, rightRows, rightRowSize, buildFilters, probeFilters):
        self.rows = rows
        self.rowSize = rowSize
        self.buildRows = buildRows
        self.buildRowSize = buildRowSize
        self.probeRows = probeRows
        self.probeRowSize = probeRowSize
        self.aggFuncs = aggFuncs
        self.numFuncs = numFuncs
        self.nKeys = nKeys
        self.sortitems = sortitems
        self.leftRows = leftRows
        self.leftRowSize = leftRowSize
        self.rightRows = rightRows
        self.rightRowSize = rightRowSize
        self.buildFilters = buildFilters
        self.probeFilters = probeFilters

class TableScan(TreeNode):
    def __init__(self, content, rows, rowSize):
        super().__init__(content)
        self.rows = rows # cardinality of rows
        self.rowSize = rowSize

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tikv":
            scanFactor = global_params.tikv_scan_factor
        elif self.engine == "Tiflash":
            scanFactor = global_params.tiflash_scan_factor
        else:
            scanFactor = 0    
        cost = self.rows * math.log2(self.rowSize) * scanFactor + (10000 * math.log2(self.rowSize) * scanFactor)
        return cost

class TableReader(TreeNode):
    def __init__(self, content, rows, rowSize):
        super().__init__(content)
        self.rows = rows
        self.rowSize = rowSize

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            netFactor = global_params.tidb_flash_net_factor
            concurrency = 4
        elif self.engine == "Tikv":
            netFactor = global_params.tidb_kv_net_factor
            concurrency = 4
        elif self.engine == "Tiflash":
            netFactor = global_params.tiflash_mpp_net_factor
            concurrency = 4
        else:
            netFactor = 10   
            concurrency = 1    
        cost = self.rows * self.rowSize * netFactor / concurrency
        return cost
    
class HashAgg(TreeNode):
    def __init__(self, content, rows, aggFuncs, numFuncs, buildRows, buildRowSize, nKeys, probeRows):
        super().__init__(content)
        self.rows = rows
        self.aggFunc = aggFuncs
        self.numFUncs = numFuncs
        self.buildRows = buildRows
        self.buildRowSize = buildRowSize
        self.nKeys = nKeys
        self.probeRows = probeRows      
    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
            memFactor = Global_Params.tidb_mem_factor
            concurrency = 4
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
            memFactor = global_params.tikv_mem_factor
            concurrency = 4
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
            memFactor = global_params.tiflash_mem_factor
            concurrency = 4
        else:
            cpuFactor = 0
            memFactor = 0 
            concurrency = 1       
        cost = 10*3*cpuFactor + (self.rows * self.aggFuncs * cpuFactor + self.rows * self.numFuncs * cpuFactor + self.buildRows * self.nKeys * cpuFactor + self.buildRows * self.buildRowSize * memFactor + self.buildRows*cpuFactor + self.probeRows*self.nKeys*cpuFactor + self.probeRows*cpuFactor) / concurrency
        return cost
    
class Sort(TreeNode):
    def __init__(self, content, rows, rowSize, sortitems, numFuncs):
        super().__init__(content)
        self.rows = rows
        self.rowSize = rowSize
        self.sortitems = sortitems
        self.numFuncs = numFuncs

    def calculate_cost(self):
        global_params = global_params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
            memFactor = global_params.tidb_mem_factor
            diskFactor = global_params.tidb_disk_factor
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
            memFactor = global_params.tikv_mem_factor
            diskFactor = global_params.tikv_disk_factor
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
            memFactor = global_params.tiflash_mem_factor
            diskFactor = global_params.tiflash_disk_factor
        else:
            cpuFactor = 0
            memFactor = 0 
            diskFactor = 0      
        cost = self.rows * math.log2(self.rows) * len(self.sortitems) * cpuFactor
        if self.rows * self.rowSize > global_params.mem_quota:   ## memory quota exceeded
            cost += global_params.memQuota*memFactor + self.rows * self.rowSize * diskFactor
        else:
            cost += self.rows * self.rowSize * memFactor
        return cost
    
class MergeJoin(TreeNode):
    def __init__(self, content, leftRows, leftRowSize, rightRows, rightRowSize, numFuncs):
        super().__init__(content)
        self.leftRows = leftRows
        self.leftRowSize = leftRowSize
        self.rightRows = rightRows
        self.rightRowsSize = rightRowSize
        self.numFuncs = numFuncs
    
    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
            memFactor = global_params.tidb_mem_factor
            diskFactor = global_params.tidb_disk_factor
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
            memFactor = global_params.tikv_mem_factor
            diskFactor = global_params.tikv_disk_factor
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
            memFactor = global_params.tiflash_mem_factor
            diskFactor = global_params.tiflash_disk_factor
        else:
            cpuFactor = 0
            memFactor = 0 
            diskFactor = 0          
        cost = self.leftRows * self.leftRowSize * cpuFactor + self.rightRows * self.rightRowSize * cpuFactor + self.leftrows * self.numFuncs * cpuFactor + self.rightrows * self.numFuncs * cpuFactor
        return cost

class HashJoin(TreeNode):
    def __init__(self, content, buildRows, buildFilters, buildRowSize, nKeys, probeRows, probeFilters, probeRowSize):
        super().__init__(content)
        self.buildRows = buildRows
        self.buildFilters = buildFilters
        self.buildRowSize = buildRowSize
        self.nKeys = nKeys
        self.probeRows = probeRows
        self.probeFilters = probeFilters
        self.probeRowSize = probeRowSize

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
            memFactor = global_params.tidb_mem_factor
            concurrency = 5
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
            memFactor = global_params.tikv_mem_factor
            concurrency = 5
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
            memFactor = global_params.tiflash_mem_factor
            concurrency = 3
        else:
            cpuFactor = 0
            memFactor = 0
            concurrency = 1
        cost = (self.buildRows * self.buildFilters * cpuFactor + self.buildRows*self.nKeys * cpuFactor + self.buildRows*self.buildRowSize*memFactor + self.buildRows * cpuFactor + self.probeRows * self.probeFilters * cpuFactor + self.probeRows* self.nKeys * cpuFactor + self.probeRows * self.probeRowSize * memFactor + self.probeRows * cpuFactor) / concurrency
        if self.engine == "Tiflash":
            return cost
        else:
            return cost + 10*3*cpuFactor   ## startup cost
 
class Selection(TreeNode):
    def __init__(self, content, rows, numFuncs):
        super().__init__(content)
        self.rows = rows
        self.numFuncs = numFuncs

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
        else:
            cpuFactor = 0

        cost = self.rows * cpuFactor * self.numFuncs
        return cost
    
class Projection(TreeNode):
    def __init__(self, content, rows, numFuncs):
        super().__init__(content)
        self.rows = rows
        self.numFuncs = numFuncs

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            cpuFactor = global_params.tidb_cpu_factor
            memFactor = global_params.tidb_mem_factor
            concurrency = 4
        elif self.engine == "Tikv":
            cpuFactor = global_params.tikv_cpu_factor
            memFactor = global_params.tikv_mem_factor
            concurrency = 4
        elif self.engine == "Tiflash":
            cpuFactor = global_params.tiflash_cpu_factor
            memFactor = global_params.tiflash_mem_factor
            concurrency = 4
        else:
            cpuFactor = 0
            memFactor = 0 
            concurrency = 1         
        cost = self.row * cpuFactor * self.numFuncs / concurrency
        return cost


## parse query tree txt and output each query's operator tree
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

    
# @test
# 检测cost计算是否正确的测试函数
# 定义不同算子的代价公式
def default_cost_formula(node):
    # 默认代价公式，可以根据算子名称或其他参数来定义
    if "TableFullScan" in node.content:
        return 10 
    elif "HashJoin" in node.content:
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
    total_cost += node
    return total_cost        



if __name__ == "__main__":
    # Parse the query trees from the file
    file_path = "ch_operator.txt"
    query_trees = parse_query_tree(file_path)

    # # Print each tree for verification
    # for i, root in enumerate(query_trees):
    #     print(f"Query Tree {i + 1}:")
    #     print_tree(root)
    #     print()

    # # calculate the cost of each query
    # # 根据default_cost_formula, 计算每条查询的总代价
    # for i, root in enumerate(query_trees):
    #     print(f"Query Tree {i + 1}:")        
    #     query_cost = calculate_plan_cost(root)
    #     print(f"Total cost of the execution plan: {query_cost}")
