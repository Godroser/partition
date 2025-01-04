import math

from ch_operators_params import Q1params,Q2params,Q3params,Q4params,Q5params,Q6params,Q7params,Q8params,Q9params,Q10params,Q11params,Q12params,Q13params,Q14params,Q15params,Q16params,Q17params,Q18params,Q19params,Q20params,Q21params,Q22params

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
        print(scanFactor)
        return cost

class TableReader(TreeNode):
    def __init__(self, content, rows, rowSize):
        super().__init__(content)
        self.rows = rows
        self.rowSize = rowSize

    def calculate_cost(self):
        global_params = Global_Params()
        if self.engine == "Tidb":
            netFactor = global_params.tidb_net_factor
            concurrency = 4
        elif self.engine == "Tikv":
            netFactor = global_params.tidb_net_factor
            concurrency = 4
        elif self.engine == "Tiflash":
            netFactor = global_params.tiflash_net_factor
            concurrency = 4
        else:
            netFactor = 0   
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
        cost = (self.buildRows * self.buildFilters * cpuFactor + self.buildRows*self.nKeys*self.cpuFactor + self.buildRows*self.buildRowSize*memFactor + self.buildRows*cpuFactor + self.probeRows * self.probeFilters * cpuFactor + self.probeRows*self.nKeys*cpuFactor + self.probeRows*self.probeRowSize*memFactor + self.probeRows*cpuFactor) / concurrency
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

def calculate_q1(engine):
    q1params = Q1params()
    content = '1'
    rows_tablescan = q1params.rows_tablescan_orderline
    rowSize_tablescan = q1params.rowSize_tablescan_orderline
    rows_selection = q1params.rows_selection_orderline
    
    tablescan = TableScan(content, rows_tablescan, rowSize_tablescan)
    selection = Selection(content, rows_selection, 1)

    tablescan.engine = engine
    selection.engine = engine

    cost = tablescan.calculate_cost() + selection.calculate_cost()
    return cost

def calculate_q2(engine):
    q2params = Q2params()
    content = '1'
    rows_tablescan_region = q2params.rows_tablescan_region
    rowsize_region = q2params.rowsize_table_region
    rows_tablescan_nation = q2params.rows_tablescan_nation
    rowsize_nation = q2params.rowsize_table_nation
    rows_tablescan_supplier = q2params.rows_tablescan_supplier
    rowsize_supplier = q2params.rowsize_table_supplier
    rows_tablescan_stock = q2params.rows_tablescan_stock
    rowsize_stock = q2params.rowsize_table_stock
    rows_tablescan_item = q2params.rows_tablescan_item
    rowsize_item = q2params.rowsize_table_item

    tablescan_region = TableScan(content, rows_tablescan_region, rowsize_region)
    selection_region = Selection(content, rows_tablescan_region, 1)
    tablescan_region.engine = engine
    selection_region.engine = engine

    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_nation)
    tablereader_nation = TableReader(content, rows_tablescan_nation, rowsize_nation)
    tablescan_nation.engine = engine
    tablereader_nation.engine = engine    

    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine    

    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_stock)
    selection_stock = Selection(content, rows_tablescan_stock, 1)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    selection_stock.engine = engine    

    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_item)
    selection_item = Selection(content, rows_tablescan_item, 1)
    tablescan_item.engine = engine
    selection_item.engine = engine

    cost = tablescan_region.calculate_cost()*2 + selection_region.calculate_cost()*2 + tablescan_nation.calculate_cost()*2 + tablereader_nation.calculate_cost()*2 + tablescan_supplier.calculate_cost()*2 + tablereader_supplier.calculate_cost()*2 + tablescan_stock.calculate_cost()*2 + tablereader_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_item.calculate_cost() + selection_item.calculate_cost()

    return cost

def calculate_q3(engine):
    q3params = Q3params()
    content = '1'
    rows_tablescan_orderline = q3params.rows_tablescan_orderline
    rowsize_tablescan_orderline = q3params.rowsize_tablescan_orderline
    rows_selection_orderline = q3params.rows_selection_orderline
    row_tablescan_neworder = q3params.rows_tablescan_neworder
    rowsize_tablescan_neworder = q3params.rowsize_tablescan_neworder
    rows_selection_neworder = q3params.rows_selection_neworder
    rows_tablescan_customer = q3params.rows_tablescan_customer
    rowsize_tablescan_customer = q3params.rowsize_tablescan_customer
    rows_selection_customer = q3params.rows_selection_customer
    rows_tablescan_orders = q3params.rows_tablescan_orders
    rowsize_tablescan_orders = q3params.rowsize_tablescan_orders
    rows_selection_orders = q3params.rows_selection_orders

    tablescan_orderline = TableScan(content, rows_tablescan_orderline, rowsize_tablescan_orderline)
    selection_orderline = Selection(content, rows_selection_orderline, 1)
    tablescan_orderline.engine = engine        
    selection_orderline.engine = engine

    tablescan_neworder = TableScan(content, row_tablescan_neworder, rowsize_tablescan_neworder)
    selection_neworder = Selection(content, rows_selection_neworder, 1)
    tablescan_neworder.engine = engine
    selection_neworder.engine = engine

    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    selection_customer = Selection(content, rows_selection_customer, 1)
    tablescan_customer.engine = engine
    selection_customer.engine = engine

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    cost = tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost() + tablescan_neworder.calculate_cost() + selection_neworder.calculate_cost() + tablescan_customer.calculate_cost() + selection_customer.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
    return cost
    
def calculate_q4(engine):
    q4params = Q4params()
    content = '1'
    rows_tablescan_orders = q4params.rows_tablescan_orders
    rowsize_tablescan_orders = q4params.rowsize_tablescan_orders
    rows_selection_orders = q4params.rows_selection_orders
    rows_tablescan_orderline = q4params.rows_tablescan_orderline
    rowsize_tablescan_orderline = q4params.rowsize_tablescan_orderline
    rows_selection_orderline = q4params.rows_selection_orderline

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = 'Tikv'
    selection_orders.engine = 'Tikv'

    tablescan_orderline = TableScan(content, rows_tablescan_orderline, rowsize_tablescan_orderline)
    selection_orderline = Selection(content, rows_selection_orderline, 1)
    tablescan_orderline.engine = 'Tikv'
    selection_orderline.engine = 'Tikv'

    cost = tablescan_orders.calculate_cost() + selection_orders.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost()
    print(tablescan_orders.rows)
    return cost

def calculate_q5(engine):
    cost = 0
    q5params = Q5params()
    content = '1'    
    rows_tablescan_region = q5params.rows_tablescan_region
    rowsize_tablescan_region = q5params.rowsize_tablescan_region
    rows_selection_region = q5params.rows_selection_region
    rows_tablescan_nation = q5params.rows_tablescan_nation
    rowsize_tablescan_nation = q5params.rowsize_tablescan_nation
    rows_selection_nation = q5params.rows_selection_nation
    rows_tablescan_supplier = q5params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q5params.rowsize_tablescan_supplier
    rows_selection_supplier = q5params.rows_selection_supplier
    rows_tablescan_stock = q5params.rows_tablescan_stock
    rowsize_tablescan_stock = q5params.rowsize_tablescan_stock
    rows_selection_stock = q5params.rows_selection_stock
    rows_tablescan_orderline = q5params.rows_tablescan_orderline
    rowsize_tablescan_orderline = q5params.rowsize_tablescan_orderline
    rows_selection_orderline = q5params.rows_selection_orderline
    rows_tablescan_customer = q5params.rows_tablescan_customer
    rowsize_tablescan_customer = q5params.rowsize_tablescan_customer
    rows_selection_customer = q5params.rows_selection_customer
    rows_tablescan_orders = q5params.rows_tablescan_orders
    rowsize_tablescan_orders = q5params.rowsize_tablescan_orders
    rows_selection_orders = q5params.rows_selection_orders

    tablescan_region = TableScan(content, rows_tablescan_region, rowsize_tablescan_region)
    selection_region = Selection(content, rows_selection_region, 1)
    tablescan_region.engine = engine
    selection_region.engine = engine

    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    selection_nation = Selection(content, rows_selection_nation, 1)
    tablescan_nation.engine = engine
    selection_nation.engine = engine

    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    selection_supplier = Selection(content, rows_selection_supplier, 1)
    tablescan_supplier.engine = engine
    selection_supplier.engine = engine

    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    selection_stock = Selection(content, rows_selection_stock, 1)
    tablescan_stock.engine = engine
    selection_stock.engine = engine

    tablescan_orderline = TableScan(content, rows_tablescan_orderline, rowsize_tablescan_orderline)
    selection_orderline = Selection(content, rows_selection_orderline, 1)
    tablescan_orderline.engine = engine
    selection_orderline.engine = engine

    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    selection_customer = Selection(content, rows_selection_customer, 1)
    tablescan_customer.engine = engine
    selection_customer.engine = engine

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    cost = tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_nation.calculate_cost() + selection_nation.calculate_cost() + tablescan_supplier.calculate_cost() + selection_supplier.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost() + tablescan_customer.calculate
    return cost

def calculate_q6(engine):
    q6params = Q6params()
    content = '1'
    rows_tablescan_order_line = q6params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q6params.rowsize_tablescan_order_line
    rows_selection_order_line = q6params.rows_selection_order_line

    tablescan_orderline = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_orderline = Selection(content, rows_selection_order_line, 1)
    tablescan_orderline.engine = engine
    selection_orderline.engine = engine    

    cost = tablescan_orderline.calculate_cost() + Selection.calculate_cost()
    return cost

def calculate_q7(engine):
    cost = 0
    q7params = Q7params()
    content = '1'

    rows_tablescan_nation = q7params.rows_tablescan_nation
    rowsize_tablescan_nation = q7params.rowsize_tablescan_nation
    rows_selection_nation = q7params.rows_selection_nation
    #rows_tablescan_nation = q7params.rows_tablescan_nation
    #rowsize_tablescan_nation = q7params.rowsize_tablescan_nation
    #rows_selection_nation = q7params.rows_selection_nation
    rows_tablescan_supplier = q7params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q7params.rowsize_tablescan_supplier
    rows_tablescan_stock = q7params.rows_tablescan_stock
    rowsize_tablescan_stock = q7params.rowsize_tablescan_stock
    rows_tablescan_orders = q7params.rows_tablescan_orders
    rowsize_tablescan_orders = q7params.rowsize_tablescan_orders
    rows_selection_orders = q7params.rows_selection_orders
    rows_tablescan_order_line = q7params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q7params.rowsize_tablescan_order_line
    rows_selection_order_line = q7params.rows_selection_order_line

    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    selection_nation = Selection(content, rows_selection_nation, 1)
    tablescan_nation.engine = engine
    selection_nation.engine = engine

    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine

    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content,  rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine  

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine

    cost = tablescan_nation.calculate_cost()*2 + selection_nation.calculate_cost()*2 + tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost() + tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost() + tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()
    return cost

def calculate_q8(engine):
    cost = 0
    q8params = Q8params()
    content = '1'

    rows_tablescan_nation = q8params.rows_tablescan_nation
    rowsize_tablescan_nation = q8params.rowsize_tablescan_nation
    rows_tablescan_region = q8params.rows_tablescan_region
    rowsize_tablescan_region = q8params.rowsize_tablescan_region
    rows_selection_region = q8params.rows_selection_region
    ##rows_tablescan_nation = q8params.rows_tablescan_nation
    ##rowsize_tablescan_nation = q8params.rowsize_tablescan_nation
    rows_tablescan_customer = q8params.rows_tablescan_customer
    rowsize_tablescan_customer = q8params.rowsize_tablescan_customer
    rows_tablescan_supplier = q8params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q8params.rowsize_tablescan_supplier
    rows_tablescan_item = q8params.rows_tablescan_item
    rowsize_tablescan_item = q8params.rowsize_tablescan_item
    rows_selection_item = q8params.rows_selection_item
    rows_tablescan_order_line = q8params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q8params.rowsize_tablescan_order_line
    rows_selection_order_line = q8params.rows_selection_order_line
    rows_tablescan_stock = q8params.rows_tablescan_stock
    rowsize_tablescan_stock = q8params.rowsize_tablescan_stock
    rows_selection_stock = q8params.rows_selection_stock
    rows_tablescan_orders = q8params.rows_tablescan_orders
    rowsize_tablescan_orders = q8params.rowsize_tablescan_orders
    rows_selection_orders = q8params.rows_selection_orders    

    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablereader_nation = TableReader(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablescan_nation.engine = engine
    tablereader_nation.engine = engine

    tablescan_region = TableScan(content, rows_tablescan_region, rowsize_tablescan_region)
    selection_region = Selection(content, rows_selection_region, 1)
    tablescan_region.engine = engine
    selection_region.engine = engine


    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablereader_customer = TableReader(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine

    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine


    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    selection_item = Selection(content, rows_selection_item, 1)
    tablescan_item.engine = engine
    selection_item.engine = engine

    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine

    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    selection_stock = Selection(content, rows_selection_stock, 1)
    tablescan_stock.engine = engine
    selection_stock.engine = engine

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    cost = tablescan_nation.calculate_cost()*2 + tablereader_nation.calculate_cost()*2 + tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_customer.calculate_cost() + tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost() + tablescan_item.calculate_cost() + selection_item.calculate_cost() + tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
    return cost

def calculate_q9(engine):
    cost = 0
    q9params = Q9params()
    content = '1'

    rows_tablescan_orders = q9params.rows_tablescan_orders
    rowsize_tablescan_orders = q9params.rowsize_tablescan_orders
    rows_tablescan_nation = q9params.rows_tablescan_nation
    rowsize_tablescan_nation = q9params.rowsize_tablescan_nation
    rows_tablescan_supplier = q9params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q9params.rowsize_tablescan_supplier
    rows_tablescan_order_line = q9params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q9params.rowsize_tablescan_order_line
    rows_selection_order_line = q9params.rows_selection_order_line
    rows_tablescan_item = q9params.rows_tablescan_item
    rowsize_tablescan_item = q9params.rowsize_tablescan_item
    rows_selection_item = q9params.rows_selection_item
    rows_tablescan_stock = q9params.rows_tablescan_stock
    rowsize_tablescan_stock = q9params.rowsize_tablescan_stock    

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    tablereader_orders = TableReader(content, rows_tablescan_orders, rowsize_tablescan_orders)
    tablescan_orders.engine = engine
    tablereader_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + tablereader_orders.calculate_cost()

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablereader_nation = TableReader(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablescan_nation.engine = engine
    tablereader_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + tablereader_nation.calculate_cost()

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item，以及 rows_selection_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    selection_item = Selection(content, rows_selection_item, 1)
    tablescan_item.engine = engine
    selection_item.engine = engine
    cost += tablescan_item.calculate_cost() + selection_item.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    return cost

def calculate_q10(engine):
    cost = 0
    q10params = Q10params()
    content = '1'

    rows_tablescan_nation = q10params.rows_tablescan_nation
    rowsize_tablescan_nation = q10params.rowsize_tablescan_nation
    rows_tablescan_order_line = q10params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q10params.rowsize_tablescan_order_line
    rows_selection_order_line = q10params.rows_selection_order_line
    rows_tablescan_orders = q10params.rows_tablescan_orders
    rowsize_tablescan_orders = q10params.rowsize_tablescan_orders
    rows_selection_orders = q10params.rows_selection_orders
    rows_tablescan_customer = q10params.rows_tablescan_customer
    rowsize_tablescan_customer = q10params.rowsize_tablescan_customer    

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablereader_nation = TableReader(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablescan_nation.engine = engine
    tablereader_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + tablereader_nation.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    # 对应 rows_tablescan_customer 和 rowsize_tablescan_customer
    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablereader_customer = TableReader(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine
    cost += tablescan_customer.calculate_cost() + tablereader_customer.calculate_cost()

    return cost

def calculate_q11(engine):
    cost = 0
    q11params = Q11params()
    content = '1'

    rows_tablescan_nation = q11params.rows_tablescan_nation
    rowsize_tablescan_nation = q11params.rowsize_tablescan_nation
    rows_selection_nation = q11params.rows_selection_nation
    rows_tablescan_supplier = q11params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q11params.rowsize_tablescan_supplier
    rows_tablescan_stock = q11params.rows_tablescan_stock
    rowsize_tablescan_stock = q11params.rowsize_tablescan_stock    

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation，以及 rows_selection_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    selection_nation = Selection(content, rows_selection_nation, 1)
    tablescan_nation.engine = engine
    selection_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + selection_nation.calculate_cost()

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()    

    return cost

def calculate_q12(engine):
    cost = 0
    q12params = Q12params()
    content = '1'   

    rows_tablescan_orders = q12params.rows_tablescan_orders
    rowsize_tablescan_orders = q12params.rowsize_tablescan_orders
    rows_selection_orders = q12params.rows_selection_orders
    rows_tablescan_order_line = q12params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q12params.rowsize_tablescan_order_line
    rows_selection_order_line = q12params.rows_selection_order_line    

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    return cost



def calculate_q13(engine):
    cost = 0
    q13params = Q13params()
    content = '1' 

    rows_tablescan_orders = q13params.rows_tablescan_orders
    rowsize_tablescan_orders = q13params.rowsize_tablescan_orders
    rows_selection_orders = q13params.rows_selection_orders
    rows_tablescan_customer = q13params.rows_tablescan_customer
    rowsize_tablescan_customer = q13params.rowsize_tablescan_customer    

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    # 对应 rows_tablescan_customer 和 rowsize_tablescan_customer
    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablereader_customer = TableReader(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine
    cost += tablescan_customer.calculate_cost() + tablereader_customer.calculate_cost()

    return cost

def calculate_q14(engine):
    cost = 0
    q14params = Q14params()
    content = '1'

    rows_tablescan_order_line = q14params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q14params.rowsize_tablescan_order_line
    rows_selection_order_line = q14params.rows_selection_order_line
    rows_tablescan_item = q14params.rows_tablescan_item
    rowsize_tablescan_item = q14params.rowsize_tablescan_item    

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    tablereader_item = TableReader(content, rows_tablescan_item, rowsize_tablescan_item)
    tablescan_item.engine = engine
    tablereader_item.engine = engine
    cost += tablescan_item.calculate_cost() + tablereader_item.calculate_cost()

    return cost

def calculate_q15(engine):
    cost = 0
    q15params = Q15params()
    content = '1' 
    
    rows_tablescan_supplier = q15params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q15params.rowsize_tablescan_supplier
    rows_tablescan_order_line = q15params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q15params.rowsize_tablescan_order_line
    rows_selection_order_line = q15params.rows_selection_order_line
    rows_tablescan_stock = q15params.rows_tablescan_stock
    rowsize_tablescan_stock = q15params.rowsize_tablescan_stock    

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    return cost

def calculate_q16(engine):
    cost = 0
    q16params = Q16params()
    content = '1'       

    rows_tablescan_supplier = q16params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q16params.rowsize_tablescan_supplier
    rows_selection_supplier = q16params.rows_selection_supplier
    rows_tablescan_item = q16params.rows_tablescan_item
    rowsize_tablescan_item = q16params.rowsize_tablescan_item
    rows_selection_item = q16params.rows_selection_item
    rows_tablescan_stock = q16params.rows_tablescan_stock
    rowsize_tablescan_stock = q16params.rowsize_tablescan_stock    

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier，以及 rows_selection_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    selection_supplier = Selection(content, rows_selection_supplier, 1)
    tablescan_supplier.engine = engine
    selection_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + selection_supplier.calculate_cost()

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item，以及 rows_selection_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    selection_item = Selection(content, rows_selection_item, 1)
    tablescan_item.engine = engine
    selection_item.engine = engine
    cost += tablescan_item.calculate_cost() + selection_item.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    return cost

def calculate_q17(engine):
    cost = 0
    q17params = Q17params()
    content = '1'    

    rows_tablescan_supplier = q17params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q17params.rowsize_tablescan_supplier
    rows_selection_supplier = q17params.rows_selection_supplier
    rows_tablescan_item = q17params.rows_tablescan_item
    rowsize_tablescan_item = q17params.rowsize_tablescan_item
    rows_selection_item = q17params.rows_selection_item
    rows_tablescan_stock = q17params.rows_tablescan_stock
    rowsize_tablescan_stock = q17params.rowsize_tablescan_stock
    rows_tablescan_item = q17params.rows_tablescan_item
    rowsize_tablescan_item = q17params.rowsize_tablescan_item
    rows_tablescan_order_line = q17params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q17params.rowsize_tablescan_order_line
    rows_tablescan_order_line = q17params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q17params.rowsize_tablescan_order_line

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier，以及 rows_selection_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    selection_supplier = Selection(content, rows_selection_supplier, 1)
    tablescan_supplier.engine = engine
    selection_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + selection_supplier.calculate_cost()

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item，以及 rows_selection_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    selection_item = Selection(content, rows_selection_item, 1)
    tablereader_item = TableReader(content, tablescan_item, selection_item)
    tablescan_item.engine = engine
    selection_item.engine = engine
    cost += tablescan_item.calculate_cost()*2 + selection_item.calculate_cost() + tablereader_item.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablereader_order_line = TableReader(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablescan_order_line.engine = engine
    tablereader_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost()*2 + tablereader_order_line.calculate_cost()*2


def calculate_q18(engine):
    cost = 0
    q18params = Q18params()
    content = '1'    

    rows_tablescan_order_line = q18params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q18params.rowsize_tablescan_order_line
    rows_tablescan_customer = q18params.rows_tablescan_customer
    rowsize_tablescan_customer = q18params.rowsize_tablescan_customer
    rows_tablescan_orders = q18params.rows_tablescan_orders
    rowsize_tablescan_orders = q18params.rowsize_tablescan_orders
    rows_selection_orders = q18params.rows_selection_orders    

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablereader_order_line = TableReader(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablescan_order_line.engine = engine
    tablereader_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + tablereader_order_line.calculate_cost()

    # 对应 rows_tablescan_customer 和 rowsize_tablescan_customer
    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablereader_customer = TableReader(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine
    cost += tablescan_customer.calculate_cost() + tablereader_customer.calculate_cost()

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    return cost

def calculate_q19(engine):
    q19params = Q19params()
    content = '1'  

    rows_tablescan_item = q19params.rows_tablescan_item
    rowsize_tablescan_item = q19params.rowsize_tablescan_item
    rows_selection_item = q19params.rows_selection_item
    rows_tablescan_order_line = q19params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q19params.rowsize_tablescan_order_line
    rows_selection_order_line = q19params.rows_selection_order_line    

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item，以及 rows_selection_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    selection_item = Selection(content, rows_selection_item, 1)
    tablescan_item.engine = engine
    selection_item.engine = engine
    cost += tablescan_item.calculate_cost() + selection_item.calculate_cost()

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line，以及 rows_selection_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    selection_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()

    return cost

def calculate_q20(engine):
    cost = 0
    q20params = Q20params()
    content = '1'  

    rows_tablescan_order_line = q20params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q20params.rowsize_tablescan_order_line
    rows_tablescan_nation = q20params.rows_tablescan_nation
    rowsize_tablescan_nation = q20params.rowsize_tablescan_nation
    rows_selection_nation = q20params.rows_selection_nation
    rows_tablescan_supplier = q20params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q20params.rowsize_tablescan_supplier
    rows_tablescan_stock = q20params.rows_tablescan_stock
    rowsize_tablescan_stock = q20params.rowsize_tablescan_stock
    rows_tablescan_order_line = q20params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q20params.rowsize_tablescan_order_line
    rows_selection_order_line = q20params.rows_selection_order_line
    rows_tablescan_orders = q20params.rows_tablescan_orders
    rowsize_tablescan_orders = q20params.rowsize_tablescan_orders
    rows_selection_orders = q20params.rows_selection_orders      

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablereader_order_line = TableReader(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    tablereader_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost()*2 + tablereader_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation，以及 rows_selection_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    selection_nation = Selection(content, rows_selection_nation, 1)
    tablescan_nation.engine = engine
    selection_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + selection_nation.calculate_cost()

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    return cost

def calculate_q21(engine):
    cost = 0
    q21params = Q21params()
    content = '1'    

    rows_tablescan_order_line = q21params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q21params.rowsize_tablescan_order_line
    rows_tablescan_nation = q21params.rows_tablescan_nation
    rowsize_tablescan_nation = q21params.rowsize_tablescan_nation
    rows_selection_nation = q21params.rows_selection_nation
    rows_tablescan_supplier = q21params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q21params.rowsize_tablescan_supplier
    rows_tablescan_stock = q21params.rows_tablescan_stock
    rowsize_tablescan_stock = q21params.rowsize_tablescan_stock
    rows_tablescan_order_line = q21params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q21params.rowsize_tablescan_order_line
    rows_selection_order_line = q21params.rows_selection_order_line
    rows_tablescan_orders = q21params.rows_tablescan_orders
    rowsize_tablescan_orders = q21params.rowsize_tablescan_orders
    rows_selection_orders = q21params.rows_selection_orders    

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablereader_order_line = TableReader(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    tablescan_order_line.engine = engine
    tablereader_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost()*2 + tablereader_order_line.calculate_cost() + selection_order_line.calculate_cost()

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation，以及 rows_selection_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    selection_nation = Selection(content, rows_selection_nation, 1)
    tablescan_nation.engine = engine
    selection_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + selection_nation.calculate_cost()

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_tablescan_stock 和 rowsize_tablescan_stock
    tablescan_stock = TableScan(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablereader_stock = TableReader(content, rows_tablescan_stock, rowsize_tablescan_stock)
    tablescan_stock.engine = engine
    tablereader_stock.engine = engine
    cost += tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost()

    # 对应 rows_tablescan_orders 和 rowsize_tablescan_orders，以及 rows_selection_orders
    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine
    cost += tablescan_orders.calculate_cost() + selection_orders.calculate_cost()

    return cost

def calculate_q22(engine):
    cost = 0
    q22params = Q22params()
    content = '1'  

    rows_tablescan_customer = q22params.rows_tablescan_customer
    rowsize_tablescan_customer = q22params.rowsize_tablescan_customer
    rows_selection_customer = q22params.rows_selection_customer      

    # 对应 rows_tablescan_customer 和 rowsize_tablescan_customer
    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    selection_customer = Selection(content, rows_selection_customer, 1)
    selection_customer.engine = engine
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine
    cost += tablescan_customer.calculate_cost() + selection_customer.calculate_cost()

    return cost

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

    print(calculate_q4())