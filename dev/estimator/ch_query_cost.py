import os
import sys
import logging
import copy

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.query_operators import query_operators
from estimator.ch_columns_ranges_meta import *
from log.logging_config import setup_logging

##
## 计算cost的代码应该重构, 维护每一个query的计算开销的算子, 通过算子名称对应的qparams里面的参数实例化这个算子, 然后再调用算子本身计算cost的方法来计算.
## 当query要在replica上执行时, 加入额外的算子, 再按照原始情况计算即可.
## 目前就是每一个query的算子计算是写死的, 这种情况下只能先判断每个表是否要在replica上执行, 如果要在replica上执行, 就再调用一遍计算函数
##

# 更新 query_operators 列表，对于要读 replica, 加入对应原表的算子
def update_query_operators_with_replica(qry_idx, qparams_list, query_operators):
    qparams = qparams_list[qry_idx]
    scan_table_replica = qparams.scan_table_replica

    # 如果当前query不读取replica, 直接返回
    if not scan_table_replica:
        return

    # 扫描要读的每一个replica, 加入原表对应的算子
    for table in scan_table_replica:
        replica_table = f"{table}_replica"
        query_info = copy.deepcopy(query_operators[qry_idx])
        operators = query_info["operators"]
        tables = query_info["tables"]
        logging.debug(f"initial operators len: {len(query_operators[qry_idx]['operators'])}")
        # lennn = len(query_operators[qry_idx]['operators'])

        # 遍历query本来要读的表
        for i, tbl in enumerate(tables):
            if tbl == table:
                operators.append(operators[i])
                tables.append(replica_table)
        logging.debug(f"finial operators len: {len(query_operators[qry_idx]['operators'])}")     
        # if lennn == len(query_operators[qry_idx]['operators']):
        #     print("table:", table)
        #     print("qry_idx:", qry_idx)
        #     print("scan_table_replica: ", scan_table_replica)

# 计算指定第qry_idx条query的代价
def calculate_query_cost(qry_idx, qparams_list):
    update_query_operators_with_replica(qry_idx, qparams_list, query_operators)
    query_info = query_operators[qry_idx]
    operators = query_info["operators"]
    tables = query_info["tables"]
    content = '1'
    cost = 0

    # print("Tables:", tables)
    # print("Operators:", operators)
    # print('qparams_list: ', qparams_list[qry_idx])

    for operator, table in zip(operators, tables):
        engine = 'Tikv'
        if table.endswith("_replica"):
            engine = 'Tiflash'

        rows_attr = f"rows_tablescan_{table}"
        rowsize_attr = f"rowsize_tablescan_{table}"
        rows_selection_attr = f"rows_selection_{table}"

        rows = getattr(qparams_list[qry_idx], rows_attr, None)
        rowsize = getattr(qparams_list[qry_idx], rowsize_attr, None)
        rows_selection = getattr(qparams_list[qry_idx], rows_selection_attr, None)

        # print("table:", table)
        # print('rows_attr: {} : {}'.format(rows_attr, rows))
        # print('rowsize_attr: {} : {}'.format(rowsize_attr, rowsize))
        # logging.debug(f"rowsize_attr: {rowsize_attr} : {rowsize}")

        if operator == "TableScan":
            op_instance = TableScan(content, rows, rowsize)
        elif operator == "Selection":
            op_instance = Selection(content, rows_selection, 1)
        elif operator == "TableReader":
            op_instance = TableReader(content, rows, rowsize)
        else:
            continue

        op_instance.engine = engine
        cost += op_instance.calculate_cost()
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
            
    return cost

def calculate_q1(engine, q1params):
    #q1params = Q1params()
    content = '1'
    rows_tablescan = q1params.rows_tablescan_orderline
    rowsize_tablescan = q1params.rowsize_tablescan_orderline
    rows_selection = q1params.rows_selection_orderline
    
    tablescan = TableScan(content, rows_tablescan, rowsize_tablescan)
    selection = Selection(content, rows_selection, 1)

    # print("Query1")
    # print("rows_tablescan: ", rows_tablescan)
    # print("rows_selection", rows_selection)

    tablescan.engine = engine
    selection.engine = engine

    cost = tablescan.calculate_cost() + selection.calculate_cost()
    return cost

def calculate_q2(engine, q2params):
    #q2params = Q2params()
    content = '1'
    rows_tablescan_region = q2params.rows_tablescan_region
    rowsize_region = q2params.rowsize_tablescan_region
    rows_tablescan_nation = q2params.rows_tablescan_nation
    rowsize_nation = q2params.rowsize_tablescan_nation
    rows_tablescan_supplier = q2params.rows_tablescan_supplier
    rowsize_supplier = q2params.rowsize_tablescan_supplier
    rows_tablescan_stock = q2params.rows_tablescan_stock
    rowsize_stock = q2params.rowsize_tablescan_stock
    rows_tablescan_item = q2params.rows_tablescan_item
    rowsize_item = q2params.rowsize_tablescan_item

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

def calculate_q3(engine, q3params):
    #q3params = Q3params()
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
    
def calculate_q4(engine, q4params):
    #q4params = Q4params()
    content = '1'
    rows_tablescan_orders = q4params.rows_tablescan_orders
    rowsize_tablescan_orders = q4params.rowsize_tablescan_orders
    rows_selection_orders = q4params.rows_selection_orders
    rows_tablescan_orderline = q4params.rows_tablescan_orderline
    rowsize_tablescan_orderline = q4params.rowsize_tablescan_orderline
    rows_selection_orderline = q4params.rows_selection_orderline

    tablescan_orders = TableScan(content, rows_tablescan_orders, rowsize_tablescan_orders)
    selection_orders = Selection(content, rows_selection_orders, 1)
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    tablescan_orderline = TableScan(content, rows_tablescan_orderline, rowsize_tablescan_orderline)
    selection_orderline = Selection(content, rows_selection_orderline, 1)
    tablescan_orderline.engine = engine
    selection_orderline.engine = engine

    cost = tablescan_orders.calculate_cost() + selection_orders.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost()

    return cost

def calculate_q5(engine, q5params):
    cost = 0
    #q5params = Q5params()
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

    cost = tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_nation.calculate_cost() + selection_nation.calculate_cost() + tablescan_supplier.calculate_cost() + selection_supplier.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost() + tablescan_customer.calculate_cost() + selection_customer.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
    return cost

def calculate_q6(engine, q6params):
    #q6params = Q6params()
    content = '1'
    rows_tablescan_order_line = q6params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q6params.rowsize_tablescan_order_line
    rows_selection_order_line = q6params.rows_selection_order_line
    #print('rows_tablescan_order_line', rows_tablescan_order_line)

    tablescan_orderline = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    selection_orderline = Selection(content, rows_selection_order_line, 1)
    tablescan_orderline.engine = engine
    selection_orderline.engine = engine    

    cost = tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost()
    return cost

def calculate_q7(engine, q7params):
    cost = 0
    #q7params = Q7params()
    content = '1'

    rows_tablescan_nation = q7params.rows_tablescan_nation
    rowsize_tablescan_nation = q7params.rowsize_tablescan_nation
    rows_selection_nation = q7params.rows_selection_nation
    rows_tablescan_nation = q7params.rows_tablescan_nation
    rowsize_tablescan_nation = q7params.rowsize_tablescan_nation
    rows_selection_nation = q7params.rows_selection_nation
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
    rows_tablescan_customer = q7params.rows_tablescan_customer
    rowsize_tablescan_customer = q7params.rowsize_tablescan_customer

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

    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    tablereader_customer = TableReader(content,  rows_tablescan_customer, rowsize_tablescan_customer)
    tablescan_customer.engine = engine
    tablereader_customer.engine = engine      

    cost = tablescan_nation.calculate_cost()*2 + selection_nation.calculate_cost()*2 + tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost() + tablescan_stock.calculate_cost() + tablereader_stock.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost() + tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost()
    return cost

def calculate_q8(engine, q8params):
    cost = 0
    #q8params = Q8params()
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

    cost = tablescan_nation.calculate_cost()*2 + tablereader_nation.calculate_cost()*2 + tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_customer.calculate_cost() + tablereader_customer.calculate_cost() + tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost() + tablescan_item.calculate_cost() + selection_item.calculate_cost() + tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
    return cost

def calculate_q9(engine, q9params):
    cost = 0
    #q9params = Q9params()
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

def calculate_q10(engine, q10params):
    cost = 0
    #q10params = Q10params()
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

def calculate_q11(engine, q11params):
    cost = 0
    #q11params = Q11params()
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

def calculate_q12(engine, q12params):
    cost = 0
    #q12params = Q12params()
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

def calculate_q13(engine, q13params):
    cost = 0
    #q13params = Q13params()
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

def calculate_q14(engine, q14params):
    cost = 0
    #q14params = Q14params()
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

def calculate_q15(engine, q15params):
    cost = 0
    #q15params = Q15params()
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

def calculate_q16(engine, q16params):
    cost = 0
    #q16params = Q16params()
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

def calculate_q17(engine, q17params):
    cost = 0
    #q17params = Q17params()
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
    tablereader_item = TableReader(content, rows_tablescan_item, rowsize_tablescan_item)
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

    return cost

def calculate_q18(engine, q18params):
    cost = 0
    #q18params = Q18params()
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

def calculate_q19(engine, q19params):
    cost = 0
    #q19params = Q19params()
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

def calculate_q20(engine, q20params):
    cost = 0
    #q20params = Q20params()
    content = '1'  

    rows_tablescan_order_line = q20params.rows_tablescan_order_line
    rowsize_tablescan_order_line = q20params.rowsize_tablescan_order_line
    rows_selection_order_line = q20params.rows_selection_order_line
    rows_tablescan_supplier = q20params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q20params.rowsize_tablescan_supplier
    rows_selection_supplier = q20params.rows_selection_supplier
    rows_tablescan_item = q20params.rows_tablescan_item
    rowsize_tablescan_item = q20params.rowsize_tablescan_item
    rows_selection_item = q20params.rows_selection_item
    rows_tablescan_nation = q20params.rows_tablescan_nation
    rowsize_tablescan_nation = q20params.rowsize_tablescan_nation
    rows_selection_nation = q20params.rows_selection_nation
    rows_tablescan_supplier = q20params.rows_tablescan_supplier
    rowsize_tablescan_supplier = q20params.rowsize_tablescan_supplier

    # 对应 rows_tablescan_order_line 和 rowsize_tablescan_order_line
    tablescan_order_line = TableScan(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablereader_order_line = TableReader(content, rows_tablescan_order_line, rowsize_tablescan_order_line)
    tablescan_order_line.engine = engine
    tablereader_order_line.engine = engine
    cost += tablescan_order_line.calculate_cost() + tablereader_order_line.calculate_cost()

    # 对应 rows_selection_order_line
    selection_order_line = Selection(content, rows_selection_order_line, 1)
    selection_order_line.engine = engine
    cost += selection_order_line.calculate_cost()

    # 对应 rows_tablescan_supplier 和 rowsize_tablescan_supplier
    tablescan_supplier = TableScan(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablereader_supplier = TableReader(content, rows_tablescan_supplier, rowsize_tablescan_supplier)
    tablescan_supplier.engine = engine
    tablereader_supplier.engine = engine
    cost += tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost()

    # 对应 rows_selection_supplier
    selection_supplier = Selection(content, rows_selection_supplier, 1)
    selection_supplier.engine = engine
    cost += selection_supplier.calculate_cost()

    # 对应 rows_tablescan_item 和 rowsize_tablescan_item
    tablescan_item = TableScan(content, rows_tablescan_item, rowsize_tablescan_item)
    tablereader_item = TableReader(content, rows_tablescan_item, rowsize_tablescan_item)
    tablescan_item.engine = engine
    tablereader_item.engine = engine
    cost += tablescan_item.calculate_cost() + tablereader_item.calculate_cost()

    # 对应 rows_selection_item
    selection_item = Selection(content, rows_selection_item, 1)
    selection_item.engine = engine
    cost += selection_item.calculate_cost()

    # 对应 rows_tablescan_nation 和 rowsize_tablescan_nation
    tablescan_nation = TableScan(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablereader_nation = TableReader(content, rows_tablescan_nation, rowsize_tablescan_nation)
    tablescan_nation.engine = engine
    tablereader_nation.engine = engine
    cost += tablescan_nation.calculate_cost() + tablereader_nation.calculate_cost()

    # 对应 rows_selection_nation
    selection_nation = Selection(content, rows_selection_nation, 1)
    selection_nation.engine = engine
    cost += selection_nation.calculate_cost()

    return cost

def calculate_q21(engine, q21params):
    cost = 0
    #q21params = Q21params()
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

def calculate_q22(engine, q22params):
    cost = 0
    #q22params = Q22params()
    content = '1'  

    rows_tablescan_customer = q22params.rows_tablescan_customer
    rowsize_tablescan_customer = q22params.rowsize_tablescan_customer
    rows_selection_customer = q22params.rows_selection_customer      

    # 对应 rows_tablescan_customer 和 rowsize_tablescan_customer
    tablescan_customer = TableScan(content, rows_tablescan_customer, rowsize_tablescan_customer)
    selection_customer = Selection(content, rows_selection_customer, 1)
    selection_customer.engine = engine
    tablescan_customer.engine = engine
    cost += tablescan_customer.calculate_cost() + selection_customer.calculate_cost()

    return cost


if __name__ == "__main__":
    # update table metadata
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

    customer_replica_meta.replica = True
    district_replica_meta.replica = True
    history_replica_meta.replica = True
    item_replica_meta.replica = True
    nation_replica_meta.replica = True
    new_order_replica_meta.replica = True
    order_line_replica_meta.replica = True
    orders_replica_meta.replica = True
    region_replica_meta.replica = True
    stock_replica_meta.replica = True
    supplier_replica_meta.replica = True
    warehouse_replica_meta.replica = True

    table_meta.extend([customer_replica_meta, district_replica_meta, history_replica_meta, item_replica_meta, nation_replica_meta, new_order_replica_meta, order_line_replica_meta, orders_replica_meta, region_replica_meta, stock_replica_meta, supplier_replica_meta, warehouse_replica_meta])



    ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)], [800, 1600, 2400, 10000]]
    keys = ['ol_delivery_d', 'ol_o_id']
    order_line_meta.update_partition_metadata(keys, ranges)  

    ranges = [[25000, 50000, 75000, 150000]]
    keys = ['i_id']
    item_meta.update_partition_metadata(keys, ranges) 

    ranges = [[25000, 50000, 75000, 150000]]
    keys = ['s_i_id']
    stock_meta.update_partition_metadata(keys, ranges)  

    ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)]]
    keys = ['o_entry_d']
    orders_meta.update_partition_metadata(keys, ranges)            

    # 根据replica更新rowsize
    qcard_list = update_rowsize(table_columns, candidates)

    # get Qcard
    qcard_list = get_qcard(table_meta, qcard_list, candidates)

    # update Qparams
    qparams_list = update_qparams_with_qcard(qcard_list)

    # calculate query cost
    engine = 'Tiflash'
    
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

    for i in range(22):
        calculate_query_cost(i, qparams_list, engine)