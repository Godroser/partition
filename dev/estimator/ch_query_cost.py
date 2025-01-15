import os
import sys

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *

def calculate_q1(engine, q1params):
    #q1params = Q1params()
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

    cost = tablescan_nation.calculate_cost()*2 + tablereader_nation.calculate_cost()*2 + tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_customer.calculate_cost() + tablescan_supplier.calculate_cost() + tablereader_supplier.calculate_cost() + tablescan_item.calculate_cost() + selection_item.calculate_cost() + tablescan_order_line.calculate_cost() + selection_order_line.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
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

# 根据分区metadata, 获取每一个quert的查询基数
def get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta):
    # get Qcard
    qcard = []
    q1card = Q1card()
    q1card.init()
    # print("Query 1")
    q1card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q1card)

    q2card = Q2card()
    q2card.init()
    # print("Query 2")
    q2card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q2card)

    q3card = Q3card()
    q3card.init()
    # print("Query 3")
    q3card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q3card)

    q4card = Q4card()
    q4card.init()
    # print("Query 4")
    q4card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q4card)

    q5card = Q5card()
    q5card.init()
    # print("Query 5")
    q5card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q5card)

    q6card = Q6card()
    q6card.init()
    # print("Query 6")
    q6card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q6card)

    q7card = Q7card()
    q7card.init()
    # print("Query 7")
    q7card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q7card)

    q8card = Q8card()
    q8card.init()
    # print("Query 8")
    q8card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q8card)

    q9card = Q9card()
    q9card.init()
    # print("Query 9")
    q9card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q9card)

    q10card = Q10card()
    q10card.init()
    # print("Query 10")
    q10card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q10card)

    q11card = Q11card()
    q11card.init()
    # print("Query 11")
    q11card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q11card)

    q12card = Q12card()
    q12card.init()
    # print("Query 12")
    q12card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q12card)

    q13card = Q13card()
    q13card.init()
    # print("Query 13")
    q13card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q13card)

    q14card = Q14card()
    q14card.init()
    # print("Query 14")
    q14card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q14card)

    q15card = Q15card()
    q15card.init()
    # print("Query 15")
    q15card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q15card)

    q16card = Q16card()
    q16card.init()
    # print("Query 16")
    q16card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q16card)

    q17card = Q17card()
    q17card.init()
    # print("Query 17")
    q17card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q17card)

    q18card = Q18card()
    q18card.init()
    # print("Query 18")
    q18card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q18card)

    q19card = Q19card()
    q19card.init()
    # print("Query 19")
    q19card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q19card)

    q20card = Q20card()
    q20card.init()
    # print("Query 20")
    q20card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q20card)

    q21card = Q21card()
    q21card.init()
    # print("Query 21")
    q21card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q21card)

    q22card = Q22card()
    q22card.init()
    # print("Query 22")
    q22card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    qcard.append(q22card)

    return qcard

def update_qparams_with_qcard(qcard_list):
    qparams_list = []
    
    for i, qcard in enumerate(qcard_list, start=1):
        qparams_class_name = f"Q{i}params"
        qparams = globals()[qparams_class_name]()
        
        # Copy attributes from qcard to qparams
        for attr in dir(qcard):
            if not attr.startswith('__') and not callable(getattr(qcard, attr)):
                setattr(qparams, attr, getattr(qcard, attr))
        
        qparams_list.append(qparams)
    
    return qparams_list    

if __name__ == "__main__":
    # update table metadata
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

    ranges =  [['2024-10-24 17:00:00', '2024-10-25 19:00:00', '2024-10-28 17:00:00', '2024-11-02 15:15:05'], [800, 1600, 2400, math.inf]]
    keys = ['ol_delivery_d', 'ol_o_id']
    order_line_meta.update_partition_metadata(keys, ranges)  

    # ranges = [[25000], [50000], [75000], [math.inf]]
    # keys = ['i_id']
    # item_meta.update_partition_metadata(keys, ranges) 

    # ranges = [[25000], [50000], [75000], [math.inf]]
    # keys = ['s_i_id']
    # stock_meta.update_partition_metadata(keys, ranges)        

    # get Qcard
    qcard_list = get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    # update Qparams
    qparams_list = update_qparams_with_qcard(qcard_list)

    # calculate query cost
    engine = 'Tiflash'
    
    print(calculate_q1(engine, qparams_list[0]))
    print(calculate_q2(engine, qparams_list[1]))
    print(calculate_q3(engine, qparams_list[2]))
    print(calculate_q4(engine, qparams_list[3]))
    print(calculate_q5(engine, qparams_list[4]))
    print(calculate_q6(engine, qparams_list[5]))
    print(calculate_q7(engine, qparams_list[6]))
    print(calculate_q8(engine, qparams_list[7]))
    print(calculate_q9(engine, qparams_list[8]))  
    print(calculate_q10(engine, qparams_list[9]))
    print(calculate_q11(engine, qparams_list[10]))
    print(calculate_q12(engine, qparams_list[11]))
    print(calculate_q13(engine, qparams_list[12]))
    print(calculate_q14(engine, qparams_list[13]))
    print(calculate_q15(engine, qparams_list[14]))
    print(calculate_q16(engine, qparams_list[15]))
    print(calculate_q17(engine, qparams_list[16]))    
    print(calculate_q18(engine, qparams_list[17]))
    print(calculate_q19(engine, qparams_list[18]))
    print(calculate_q20(engine, qparams_list[19]))
    print(calculate_q21(engine, qparams_list[20]))
    print(calculate_q22(engine, qparams_list[21]))