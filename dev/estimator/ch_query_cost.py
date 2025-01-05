from operators import *
from ch_query_params import *

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
    tablescan_orders.engine = engine
    selection_orders.engine = engine

    tablescan_orderline = TableScan(content, rows_tablescan_orderline, rowsize_tablescan_orderline)
    selection_orderline = Selection(content, rows_selection_orderline, 1)
    tablescan_orderline.engine = engine
    selection_orderline.engine = engine

    cost = tablescan_orders.calculate_cost() + selection_orders.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost()

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

    cost = tablescan_region.calculate_cost() + selection_region.calculate_cost() + tablescan_nation.calculate_cost() + selection_nation.calculate_cost() + tablescan_supplier.calculate_cost() + selection_supplier.calculate_cost() + tablescan_stock.calculate_cost() + selection_stock.calculate_cost() + tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost() + tablescan_customer.calculate_cost() + selection_customer.calculate_cost() + tablescan_orders.calculate_cost() + selection_orders.calculate_cost()
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

    cost = tablescan_orderline.calculate_cost() + selection_orderline.calculate_cost()
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
    cost = 0
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
    cost += tablescan_customer.calculate_cost() + selection_customer.calculate_cost()

    return cost



if __name__ == "__main__":
  engine = 'Tiflash'
  print(calculate_q1(engine))
  print(calculate_q2(engine))
  print(calculate_q3(engine))
  print(calculate_q4(engine))
  print(calculate_q5(engine))
  print(calculate_q6(engine))
  print(calculate_q7(engine))
  print(calculate_q8(engine))  
  print(calculate_q9(engine))
  print(calculate_q10(engine))
  print(calculate_q11(engine))
  print(calculate_q12(engine))
  print(calculate_q13(engine))
  print(calculate_q14(engine))
  print(calculate_q15(engine))
  print(calculate_q16(engine))    
  print(calculate_q17(engine))
  print(calculate_q18(engine))
  print(calculate_q19(engine))
  print(calculate_q20(engine))
  print(calculate_q21(engine))
  print(calculate_q22(engine))