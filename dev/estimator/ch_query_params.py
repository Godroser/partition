# class params():
#     def __init__(self):
#       self.rows = 100
#       self.rowSize = 10
#       self.dict_tablescan = {'rows': rows, 'rowSize': rowSize}
#       self.dict_selection = {'rows': , 'numFuncs': 1}
#       self.dict_hashagg = {'rows': 100, 'rowSize': 10, 'sortitems': 1, 'numFuncs': , 'buildRows': , 'buildRowSize': , 'nKeys': , 'probeRows':}
#       self.dict_tablereader = {'rows': , 'rowSize': }
#       self.dict_hashagg1 = {'rows': 100, 'rowSize': 10, 'sortitems': 1, 'numFuncs': , 'buildRows': , 'buildRowSize': , 'nKeys': , 'probeRows':}
#       self.dict_projection = {'rows': , 'numFuncs': }
#       self.dict_sort = {'rows': , 'rowSize': , 'sortitems': , 'numFuncs': }


class Q1params():
  def __init__(self):
    ##tbd表示和分区有关的参数
    self.rows_tablescan_orderline = 1250435 ##tbd
    self.rowSize_tablescan_orderline = 65
    self.rows_selection_orderline = 885150 

    # # 只计算受分区影响的算子代价
    # self.dict_tablescan = {'rows': self.rows_tablescan, 'rowSize': self.rowSize_tablescan}
    # self.dict_selection = {'rows': self.rows_selection, 'numFuncs': 1}
    # self.dict_hashagg = {'rows': 30, 'rowSize': 65, 'aggFuncs': 1, 'numFuncs': 1, 'buildRows': 30, 'buildRowSize': 65, 'nKeys': 1, 'probeRows':}
    # self.dict_tablereader = {'rows': , 'rowSize': }
    # self.dict_hashagg1 = {'rows': 100, 'rowSize': 10, 'sortitems': 1, 'numFuncs': , 'buildRows': , 'buildRowSize': , 'nKeys': , 'probeRows':}
    # self.dict_projection = {'rows': , 'numFuncs': }
    # self.dict_sort = {'rows': , 'rowSize': , 'sortitems': , 'numFuncs': }

class Q2params():
  def __init__(self):
    self.rows_tablescan_region = 5 #tbd
    self.rowsize_tablescan_region = 185
    self.rows_selection_region = 5
    self.rows_tablescan_nation = 25 #tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25
    self.rows_tablescan_supplier = 10000 #tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_selection_supplier = 10000 #tbd
    self.rows_tablescan_stock = 400000 #tbd
    self.rowsize_tablescan_stock = 314
    self.rows_selection_stock = 400000 #tbd
    self.rows_tablescan_item = 100000 #tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 #tbd

class Q3params():
  def __init__(self):
    self.rows_tablescan_orderline = 1250435 ##tbd
    self.rowsize_tablescan_orderline = 65
    self.rows_selection_orderline = 1250435 ##tbd
    self.rows_tablescan_neworder = 36418 ##tbd
    self.rowsize_tablescan_neworder = 12
    self.rows_selection_neworder = 36418 ##tbd
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671
    self.rows_selection_customer = 120000 ##tbd
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    
class Q4params():
  def __init__(self):
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    self.rows_tablescan_orderline = 1250435 ##tbd
    self.rowsize_tablescan_orderline = 65
    self.rows_selection_orderline = 1250435 ##tbd

class Q5params():
  def __init__(self):
    self.rows_tablescan_region = 5 #tbd
    self.rowsize_tablescan_region = 185
    self.rows_selection_region = 5 #tbd
    self.rows_tablescan_nation = 25 #tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25
    self.rows_tablescan_supplier = 10000 #tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_selection_supplier = 400000 #tbd
    self.rows_tablescan_stock = 400000 #tbd
    self.rowsize_tablescan_stock = 314
    self.rows_selection_stock = 400000 #tbd
    self.rows_tablescan_orderline = 1250435 ##tbd
    self.rowsize_tablescan_orderline = 65
    self.rows_selection_orderline = 1250435 ##tbd
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671
    self.rows_selection_customer = 120000 ##tbd        
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd    

class Q6params():
  def __init__(self):
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd    

class Q7params():
  def __init__(self):
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25 ##tbd
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25 ##tbd
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd    

class Q8params():
  def __init__(self):
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_tablescan_region = 5 ##tbd
    self.rowsize_tablescan_region = 185
    self.rows_selection_region = 5 ##tbd
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314
    self.rows_selection_stock = 400000 ##tbd
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd    

class Q9params():
  def __init__(self):
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314    

class Q10params():
  def __init__(self):
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671    

class Q11params():
  def __init__(self):
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25 ##tbd
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314    

class Q12params():
  def __init__(self):  
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd    

class Q13params():
  def __init__(self):
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671

class Q14params():
  def __init__(self):
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87

class Q15params():
  def __init__(self):
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314

class Q16params():
  def __init__(self):
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_selection_supplier = 10000 ##tbd
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314

class Q17params():
  def __init__(self):
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_selection_supplier = 10000 ##tbd
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65

class Q18params():
  def __init__(self):
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd

class Q19params():
  def __init__(self):
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd

class Q20params():
  def __init__(self):
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_selection_supplier = 10000 ##tbd
    self.rows_tablescan_item = 100000 ##tbd
    self.rowsize_tablescan_item = 87
    self.rows_selection_item = 100000 ##tbd
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25 ##tbd
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202

class Q21params():
  def __init__(self):
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_tablescan_nation = 25 ##tbd
    self.rowsize_tablescan_nation = 193
    self.rows_selection_nation = 25 ##tbd
    self.rows_tablescan_supplier = 10000 ##tbd
    self.rowsize_tablescan_supplier = 202
    self.rows_tablescan_stock = 400000 ##tbd
    self.rowsize_tablescan_stock = 314
    self.rows_tablescan_order_line = 1250435 ##tbd
    self.rowsize_tablescan_order_line = 65
    self.rows_selection_order_line = 1250435 ##tbd
    self.rows_tablescan_orders = 125038 ##tbd
    self.rowsize_tablescan_orders = 36
    self.rows_selection_orders = 125038 ##tbd

class Q22params():
  def __init__(self):
    self.rows_tablescan_customer = 120000 ##tbd
    self.rowsize_tablescan_customer = 671
    self.rows_selection_customer = 120000 ##tbd