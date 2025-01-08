from ch_partition_meta import *

## 每个query只有selection算子会受到过滤条件和分区元数据的影响
## 因此每个Qcard类只需要找到selection算子，再通过get_query_card方法计算基数
## 最后修改ch_query_params里的Qparams类的变量

class Qcard():
    def __init__(self):
        self.rows_tablescan_nation = 25 ##tbd
        self.rows_selection_nation = 25
        self.rows_tablescan_region = 5 ##tbd
        self.rows_selection_region = 5 ##tbd
        self.rows_tablescan_customer = 120000 ##tbd
        self.rows_selection_customer = 120000
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rows_selection_supplier = 10000
        self.rows_tablescan_item = 100000 ##tbd
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rows_selection_stock = 400000 ##tbd
        self.rows_tablescan_orders = 125038 ##tbd
        self.rows_selection_orders = 125038 ##tbd    
        self.rows_tablescan_district = 40
        self.rows_selection_district = 40
        self.rows_tablescan_warehouse = 4
        self.rows_selection_warehouse = 4
        self.rows_tablescan_new_order = 36418
        self.rows_selection_new_order = 36418
        self.rows_tablescan_history = 124913
        self.rows_selection_history = 124913

    #  modify specific attribute
    def update_param(self, key, value):
        # 动态设置属性
        if hasattr(self, key):  # 确保属性存在
            setattr(self, key, value)
        else:
            raise ValueError(f"Attribute {key} does not exist")  

    # get the card of the key_idx-th operator
    # update the row_tablescan params
    def get_operator_card(self, partition_meta, key_idx):
        # 检查keys是否命中分区键
        # 如果是,根据filter operators values从partition_meta类获取基数
        # 初始化需要扫描的分区范围
        if self.keys[key_idx] != partition_meta.keys[0]:
            return None
                
        start_partition = 0
        end_partition = len(partition_meta.partition_range) - 1

        # 根据第key_idx个的operator 和 value 确定需要扫描的分区
        for op_idx, operator in enumerate(self.operators):
            if key_idx != op_idx:
                continue

            value = self.values[op_idx]
            if operator == 'gt' or operator == 'ge':
                # 大于或大于等于
                for i in range(start_partition, end_partition + 1):
                    if partition_meta.partition_range[i][0] > value:
                        start_partition = i
                        break
            elif operator == 'lt' or operator == 'le':
                # 小于或小于等于
                for i in range(end_partition, start_partition, -1):
                    print(i)
                    if partition_meta.partition_range[i][0] < value:
                        end_partition = i
                        break
            elif operator == 'eq':
                # 等于
                for i in range(start_partition, end_partition + 1):
                    if partition_meta.partition_range[i][0] > value:
                        start_partition = i
                        end_partition = i
                        break

        # 计算需要扫描的分区及其基数
        scanned_partitions = []
        scanned_partition_cnt = 0
        for i in range(start_partition, end_partition + 1):
            scanned_partitions.append(i)
            scanned_partition_cnt += partition_meta.partition_cnt[i]

        # params_dict = {
        #     'tablescan_customer' : self.rows_tablescan_customer,
        #     'selection_customer' : self.rows_selection_customer,
        #     'tablescan_district' : self.rows_tablescan_district,
        #     'selection_district' : self.rows_selection_district,
        #     'tablescan_history' : self.rows_tablescan_history,
        #     'selection_history' : self.rows_selection_history,
        #     'tablescan_item' : self.rows_tablescan_item,
        #     'selection_item' : self.rows_selection_item,
        #     'tablescan_nation' : self.rows_tablescan_nation,
        #     'selection_nation' : self.rows_selection_nation,
        #     'tablescan_new_order' : self.rows_tablescan_new_order,
        #     'selection_new_order' : self.rows_selection_new_order,
        #     'tablescan_order_line' : self.rows_tablescan_order_line,
        #     'selection_order_line' : self.rows_selection_order_line,
        #     'tablescan_orders' : self.rows_tablescan_orders,
        #     'selection_orders' : self.rows_selection_orders,
        #     'tablescan_region' : self.rows_tablescan_region,
        #     'selection_region' : self.rows_selection_region,
        #     'tablescan_stock' : self.rows_tablescan_stock,
        #     'selection_stock' : self.rows_selection_stock,
        #     'tablescan_supplier' : self.rows_tablescan_supplier,
        #     'selection_supplier' : self.rows_selection_supplier,
        #     'tablescan_warehouse' : self.rows_tablescan_warehouse,
        #     'selection_warehouse': self.rows_selection_warehouse,   
        # }    
        # params_name = "tablescan_" + self.tables[key_idx]
        # params = params_dict.get(params_name)

        self.update_param('rows_tablescan_' + self.tables[key_idx], scanned_partition_cnt)
        # print(self.rows_tablescan_order_line)
        self.update_param('rows_selection_' + self.tables[key_idx], scanned_partition_cnt)
        # print(self.rows_selection_order_line)            
        
        return scanned_partitions, scanned_partition_cnt

    def get_query_card(self):    
        table_dict = {
            'customer' : customer_meta,
            'district' : district_meta,
            'history' : history_meta,
            'item' : item_meta,
            'nation' : nation_meta,
            'new_order' : new_order_meta,
            'order_line' : order_line_meta,
            'orders' : orders_meta,
            'region': region_meta,
            'stock' : stock_meta,
            'supplier' : supplier_meta,
            'warehouse' : warehouse_meta,  
        }        
        for table_idx, table_name in enumerate(self.tables):
            partition_meta_name = table_name
            print("Table: ", partition_meta_name)
            # 调用 get_operator_card 函数
            partition_meta = table_dict.get(partition_meta_name)
            #print(partition_meta)

            scanned_partitions, scanned_partition_cnt = self.get_operator_card(partition_meta, table_idx)
            print("Scanned partitions:", scanned_partitions)
            print("Scanned tuples count:", scanned_partition_cnt)               
            

class Q1card(Qcard):
    def init(self):
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowSize_tablescan_orderline = 65
        self.rows_selection_orderline = 885150 
        self.keys = ['ol_delivery_d']  # filter keys
        self.values = ['2024-10-27 14:00:00'] # filter values
        self.tables = ['order_line']
        self.operators = ['gt'] # filter operators '>'    

# Query2基数不受分区影响
class Q2card(Qcard):
    def init(self):
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

class Q3card(Qcard):
    def init(self):
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
        self.keys = ['o_entry_d']  # filter keys
        self.tables = ['orders']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['gt'] # filter operators '>'


class Q4card(Qcard):
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowsize_tablescan_orderline = 65
        self.rows_selection_orderline = 1250435 ##tbd       
        self.keys = ['o_entry_d']  # filter keys
        self.tables = ['orders']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['gt'] # filter operators '>'  

class Q5card(Qcard):
    def init(self):
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
        self.keys = ['o_entry_d']  # filter keys
        self.tables = ['orders']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'  

class Q6card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd       
        self.keys = ['ol_delivery_d']  # filter keys
        self.tables = ['order_line']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'  

class Q7card(Qcard):
    def init(self):
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
        self.keys = ['o_entry_d']  # filter keys
        self.tables = ['orders']
        self.values = ['2024-10-25 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'  

class Q8card(Qcard):
    def init(self):
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
        self.keys = ['i_id', 'ol_i_id', 's_i_id', 'o_entry_d']  # filter keys
        self.tables = ['item', 'order_line', 'stock', 'orders'] # filter tables
        self.values = [1000, 1000, 1000, '2024-10-23 17:00:00'] # filter values
        self.operators = ['lt', 'lt', 'lt', 'ge'] # filter operators '>'  

class Q9card(Qcard):
    def init(self):
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

class Q10card(Qcard):
    def init(self):
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
        self.keys = ['o_entry_d']  # filter keys
        self.tables = ['orders']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'  

class Q11card(Qcard):
    def init(self):
        self.rows_tablescan_nation = 25 ##tbd
        self.rowsize_tablescan_nation = 193
        self.rows_selection_nation = 25 ##tbd
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314    

class Q12card(Qcard):
    def init(self):        
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd     
        self.keys = ['ol_delivery_d']  # filter keys
        self.tables = ['order_line']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>' 

class Q13card(Qcard):            
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671        
        self.keys = ['o_carrier_id']  # filter keys
        self.tables = ['orders']
        self.values = [8] # filter values
        self.operators = ['gt'] # filter operators '>'         

class Q14card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87   
        self.keys = ['ol_delivery_d']  # filter keys
        self.tables = ['order_line']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'             

class Q15card():
    def init(self):
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314  
        self.keys = ['ol_delivery_d']  # filter keys
        self.tables = ['order_line']
        self.values = ['2024-10-23 17:00:00'] # filter values
        self.operators = ['ge'] # filter operators '>'        

class Q16card():
    def init(self):
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_selection_supplier = 10000 ##tbd
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314    

class Q17card():
    def init(self):
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

class Q18card():
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd    

class Q19card():
    def init(self):
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd  
        self.keys = ['ol_quantity']  # filter keys
        self.tables = ['order_line']
        self.values = [1] # filter values
        self.operators = ['ge'] # filter operators '>'        

class Q20card():   
    def init(self):
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
        self.keys = ['ol_delivery_d']  # filter keys
        self.tables = ['order_line']
        self.values = ['2024-12-23 12:00:00'] # filter values
        self.operators = ['gt'] # filter operators '>'        

class Q21card():
    def init(self):
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

class Q22card():
    def init(self):
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_selection_customer = 120000 ##tbd     
        self.keys = ['c_balance']  # filter keys
        self.tables = ['customer']
        self.values = [49556.891238] # filter values
        self.operators = ['gt'] # filter operators '>'             

# 示例使用
if __name__ == "__main__":
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

    # ranges =  [['2024-10-24 17:00:00'], ['2024-10-25 19:00:00'], ['2024-10-28 17:00:00'], ['2024-11-02 15:15:05']]
    # keys = ['ol_delivery_d']
    # order_line_meta.update_partition_metadata(keys, ranges)
    # print("partition keys: ", order_line_meta.keys)
    # print("partition_cnt: ", order_line_meta.partition_cnt)
    # print("partition_range: ", order_line_meta.partition_range)    

    # q1card = Q1card()
    # q1card.init()
    # q1card.get_query_card()
    # print(q1card.rows_tablescan_order_line)




    
    ranges = [[25000], [50000], [75000], [math.inf]]
    keys = ['i_id']
    item_meta.update_partition_metadata(keys, ranges)

    ranges = [[25000], [50000], [75000], [math.inf]]
    keys = ['ol_i_id']
    order_line_meta.update_partition_metadata(keys, ranges)    

    ranges = [[25000], [50000], [75000], [math.inf]]
    keys = ['s_i_id']
    stock_meta.update_partition_metadata(keys, ranges)    

    ranges =  [['2024-10-24 17:00:00'], ['2024-10-25 19:00:00'], ['2024-10-28 17:00:00'], ['2024-11-02 15:15:05']]
    keys = ['o_entry_d']
    orders_meta.update_partition_metadata(keys, ranges)    

    q8card = Q8card()
    q8card.init()
    q8card.get_query_card()