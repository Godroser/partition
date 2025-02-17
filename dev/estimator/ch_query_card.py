#from ch_partition_meta import *
from estimator.ch_partition_meta import *

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

    # get the card of the table
    # table_idx: the index of the table in self.tables
    # update the row_tablescan params
    def get_table_card(self, partition_meta, table_idx):
        # 这个表要扫描的分区和基数
        scanned_partitions = []    
        scanned_partition_card = 0             
        
        # 检查表是否有分区
        if len(partition_meta.keys) == 0:
            scanned_partitions = [0, 1, 2, 3]
            scanned_partition_card = partition_meta.count
            self.update_param('rows_tablescan_' + self.tables[table_idx], scanned_partition_card)
            self.update_param('rows_selection_' + self.tables[table_idx], scanned_partition_card)            
            return scanned_partitions, scanned_partition_card
        
        # 默认扫描全部分区
        for i in range(len(partition_meta.partition_range[0])):
            scanned_partitions.append(i)  


        # print(self.keys[table_idx])
        # print(partition_meta.partition_range)

        #遍历这个表所有的过滤条件
        for key_idx, key in enumerate(self.keys[table_idx]):
            # 检查key是否命中分区键
            # 如果是,根据filter operators values从partition_meta类获取基数
            # 否则所有分区都要扫描
            #print("key_idx: ", key_idx)
            if key != partition_meta.keys[0]:
                scanned_partition = []
                for i in range(len(partition_meta.partition_range[0])):
                    scanned_partition.append(i)
                # 求各个过滤条件的交集
                scanned_partitions = list(set(scanned_partitions) & set(scanned_partition))   
                continue                 
                    
            start_partition = 0
            end_partition = len(partition_meta.partition_range[0]) - 1


            # 根据第key_idx个的key operator 和 value 确定需要扫描的分区
            operator = self.operators[table_idx][key_idx]
            value = self.values[table_idx][key_idx]

            if operator == 'gt' or operator == 'ge':
                # 大于或大于等于
                for i in range(start_partition, end_partition + 1):
                    #print("debug partition_meta.partition_range[i][0]: ", partition_meta.partition_range[key_idx][i])
                    #print("debug value: ", value)
                    if partition_meta.partition_range[0][i] > value:
                        start_partition = i
                        break
            elif operator == 'lt' or operator == 'le':
                # 小于或小于等于
                #print("check: ",end_partition, start_partition)
                for i in range(end_partition, start_partition-1, -1):
                    #print(i)
                    # print("check: ",partition_meta.partition_range)
                    #print("check: ", partition_meta.partition_range[0][i], value)
                    if partition_meta.partition_range[0][i] < value: 
                        end_partition = i+1
                        break
            elif operator == 'eq':
                # 等于
                for i in range(start_partition, end_partition + 1):
                    if partition_meta.partition_range[0][i] > value:
                        start_partition = i
                        end_partition = i
                        break

            # 计算需要扫描的分区及其基数
            scanned_partition = []
            for i in range(start_partition, end_partition + 1):
                scanned_partition.append(i)
                # #print("check artition_meta.partition_cnt[i]: ", partition_meta.partition_cnt[i])
                # scanned_partition_card += partition_meta.partition_cnt[i]

            # 求各个过滤条件的交集
            scanned_partitions = list(set(scanned_partitions) & set(scanned_partition))

        # 计算需要扫描分区的基数
        for p in scanned_partitions:
            scanned_partition_card += partition_meta.partition_cnt[p]

        self.update_param('rows_tablescan_' + self.tables[table_idx], scanned_partition_card)
        # print(self.rows_tablescan_order_line)
        self.update_param('rows_selection_' + self.tables[table_idx], scanned_partition_card)
        # print(self.rows_selection_order_line)            
        
        return scanned_partitions, scanned_partition_card



    # # get the card of the key_idx-th operator
    # # update the row_tablescan params
    # def get_operator_card(self, partition_meta, key_idx):
    #     # 检查表是否有分区
    #     if len(partition_meta.keys) == 0:
    #         return 0, 0  # do nothing
        
    #     # 检查keys是否命中分区键
    #     # 如果是,根据filter operators values从partition_meta类获取基数
    #     # 初始化需要扫描的分区范围
    #     #print("key_idx: ", key_idx)
    #     if self.keys[key_idx] != partition_meta.keys[0]:
    #         return 0, 0
                
    #     start_partition = 0
    #     end_partition = len(partition_meta.partition_range[0]) - 1

    #     # 根据第key_idx个的operator 和 value 确定需要扫描的分区
    #     for op_idx, operator in enumerate(self.operators):
    #         if key_idx != op_idx:
    #             continue

    #         value = self.values[op_idx]
    #         if operator == 'gt' or operator == 'ge':
    #             # 大于或大于等于
    #             for i in range(start_partition, end_partition + 1):
    #                 #print("debug partition_meta.partition_range[i][0]: ", partition_meta.partition_range[key_idx][i])
    #                 #print("debug value: ", value)
    #                 if partition_meta.partition_range[0][i] > value:
    #                     start_partition = i
    #                     break
    #         elif operator == 'lt' or operator == 'le':
    #             # 小于或小于等于
    #             #print("check: ",end_partition, start_partition)

    #             for i in range(end_partition, start_partition, -1):
    #                 # print(i)
    #                 # print("check: ",partition_meta.partition_range)
    #                 if partition_meta.partition_range[0][i] < value:
    #                     end_partition = i
    #                     break
    #         elif operator == 'eq':
    #             # 等于
    #             for i in range(start_partition, end_partition + 1):
    #                 if partition_meta.partition_range[0][i] > value:
    #                     start_partition = i
    #                     end_partition = i
    #                     break

    #     # 计算需要扫描的分区及其基数
    #     scanned_partitions = []
    #     scanned_partition_cnt = 0
    #     for i in range(start_partition, end_partition + 1):
    #         scanned_partitions.append(i)
    #         #print("check artition_meta.partition_cnt[i]: ", partition_meta.partition_cnt[i])
    #         scanned_partition_cnt += partition_meta.partition_cnt[i]

    #     self.update_param('rows_tablescan_' + self.tables[key_idx], scanned_partition_cnt)
    #     # print(self.rows_tablescan_order_line)
    #     self.update_param('rows_selection_' + self.tables[key_idx], scanned_partition_cnt)
    #     # print(self.rows_selection_order_line)            
        
    #     return scanned_partitions, scanned_partition_cnt

    def get_query_card(self, customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta):    
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
            #print("Table: ", partition_meta_name)
            # 调用 get_table_card 函数
            partition_meta = table_dict.get(partition_meta_name)
            #print(partition_meta)
            # print("Partition keys: ", partition_meta.keys)
            scanned_partitions, scanned_partition_cnt = self.get_table_card(partition_meta, table_idx)
            # print("Scanned partitions:", scanned_partitions)
            # print("Scanned tuples count:", scanned_partition_cnt)               
            

class Q1card(Qcard):
    def init(self):
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowSize_tablescan_orderline = 65
        self.rows_selection_orderline = 885150 

        ## tables:[] keys:[[]]一个table内涉及多个keys的过滤 operators:[[]] values:[[]]
        self.keys = [['ol_delivery_d']]  # filter keys
        self.values = [[datetime(2024, 10, 27, 14, 0, 0)]]  # filter values
        self.tables = ['order_line']
        self.operators = [['gt']] # filter operators '>'    

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
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []         

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
        self.keys = [['o_entry_d']]  # filter keys
        self.tables = ['orders']
        self.values = [[datetime(2024, 10, 27, 17, 0, 0)]] # filter values
        self.operators = [['gt']] # filter operators '>'


class Q4card(Qcard):
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowsize_tablescan_orderline = 65
        self.rows_selection_orderline = 1250435 ##tbd       
        self.keys = [['o_entry_d', 'o_entry_d']]  # filter keys
        self.tables = ['orders']
        self.values = [[datetime(2024, 10, 27, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [['gt', 'lt']] # filter operators '>'  

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
        self.values = [datetime(2024, 10, 27, 17, 0, 0)] # filter values
        self.operators = [['ge']] # filter operators '>'  

class Q6card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd       
        self.keys = [['ol_delivery_d', 'ol_delivery_d', 'ol_quantity', 'ol_quantity']]  # filter keys
        self.tables = ['order_line']
        self.values = [[datetime(2024, 10, 23, 17, 0, 0), datetime(2024, 10, 25, 17, 0, 0), 1, 100000]] # filter values
        self.operators = [['ge', 'lt', 'gt', 'lt']] # filter operators '>'  

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
        self.keys = [['o_entry_d']]  # filter keys
        self.tables = ['orders']
        self.values = [[datetime(2024, 10, 27, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [['ge', 'lt']] # filter operators '>'  

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
        self.keys = [['ol_i_id'], ['o_entry_d', 'o_entry_d']]  # filter keys
        self.tables = ['order_line', 'orders'] # filter tables
        self.values = [[1000],[datetime(2024, 10, 23, 17, 0, 0), datetime(2024, 10, 25, 17, 0, 0)]] # filter values
        self.operators = [['lt'], ['ge', 'lt']] # filter operators '>'  

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
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []             

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
        self.keys = [['o_entry_d']]  # filter keys
        self.tables = ['orders']
        self.values = [[datetime(2024, 10, 27, 17, 0, 0)]] # filter values
        self.operators = [['ge']] # filter operators '>'  

class Q11card(Qcard):
    def init(self):
        self.rows_tablescan_nation = 25 ##tbd
        self.rowsize_tablescan_nation = 193
        self.rows_selection_nation = 25 ##tbd
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314  
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []            

class Q12card(Qcard):
    def init(self):        
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd     
        self.keys = [['ol_delivery_d']]  # filter keys
        self.tables = ['order_line']
        self.values = [[datetime(2024, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [['ge']] # filter operators '>' 

class Q13card(Qcard):            
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671        
        self.keys = [['o_carrier_id']]  # filter keys
        self.tables = ['orders']
        self.values = [[8]] # filter values
        self.operators = [['gt']] # filter operators '>'         

class Q14card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87   
        self.keys = [['ol_delivery_d', 'ol_delivery_d']]  # filter keys
        self.tables = ['order_line']
        self.values = [[datetime(2024, 10, 23, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [['ge', 'lt']] # filter operators '>'             

class Q15card(Qcard):
    def init(self):
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314  
        self.keys = [['ol_delivery_d']]  # filter keys
        self.tables = ['order_line']
        self.values = [[datetime(2024, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [['ge']] # filter operators '>'        

class Q16card(Qcard):
    def init(self):
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_selection_supplier = 10000 ##tbd
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314    
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []          

class Q17card(Qcard):
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
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []          

class Q18card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd    
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []          

class Q19card(Qcard):
    def init(self):
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd  
        self.keys = [['ol_quantity'], ['i_price', 'i_price']]  # filter keys
        self.tables = ['order_line', 'item']
        self.values = [[5], [1, 40]] # filter values
        self.operators = [['ge'], ['ge', 'lt']] # filter operators '>'        

class Q20card(Qcard):   
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
        self.keys = [['ol_delivery_d']]  # filter keys
        self.tables = ['order_line']
        self.values = [[datetime(2024, 10, 27, 17, 0, 0)]] # filter values
        self.operators = [['gt']] # filter operators '>'        

class Q21card(Qcard):
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
        self.keys = []
        self.values = []
        self.tables = []
        self.operators = []          

class Q22card(Qcard):
    def init(self):
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_selection_customer = 120000 ##tbd     
        self.keys = [['c_balance']]  # filter keys
        self.tables = ['customer']
        self.values = [[0]] # filter values
        self.operators = [['gt']] # filter operators '>'             

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




    
    ranges = [[25000, 50000, 75000, 500000]]
    keys = ['i_id']
    item_meta.update_partition_metadata(keys, ranges)

    ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)], [800, 1600, 2400, 10000]]
    keys = ['ol_delivery_d', 'ol_o_id']
    order_line_meta.update_partition_metadata(keys, ranges)   

    ranges = [[25000, 50000, 75000, 500000]]
    keys = ['s_i_id']
    stock_meta.update_partition_metadata(keys, ranges)    

    # ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)], [750, 1500, 2250, 3600]]
    # keys = ['o_entry_d', 'o_id']
    ranges =  [[datetime(2024, 10, 24, 17, 0, 0), datetime(2024, 10, 25, 19, 0, 0), datetime(2024, 10, 28, 17, 0, 0), datetime(2024, 11, 2, 15, 15, 5)]]
    keys = ['o_entry_d']
    orders_meta.update_partition_metadata(keys, ranges)

    ranges = [[750, 1500, 2250, 3001]]
    keys = ['c_id']
    customer_meta.update_partition_metadata(keys, ranges)

    print("Q8")
    q8card = Q8card()
    q8card.init()
    # print("rows_tablescan_item: ", q8card.rows_tablescan_item)
    # print("rows_tablescan_stock: ", q8card.rows_tablescan_stock)
    # print("rows_tablescan_orders: ", q8card.rows_tablescan_orders)    
    q8card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)
    # print("rows_tablescan_item: ", q8card.rows_tablescan_item)
    # print("rows_tablescan_stock: ", q8card.rows_tablescan_stock)
    # print("rows_tablescan_orders: ", q8card.rows_tablescan_orders)

    print("Q1")
    q1card = Q1card()
    q1card.init()
    q1card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    print("Q2")
    q2card = Q2card()
    q2card.init()
    q2card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    print("Q10")
    q10card = Q10card()
    q10card.init()
    q10card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    print("Q7")
    q7card = Q7card()
    q7card.init()
    q7card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)

    print("Q22")
    q22card = Q22card()
    q22card.init()
    q22card.get_query_card(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta)  