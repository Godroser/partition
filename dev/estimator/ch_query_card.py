#from ch_partition_meta import *
from estimator.ch_partition_meta import *
from estimator.ch_query_params import *

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

        self.rowsize_tablescan_nation = 0
        self.rowsize_tablescan_region = 0
        self.rowsize_tablescan_customer = 0
        self.rowsize_tablescan_supplier = 0
        self.rowsize_tablescan_item = 0
        self.rowsize_tablescan_order_line = 0
        self.rowsize_tablescan_stock = 0
        self.rowsize_tablescan_orders = 0
        self.rowsize_tablescan_district = 0
        self.rowsize_tablescan_warehouse = 0
        self.rowsize_tablescan_new_order = 0
        self.rowsize_tablescan_history = 0

        self.rows_tablescan_nation_replica = 0
        self.rows_selection_nation_replica = 0
        self.rows_tablescan_region_replica = 0
        self.rows_selection_region_replica = 0
        self.rows_tablescan_customer_replica = 0
        self.rows_selection_customer_replica = 0
        self.rows_tablescan_supplier_replica = 0
        self.rows_selection_supplier_replica = 0
        self.rows_tablescan_item_replica = 0
        self.rows_selection_item_replica = 0
        self.rows_tablescan_order_line_replica = 0
        self.rows_selection_order_line_replica = 0
        self.rows_tablescan_stock_replica = 0
        self.rows_selection_stock_replica = 0
        self.rows_tablescan_orders_replica = 0
        self.rows_selection_orders_replica = 0
        self.rows_tablescan_district_replica = 0
        self.rows_selection_district_replica = 0
        self.rows_tablescan_warehouse_replica = 0
        self.rows_selection_warehouse_replica = 0
        self.rows_tablescan_new_order_replica = 0
        self.rows_selection_new_order_replica = 0
        self.rows_tablescan_history_replica = 0
        self.rows_selection_history_replica = 0

        self.rowsize_tablescan_nation_replica = 0
        self.rowsize_tablescan_region_replica = 0
        self.rowsize_tablescan_customer_replica = 0
        self.rowsize_tablescan_supplier_replica = 0
        self.rowsize_tablescan_item_replica = 0
        self.rowsize_tablescan_order_line_replica = 0
        self.rowsize_tablescan_stock_replica = 0
        self.rowsize_tablescan_orders_replica = 0
        self.rowsize_tablescan_district_replica = 0
        self.rowsize_tablescan_warehouse_replica = 0
        self.rowsize_tablescan_new_order_replica = 0
        self.rowsize_tablescan_history_replica = 0

        # 维护访问replica的表, 记录table_name
        self.scan_table_replica = []

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
    def get_table_card(self, partition_meta, table_idx, candidates):
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
            if key == None:
                continue

            # 这里要确保处理的key所在的表和partition_meta对应的表相同
            # key在replica, partition_meta是原表的
            if (key in candidates[table_idx]['replicas']) and table_idx < 12:
                print("skip replica key")
                continue
            # key在原表, partition_meta是replica的
            if (key not in candidates[table_idx]['replicas']) and table_idx >= 12:
                print("skip original table key")
                continue

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

    # 计算每个query扫描的各个表的基数
    def get_query_card(self, table_meta, candidates):    
        table_dict = {'customer': 0, 'district': 1, 'history': 2, 'item': 3, 'nation': 4, 'new_order': 5, 'order_line': 6, 'orders': 7, 'region': 8, 'stock': 9, 'supplier': 10, 'warehouse': 11}  

        for table_idx, table_name in enumerate(self.tables):
            # 对应table的candidate
            candidate = next((c for c in candidates if c['name'] == table_name), None)            

            # 如果这个表没有replica, 则只需要扫描原始table
            if candidate['replicas'] == None:
                # 这个表上没有过滤操作, 默认扫描全部tuple
                if self.operators[table_idx] == None:
                    continue
                table_meta_idx = table_dict.get(table_name)
                # print("Table: ", partition_meta_name)
                # 调用 get_table_card 函数
                partition_meta = table_meta[table_meta_idx]
                #print(partition_meta)
                # print("Partition keys: ", partition_meta.keys)
                scanned_partitions, scanned_partition_cnt = self.get_table_card(partition_meta, table_idx, candidates)
                # print("Scanned partitions:", scanned_partitions)
                # print("Scanned tuples count:", scanned_partition_cnt)  

            # 如果这个表有replica, 则需要扫描原始table和replica
            else:  
                # 这个表上没有过滤操作, 默认扫描全部tuple
                if self.operators[table_idx] == None:
                    continue
                # 计算原始表基数
                table_meta_idx = table_dict.get(table_name)
                # print("Table: ", partition_meta_name)
                # 调用 get_table_card 函数
                partition_meta = table_meta[table_meta_idx]
                scanned_partitions, scanned_partition_cnt = self.get_table_card(partition_meta, table_idx, candidates)
                # print("Scanned partitions:", scanned_partitions)
                # print("Scanned tuples count:", scanned_partition_cnt) 
                
                # 计算replica的基数
                table_replica_meta_idx = table_meta_idx + 12
                partition_meta_replica = table_meta[table_replica_meta_idx]
                scanned_partitions_replica, scanned_partition_cnt_replica = self.get_table_card(partition_meta_replica, table_idx, candidates)
                # print("Scanned partitions:", scanned_partitions_replica)
                # print("Scanned tuples count:", scanned_partition_cnt_replica)
             
                

             

    # 更新query涉及的每一个表的rowsize
    def update_table_rowsize(self, table_columns, candidates):
        table_dict = {'customer': 0, 'district': 1, 'history': 2, 'item': 3, 'nation': 4, 'new_order': 5, 'order_line': 6, 'orders': 7, 'region': 8, 'stock': 9, 'supplier': 10, 'warehouse': 11}     

        for table_idx, table_name in enumerate(self.tables):
            # 对应table的candidate
            candidate = next((c for c in candidates if c['name'] == table_name), None)

            # 如果这个表没有replica, 则不需要更新rowsize
            if candidate['replicas'] == None:
                continue

            # 计算rowsize
            rowsize_replica = 0
            rowsize = 0

            scan_replica = False

            # 找到对应的table_column类
            column_class_idx = table_dict.get(table_name)
            table_column = table_columns[column_class_idx]

            # 遍历replica里面的列, 修改rowsize
            for column in candidate['replicas']:

                if column in table_column.columns:
                    idx = table_column.columns.index(column)
                    rowsize_replica += table_column.columns_size[idx]
                    scan_replica = True
                else:
                    print("Column not found in table columns")
                    break
            
            # 计算rowsize的大小
            rowsize = sum(table_column.columns_size) - rowsize_replica

            # 加上主键的大小
            primary_keys_size = sum(table_column.columns_size[table_column.columns.index(pk)] for pk in table_column.primary_keys)
            rowsize_replica += primary_keys_size
        
            self.update_param('rowsize_tablescan_' + table_name, rowsize)
            self.update_param('rowsize_tablescan_' + table_name + '_replica', rowsize_replica)

            # 增加扫描replica的列表
            if scan_replica == True:
                self.scan_table_replica.append(table_name)



class Q1card(Qcard):
    def init(self):
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowsize_tablescan_orderline = 65
        self.rows_selection_orderline = 885150 

        # 记录每个表读取的columns, 和tables顺序一致
        self.columns = [['ol_number',  'ol_quantity', 'ol_amount', 'ol_delivery_d']]

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

        # 记录每个表读取的columns, 和tables顺序一致
        self.columns = [ ["i_id", "i_name", "i_data"], ["s_i_id", "s_w_id", "s_quantity"], ["s_suppkey", "s_name", "s_address", "s_phone", "s_comment", "s_nationkey"], ["n_nationkey", "n_name", "n_regionkey"], ["r_regionkey", "r_name"], ["m_i_id", "m_s_quantity"] ]  


        self.keys = [[], [], [], [], []]
        self.values = [[], [], [], [], []]
        self.tables = ["item", "stock", "supplier", "nation", "region"]  
        self.operators = [[], [], [], [], []]         

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

        self.columns = [["c_state", "c_id", "c_w_id", "c_d_id"], ["no_w_id", "no_d_id", "no_o_id"], ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"], ["ol_o_id", "ol_w_id", "ol_d_id", "ol_amount"]]

        self.keys = [[], [], ['o_entry_d'], []]  # filter keys
        self.tables = ["customer", "new_order", "orders", "order_line"]
        self.values = [[], [], [datetime(2024, 10, 27, 17, 0, 0)], []] # filter values
        self.operators = [[], [], ['gt'], []] # filter operators '>'


class Q4card(Qcard):
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_orderline = 1250435 ##tbd
        self.rowsize_tablescan_orderline = 65
        self.rows_selection_orderline = 1250435 ##tbd       

        self.columns = [["o_ol_cnt", "o_id", "o_w_id", "o_d_id", "o_entry_d"], ["ol_o_id", "ol_w_id", "ol_d_id", "ol_delivery_d"]]

        self.keys = [['o_entry_d', 'o_entry_d'], []]  # filter keys
        self.tables = ["orders", "order_line"]
        self.values = [[datetime(2024, 10, 30, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)], []] # filter values
        self.operators = [['gt', 'lt'], []] # filter operators '>'  

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

    
        self.columns = [["c_id", "c_w_id", "c_d_id", "c_state"], ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"], ["ol_o_id", "ol_w_id", "ol_d_id", "ol_amount", "ol_i_id"], ["s_w_id", "s_i_id"], ["s_suppkey", "s_nationkey"], ["n_name", "n_nationkey", "n_regionkey"], ["r_name", "r_regionkey"]]
          
        self.keys = [[], ['o_entry_d'], [], [], [], [], []]  # filter keys
        self.tables = ["customer", "orders", "order_line", "stock", "supplier", "nation", "region"]
        self.values = [[], [datetime(2024, 10, 30, 17, 0, 0)], [], [], [], [], []] # filter values
        self.operators = [[], ['ge'], [], [], [], [], []] # filter operators '>'  

class Q6card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd       

        self.columns = [["ol_amount", "ol_delivery_d", "ol_quantity"]]

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

        self.columns = [
            ["ol_amount", "ol_supply_w_id", "ol_i_id", "ol_w_id", "ol_d_id", "ol_o_id"],
            ["s_w_id", "s_i_id"],
            ["o_w_id", "o_d_id", "o_id", "o_c_id", "o_entry_d"],
            ["c_id", "c_w_id", "c_d_id", "c_state"],
            ["s_suppkey", "s_nationkey"],
            ["n_nationkey", "n_name"]]


        self.keys = [[], [], ['o_entry_d', 'o_entry_d'], [], [], []]  # filter keys
        self.tables = ["order_line", "stock", "orders", "customer", "supplier", "nation"]
        self.values = [[], [], [datetime(2024, 10, 30, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)], [], [], []] # filter values
        self.operators = [[], [], ['ge', 'lt'], [], [], []] # filter operators '>'  

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

        self.columns = [["i_id", "i_data"],
            ["s_suppkey", "s_nationkey"],
            ["s_w_id", "s_i_id"],
            ["ol_amount", "ol_i_id", "ol_supply_w_id", "ol_w_id", "ol_d_id", "ol_o_id"],
            ["o_w_id", "o_d_id", "o_id", "o_c_id", "o_entry_d"],
            ["c_id", "c_w_id", "c_d_id", "c_state"],
            ["n_nationkey", "n_name", "n_regionkey"],
            ["r_name", "r_regionkey"]
        ]

        self.keys = [[], [], [], ['ol_i_id'], ['o_entry_d', 'o_entry_d'], [], [], []]  # filter keys
        self.tables = ["item", "supplier", "stock", "order_line", "orders", "customer", "nation", "region"] # filter tables
        self.values = [[], [], [], [1000],[datetime(2024, 10, 30, 17, 0, 0), datetime(2025, 10, 25, 17, 0, 0)], [], [], []] # filter values
        self.operators = [[], [], [], ['lt'], ['ge', 'lt'], [], [], []] # filter operators '>'  

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

        self.columns = [
            ["i_id", "i_data"],
            ["s_w_id", "s_i_id"],
            ["s_suppkey", "s_nationkey"],
            ["ol_amount", "ol_i_id", "ol_supply_w_id", "ol_w_id", "ol_d_id", "ol_o_id"],
            ["o_w_id", "o_d_id", "o_id", "o_entry_d"],
            ["n_name", "n_nationkey"]
        ]
         
        self.keys = [[], [], [], [], [], []]
        self.values = [[], [], [], [], [], []]
        self.tables = ["item", "stock", "supplier", "order_line", "orders", "nation"]
        self.operators = [[], [], [], [], [], []]             

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

        self.columns = [
            ["c_id", "c_last", "c_w_id", "c_d_id", "c_city", "c_phone", "c_state"],
            ["o_c_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            ["ol_amount", "ol_w_id", "ol_d_id", "ol_o_id", "ol_delivery_d"],
            ["n_name", "n_nationkey"]
        ]

        self.keys = [[], ['o_entry_d'], [], []]  # filter keys
        self.tables = ["customer", "orders", "order_line", "nation"]
        self.values = [[], [datetime(2024, 10, 30, 17, 0, 0)], [], []] # filter values
        self.operators = [[], ['ge'], [], []] # filter operators '>'  

class Q11card(Qcard):
    def init(self):
        self.rows_tablescan_nation = 25 ##tbd
        self.rowsize_tablescan_nation = 193
        self.rows_selection_nation = 25 ##tbd
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314  

        self.columns = [
            ["s_i_id", "s_w_id", "s_order_cnt"],
            ["s_suppkey", "s_nationkey"],
            ["n_name", "n_nationkey"]
        ]

        self.keys = [[], [], []]
        self.values = [[], [], []]
        self.tables = ["stock", "supplier", "nation"]
        self.operators = [[], [], []]            

class Q12card(Qcard):
    def init(self):        
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd     

        self.columns = [
            ["o_ol_cnt", "o_carrier_id", "o_w_id", "o_d_id", "o_id", "o_entry_d"],
            ["ol_w_id", "ol_d_id", "ol_o_id", "ol_delivery_d"]
        ]

        self.keys = [[], ['ol_delivery_d']]  # filter keys
        self.tables = ["orders", "order_line"]
        self.values = [[], [datetime(2024, 10, 23, 17, 0, 0)]] # filter values
        self.operators = [[], ['ge']] # filter operators '>' 

class Q13card(Qcard):            
    def init(self):
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671        

        
        self.columns = [
            ["c_id", "c_w_id", "c_d_id"],
            ["o_id", "o_w_id", "o_d_id", "o_c_id", "o_carrier_id"]
        ]

        self.keys = [[], ['o_carrier_id']]  # filter keys
        self.tables = ["customer", "orders"]
        self.values = [[], [8]] # filter values
        self.operators = [[], ['gt']] # filter operators '>'         

class Q14card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87   


        self.columns = [
            ["ol_amount", "ol_i_id", "ol_delivery_d"],
            ["i_id", "i_data"]
        ]

        self.keys = [['ol_delivery_d', 'ol_delivery_d'], []]  # filter keys
        self.tables = ["order_line", "item"]
        self.values = [[datetime(2024, 10, 23, 17, 0, 0), datetime(2025, 10, 23, 17, 0, 0)], []] # filter values
        self.operators = [['ge', 'lt'], []] # filter operators '>'             

class Q15card(Qcard):
    def init(self):
        self.rows_tablescan_supplier = 10000 ##tbd
        self.rowsize_tablescan_supplier = 202
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd
        self.rows_tablescan_stock = 400000 ##tbd
        self.rowsize_tablescan_stock = 314  


        self.columns = [
            ["ol_amount", "ol_i_id", "ol_supply_w_id", "ol_delivery_d"],
            ["s_w_id", "s_i_id"],
            ["s_suppkey", "s_name", "s_address", "s_phone"]
        ]

        self.keys = [['ol_delivery_d'], [], []]  # filter keys
        self.tables = ["order_line", "stock", "supplier"]
        self.values = [[datetime(2024, 10, 23, 17, 0, 0)], [], []] # filter values
        self.operators = [['ge'], [], []] # filter operators '>'        

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

        
        self.columns = [
            ["s_w_id", "s_i_id"],
            ["i_name", "i_id", "i_data", "i_price"],
            ["s_suppkey", "s_comment"]
        ]

        self.keys = [[], [], []]
        self.values = [[], [], []]
        self.tables = ["stock", "item", "supplier"]
        self.operators = [[], [], []]          

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

        
        self.columns = [
            ["ol_amount", "ol_quantity", "ol_i_id"],
            ["i_id"]
        ]

        self.keys = [[], []]
        self.values = [[], []]
        self.tables = ["order_line", "item"]
        self.operators = [[], []]          

class Q18card(Qcard):
    def init(self):
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_tablescan_orders = 125038 ##tbd
        self.rowsize_tablescan_orders = 36
        self.rows_selection_orders = 125038 ##tbd    

        
        self.columns = [
            ["c_id", "c_last", "c_w_id", "c_d_id"],
            ["o_id", "o_c_id", "o_w_id", "o_d_id", "o_entry_d", "o_ol_cnt"],
            ["ol_amount", "ol_w_id", "ol_d_id", "ol_o_id"]
        ]

        self.keys = [[], [], []]
        self.values = [[], [], []]
        self.tables = ["customer", "orders", "order_line"]
        self.operators = [[], [], []]          

class Q19card(Qcard):
    def init(self):
        self.rows_tablescan_item = 100000 ##tbd
        self.rowsize_tablescan_item = 87
        self.rows_selection_item = 100000 ##tbd
        self.rows_tablescan_order_line = 1250435 ##tbd
        self.rowsize_tablescan_order_line = 65
        self.rows_selection_order_line = 1250435 ##tbd  


        self.columns = [
            ["ol_amount", "ol_i_id", "ol_quantity", "ol_w_id"],
            ["i_id", "i_price", "i_data"]
        ]

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

        
        self.columns = [
            ["s_name", "s_address", "s_suppkey", "s_nationkey"],
            ["n_nationkey", "n_name"],
            ["s_i_id", "s_w_id", "s_quantity"],
            ["ol_i_id", "ol_quantity", "ol_delivery_d"],
            ["i_id", "i_data"]
        ]

        self.keys = [[], [], [], ['ol_delivery_d'], []]  # filter keys
        self.tables = ["supplier", "nation", "stock", "order_line", "item"]
        self.values = [[], [], [], [datetime(2024, 10, 27, 17, 0, 0)], []] # filter values
        self.operators = [[], [], [], ['gt'], []] # filter operators '>'        

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

        
        self.columns = [
            ["s_name", "s_suppkey", "s_nationkey"],
            ["s_w_id", "s_i_id"],
            ["ol_i_id", "ol_w_id", "ol_o_id", "ol_d_id", "ol_delivery_d"],
            ["o_id", "o_entry_d"],
            ["n_nationkey", "n_name"]
        ]

        self.keys = [[], [], [], [], []]
        self.values = [[], [], [], [], []]
        self.tables = ["supplier", "stock", "order_line", "orders", "nation"]
        self.operators = [[], [], [], [], []]          

class Q22card(Qcard):
    def init(self):
        self.rows_tablescan_customer = 120000 ##tbd
        self.rowsize_tablescan_customer = 671
        self.rows_selection_customer = 120000 ##tbd     

        
        self.columns = [
            ["c_state", "c_phone", "c_balance", "c_id", "c_w_id", "c_d_id"],
            ["o_c_id", "o_w_id", "o_d_id"]
        ]

        self.keys = [['c_balance'], []]  # filter keys
        self.tables = ["customer", "orders"]
        self.values = [[0], []] # filter values
        self.operators = [['gt'], []] # filter operators '>'             


# 根据分区metadata, 获取每一个quert的查询基数
#def get_qcard(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta):
def get_qcard(table_meta, qcard_list, candidates):
    customer_meta = table_meta[0]
    district_meta = table_meta[1] 
    history_meta = table_meta[2] 
    item_meta = table_meta[3] 
    nation_meta = table_meta[4]
    new_order_meta = table_meta[5] 
    order_line_meta = table_meta[6] 
    orders_meta = table_meta[7]
    region_meta = table_meta[8] 
    stock_meta = table_meta[9]
    supplier_meta = table_meta[10] 
    warehouse_meta = table_meta[11]

    # get Qcard
    # qcard = []
    # q1card = Q1card()
    # q1card.init()
    #print("Query 1")
    qcard_list[0].get_query_card(table_meta, candidates)
    # qcard.append(q1card)

    # q2card = Q2card()
    # q2card.init()
    #print("Query 2")
    qcard_list[1].get_query_card(table_meta, candidates)
    # qcard.append(q2card)

    # q3card = Q3card()
    # q3card.init()
    #print("Query 3")
    qcard_list[2].get_query_card(table_meta, candidates)
    # qcard.append(q3card)

    # q4card = Q4card()
    # q4card.init()
    #print("Query 4")
    qcard_list[3].get_query_card(table_meta, candidates)
    # qcard.append(q4card)

    # q5card = Q5card()
    # q5card.init()
    #print("Query 5")
    qcard_list[4].get_query_card(table_meta, candidates)
    # qcard.append(q5card)

    # q6card = Q6card()
    # q6card.init()
    #print("Query 6")
    qcard_list[5].get_query_card(table_meta, candidates)
    # qcard.append(q6card)

    # q7card = Q7card()
    # q7card.init()
    #print("Query 7")
    qcard_list[6].get_query_card(table_meta, candidates)
    # qcard.append(q7card)

    # q8card = Q8card()
    # q8card.init()
    #print("Query 8")
    qcard_list[7].get_query_card(table_meta, candidates)
    # qcard.append(q8card)

    # q9card = Q9card()
    # q9card.init()
    #print("Query 9")
    qcard_list[8].get_query_card(table_meta, candidates)
    # qcard.append(q9card)

    # q10card = Q10card()
    # q10card.init()
    #print("Query 10")
    qcard_list[9].get_query_card(table_meta, candidates)
    # qcard.append(q10card)

    # q11card = Q11card()
    # q11card.init()
    #print("Query 11")
    qcard_list[10].get_query_card(table_meta, candidates)
    # qcard.append(q11card)

    # q12card = Q12card()
    # q12card.init()
    #print("Query 12")
    qcard_list[11].get_query_card(table_meta, candidates)
    # qcard.append(q12card)

    # q13card = Q13card()
    # q13card.init()
    #print("Query 13")
    qcard_list[12].get_query_card(table_meta, candidates)
    # qcard.append(q13card)

    # q14card = Q14card()
    # q14card.init()
    #print("Query 14")
    qcard_list[13].get_query_card(table_meta, candidates)
    # qcard.append(q14card)

    # q15card = Q15card()
    # q15card.init()
    #print("Query 15")
    qcard_list[14].get_query_card(table_meta, candidates)
    # qcard.append(q15card)

    # q16card = Q16card()
    # q16card.init()
    #print("Query 16")
    qcard_list[15].get_query_card(table_meta, candidates)
    # qcard.append(q16card)

    # q17card = Q17card()
    # q17card.init()
    #print("Query 17")
    qcard_list[16].get_query_card(table_meta, candidates)
    # qcard.append(q17card)

    # q18card = Q18card()
    # q18card.init()
    #print("Query 18")
    qcard_list[17].get_query_card(table_meta, candidates)
    # qcard.append(q18card)

    # q19card = Q19card()
    # q19card.init()
    #print("Query 19")
    qcard_list[18].get_query_card(table_meta, candidates)
    # qcard.append(q19card)

    # q20card = Q20card()
    # q20card.init()
    #print("Query 20")
    qcard_list[19].get_query_card(table_meta, candidates)
    # qcard.append(q20card)

    # q21card = Q21card()
    # q21card.init()
    #print("Query 21")
    qcard_list[20].get_query_card(table_meta, candidates)
    # qcard.append(q21card)

    # q22card = Q22card()
    # q22card.init()
    #print("Query 22")
    qcard_list[21].get_query_card(table_meta, candidates)
    # qcard.append(q22card)

    # return qcard

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