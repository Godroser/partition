import math
import sys
import os
from datetime import datetime, timedelta

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mcts
from estimator.operators import *
from estimator.ch_query_params import *
from estimator.ch_partition_meta import *
from estimator.ch_query_card import *
from estimator.ch_query_cost import *
from estimator.ch_table_meta import *
from config import Config

# update metadata given the partition and replica candidate
# candidate format:{'name': , 'columns':, 'partitionable_columns': , 'partition_keys': [], 'replicas': [], 'replica_partition_keys': []}
def update_meta(table_columns, table_meta, candidates):
    table_dict = {'customer': 0, 'district': 1, 'history': 2, 'item': 3, 'nation': 4, 'new_order': 5, 'order_line': 6, 'orders': 7, 'region': 8, 'stock': 9, 'supplier': 10, 'warehouse': 11}

    for candidate in candidates:
        # get tables key min and max value
        table_name = candidate['name']
        idx = table_dict.get(table_name)
        minmaxvalues = table_columns[idx].get_key_ranges(candidate['partiton_keys'])  # minmaxvalue[0]: min value, minmaxvalue[1]: max value

        # get tables keys ranges 支持多个列
        table_ranges = []
        for minmaxvalue in minmaxvalues:
          if isinstance(minmaxvalue[0], int) and isinstance(minmaxvalue[1], int):
              # 对于整型，直接均匀分成四份
              step = (minmaxvalue[1] - minmaxvalue[0]) // 4
              table_ranges.append(minmaxvalue[0] + i * step for i in range(4))
          elif isinstance(minmaxvalue[0], datetime) and isinstance(minmaxvalue[1], datetime):
              # 对于datetime类型，根据天均匀分成四份
              total_days = (minmaxvalue[1] - minmaxvalue[0]).days
              step = total_days // 4
              table_ranges.append(minmaxvalue[0] + timedelta(days=i * step) for i in range(4))
          else:
              raise ValueError(f"Unsupported type for partition keys")

        # update partition metadata
        table_meta[idx].update_partition_metadata(candidate['partition_keys'], table_ranges)



if __name__ == "__main__":
    # 1. 初始化表参数
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
    # append的顺序要求一致
    table_meta.append(customer_meta, district_meta, history_meta, item_meta, nation_meta, new_order_meta, order_line_meta, orders_meta, region_meta, stock_meta, supplier_meta, warehouse_meta) 

    # 2. 初始化表的列参数
    table_columns = []
    customer_columns = Customer_columns()
    district_columns = District_columns()
    history_columns = History_columns()    
    item_columns = Item_columns()
    nation_columns = Nation_columns()
    new_order_columns = New_order_columns()
    order_line_columns = Order_line_columns()    
    orders_columns = Orders_columns()
    region_columns = Region_columns()
    stock_columns = Stock_columns()
    supplier_columns = Supplier_columns()
    warehouse_columns = Warehouse_columns()
    table_columns.append(customer_columns, district_columns, history_columns, item_columns, nation_columns, new_order_columns, order_line_columns, orders_columns, region_columns, stock_columns, supplier_columns, warehouse_columns)

    # 3. 根据分区副本情况更新元数据
    candidates = [{'name': 'customer', 'partition_keys': ['c_id', 'c_w_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}, {'name': 'order_line', 'partition_keys': ['ol_i_id'], 'replicas': ['col3'], 'replica_partition_keys': ['col3']}]

    update_meta(table_columns, table_meta, candidates)

    # get Qcard
    qcard_list = get_qcard()

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