import sys
import os

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from mysql.connector import pooling
from config import *

# def get_connection(autocommit: bool = True) -> MySQLConnection:
#     config = Config()
#     db_conf = {
#         "host": config.TIDB_HOST,
#         "port": config.TIDB_PORT,
#         "user": config.TIDB_USER,
#         "password": config.TIDB_PASSWORD,
#         "database": config.TIDB_DB_NAME,
#         "autocommit": autocommit,
#         # mysql-connector-python will use C extension by default,
#         # to make this example work on all platforms more easily,
#         # we choose to use pure python implementation.
#         "use_pure": True
#     }

#     if config.ca_path:
#         db_conf["ssl_verify_cert"] = True
#         db_conf["ssl_verify_identity"] = True
#         db_conf["ssl_ca"] = config.ca_path
#     return mysql.connector.connect(**db_conf)
#     #return mysql.connector.pooling.MySQLConnectionPool(pool_name="mypool", pool_size=10, **db_conf).get_connection()

class Customer_columns:
    def __init__(self):
        self.name = "customer"
        self.columns = ["c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_street_1", "c_street_2", "c_city", "c_state", "c_zip", "c_phone", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_delivery_cnt", "c_data"]
        self.columns_size =  [4, 4, 4, 33, 4, 33, 41, 41, 41, 4, 18, 32, 8, 4, 6, 3, 6, 6, 4, 4, 1002]
        self.primary_keys = ["c_id", "c_d_id", "c_w_id"]
        self.partitionable_columns = ["c_id", "c_d_id", "c_w_id",  "c_payment_cnt"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM customer;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class District_columns:
    def __init__(self):
        self.name = "district"
        self.columns = ["d_id", "d_w_id", "d_name", "d_street_1", "d_street_2", "d_city", "d_state", "d_zip", "d_tax", "d_ytd", "d_next_o_id"]
        self.columns_size =  [4, 4, 21, 41, 41, 41, 4, 18, 3, 6, 4]
        self.primary_keys = ["d_id", "d_w_id"]
        self.partitionable_columns = ["d_id", "d_w_id", "d_next_o_id"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM district;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Item_columns:
    def __init__(self):
        self.name = "item"
        self.columns = ["i_id", "i_im_id", "i_name", "i_price", "i_data"]
        self.columns_size =  [4, 4, 49, 3, 101]
        self.primary_keys = ["i_id"]
        self.partitionable_columns = ["i_id", "i_im_id"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM item;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class New_order_columns:
    def __init__(self):
        self.name = "new_order"
        self.columns = ["no_o_id", "no_d_id", "no_w_id"]
        self.columns_size =  [4, 4, 4]
        self.primary_keys = ["no_o_id", "no_d_id", "no_w_id"]
        self.partitionable_columns = ["no_o_id", "no_d_id", "no_w_id"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM new_order;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Orders_columns:
    def __init__(self):
        self.name = "orders"
        self.columns = ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"]
        self.partitionable_columns = ["o_id", "o_d_id", "o_w_id", "o_c_id", "o_entry_d", "o_carrier_id", "o_ol_cnt", "o_all_local"]
        self.primary_keys = ["o_id"]
        self.columns_size =  [4, 4, 4, 4, 8, 4, 4, 4]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM orders;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Order_line_columns:
    def __init__(self):
        self.name = "order_line"
        self.columns = ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_delivery_d", "ol_quantity", "ol_amount", "ol_dist_info"]
        self.columns_size =  [4, 4, 4, 4, 4, 4, 8, 4, 3, 96]
        self.primary_keys = ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number"]
        self.partitionable_columns = ["ol_o_id", "ol_d_id", "ol_w_id", "ol_number", "ol_i_id", "ol_supply_w_id", "ol_quantity", "ol_delivery_d"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM order_line;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Stock_columns:
    def __init__(self):
        self.name = "stock"
        self.columns = ["s_i_id", "s_w_id", "s_quantity", "s_dist_01", "s_dist_02", "s_dist_03", "s_dist_04", "s_dist_05", "s_dist_06", "s_dist_07", "s_dist_08", "s_dist_09", "s_dist_10", "s_ytd", "s_order_cnt", "s_remote_cnt", "s_data"]
        self.columns_size =  [4, 4, 4, 96, 96, 96, 96, 96, 96, 96, 96, 96, 96, 4, 4, 4, 101]
        self.primary_keys = ["s_i_id", "s_w_id"]
        self.partitionable_columns = ["s_i_id", "s_w_id", "s_ytd", "s_order_cnt", "s_remote_cnt"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM stock;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Warehouse_columns:
    def __init__(self):
        self.name = "warehouse"
        self.columns = ["w_id", "w_name", "w_street_1", "w_street_2", "w_city", "w_state", "w_zip", "w_tax", "w_ytd"]
        self.columns_size =  [4, 21, 41, 41, 41, 8, 36, 3, 6]
        self.primary_keys = ["w_id"]
        self.partitionable_columns = ["w_id"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM warehouse;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges    
    
class History_columns:
    def __init__(self):
        self.name = "history"
        self.columns = ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date", "h_amount", "h_data"]
        self.columns_size =  [4, 4, 4, 4, 4, 8, 3, 49]
        self.primary_keys = ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id"]
        self.partitionable_columns = ["h_c_id", "h_c_d_id", "h_c_w_id", "h_d_id", "h_w_id", "h_date"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM history;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Nation_columns:
    def __init__(self):
        self.name = "nation"
        self.columns = ["n_nationkey", "n_name", "n_regionkey", "n_comment"]
        self.columns_size =  [8, 100, 8, 305]
        self.primary_keys = ["n_nationkey"]
        self.partitionable_columns = ["n_nationkey", "n_regionkey"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM nation;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges
    
class Supplier_columns:
    def __init__(self):
        self.name = "supplier"
        self.columns = ["s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"]
        self.columns_size =  [8, 100, 81, 8, 60, 7, 203]
        self.primary_keys = ["s_suppkey"]
        self.partitionable_columns = ["s_suppkey", "s_nationkey"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM supplier;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges

class Region_columns:
    def __init__(self):
        self.name = "region"
        self.columns = ["r_regionkey", "r_name", "r_comment"]
        self.columns_size =  [8, 100, 305]
        self.primary_keys = ["r_regionkey"]
        self.partitionable_columns = ["r_regionkey"]
        self.partition_keys = []
        self.replicas = []
        self.replica_partition_keys = []

    def get_keys_ranges(self, keys):
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:   
                ranges = []
                for key in keys:
                    cur.execute(f"SELECT MIN({key}), MAX({key}) FROM region;")
                    min_val, max_val = cur.fetchone()
                    ranges.append([min_val, max_val])
        return ranges   

if __name__ == "__main__":
    customer_columns = Customer_columns()
    district_columns = District_columns()
    item_columns = Item_columns()
    new_order_columns = New_order_columns()
    orders_columns = Orders_columns()
    order_line_columns = Order_line_columns()
    stock_columns = Stock_columns()
    warehouse_columns = Warehouse_columns()
    history_columns = History_columns()
    nation_columns = Nation_columns()
    supplie_columnsr = Supplier_columns()
    region_columns = Region_columns()

