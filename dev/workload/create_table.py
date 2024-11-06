import sys
import os

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config



def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": config.TIDB_DB_NAME,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True
    }

    if config.ca_path:
        db_conf["ssl_verify_cert"] = True
        db_conf["ssl_verify_identity"] = True
        db_conf["ssl_ca"] = config.ca_path
    return mysql.connector.connect(**db_conf)

def create_warehouse_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'warehouse';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table warehouse Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `warehouse` (
                `w_id` int(11) NOT NULL,
                `w_name` varchar(10) DEFAULT NULL,
                `w_street_1` varchar(20) DEFAULT NULL,
                `w_street_2` varchar(20) DEFAULT NULL,
                `w_city` varchar(20) DEFAULT NULL,
                `w_state` char(2) DEFAULT NULL,
                `w_zip` char(9) DEFAULT NULL,
                `w_tax` decimal(4,4) DEFAULT NULL,
                `w_ytd` decimal(12,2) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table warehouse Created!")


def create_district_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'district';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table distinct Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `district` (
                `d_id` int(11) NOT NULL,
                `d_w_id` int(11) NOT NULL,
                `d_name` varchar(10) DEFAULT NULL,
                `d_street_1` varchar(20) DEFAULT NULL,
                `d_street_2` varchar(20) DEFAULT NULL,
                `d_city` varchar(20) DEFAULT NULL,
                `d_state` char(2) DEFAULT NULL,
                `d_zip` char(9) DEFAULT NULL,
                `d_tax` decimal(4,4) DEFAULT NULL,
                `d_ytd` decimal(12,2) DEFAULT NULL,
                `d_next_o_id` int(11) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table district Created!")

def create_supplier_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'supplier';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table supplier Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `supplier` (
                `S_SUPPKEY` bigint(20) NOT NULL,
                `S_NAME` char(25) NOT NULL,
                `S_ADDRESS` varchar(40) NOT NULL,
                `S_NATIONKEY` bigint(20) NOT NULL,
                `S_PHONE` char(15) NOT NULL,
                `S_ACCTBAL` decimal(15,2) NOT NULL,
                `S_COMMENT` varchar(101) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table supplier Created!")


def create_customer_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'customer';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table customer Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `customer` (
                `c_id` int(11) NOT NULL,
                `c_d_id` int(11) NOT NULL,
                `c_w_id` int(11) NOT NULL,
                `c_first` varchar(16) DEFAULT NULL,
                `c_middle` char(2) DEFAULT NULL,
                `c_last` varchar(16) DEFAULT NULL,
                `c_street_1` varchar(20) DEFAULT NULL,
                `c_street_2` varchar(20) DEFAULT NULL,
                `c_city` varchar(20) DEFAULT NULL,
                `c_state` char(2) DEFAULT NULL,
                `c_zip` char(9) DEFAULT NULL,
                `c_phone` char(16) DEFAULT NULL,
                `c_since` datetime DEFAULT NULL,
                `c_credit` char(2) DEFAULT NULL,
                `c_credit_lim` decimal(12,2) DEFAULT NULL,
                `c_discount` decimal(4,4) DEFAULT NULL,
                `c_balance` decimal(12,2) DEFAULT NULL,
                `c_ytd_payment` decimal(12,2) DEFAULT NULL,
                `c_payment_cnt` int(11) DEFAULT NULL,
                `c_delivery_cnt` int(11) DEFAULT NULL,
                `c_data` varchar(500) DEFAULT NULL,
                KEY `idx_customer` (`c_w_id`,`c_d_id`,`c_last`,`c_first`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table customer Created!")

def create_history_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'history';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table history Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `history` (
                `h_c_id` int(11) NOT NULL,
                `h_c_d_id` int(11) NOT NULL,
                `h_c_w_id` int(11) NOT NULL,
                `h_d_id` int(11) NOT NULL,
                `h_w_id` int(11) NOT NULL,
                `h_date` datetime DEFAULT NULL,
                `h_amount` decimal(6,2) DEFAULT NULL,
                `h_data` varchar(24) DEFAULT NULL,
                KEY `idx_h_w_id` (`h_w_id`),
                KEY `idx_h_c_w_id` (`h_c_w_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table history Created!")


def create_orders_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'orders';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table orders Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `orders` (
                `o_id` int(11) NOT NULL,
                `o_d_id` int(11) NOT NULL,
                `o_w_id` int(11) NOT NULL,
                `o_c_id` int(11) DEFAULT NULL,
                `o_entry_d` datetime DEFAULT NULL,
                `o_carrier_id` int(11) DEFAULT NULL,
                `o_ol_cnt` int(11) DEFAULT NULL,
                `o_all_local` int(11) DEFAULT NULL,
                KEY `idx_order` (`o_w_id`,`o_d_id`,`o_c_id`,`o_id`)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                """
            )
            print("Table orders Created!")

def create_new_order_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'new_order';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table new_order Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `new_order` (
                `no_o_id` int(11) NOT NULL,
                `no_d_id` int(11) NOT NULL,
                `no_w_id` int(11) NOT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                """
            )
            print("Table new_order Created!")




def create_orderline_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'order_line';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table order_line Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `order_line` (
                `ol_o_id` int(11) NOT NULL,
                `ol_d_id` int(11) NOT NULL,
                `ol_w_id` int(11) NOT NULL,
                `ol_number` int(11) NOT NULL,
                `ol_i_id` int(11) NOT NULL,
                `ol_supply_w_id` int(11) DEFAULT NULL,
                `ol_delivery_d` datetime DEFAULT NULL,
                `ol_quantity` int(11) DEFAULT NULL,
                `ol_amount` decimal(6,2) DEFAULT NULL,
                `ol_dist_info` char(24) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table order_line Created!")


def create_stock_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'stock';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table stock Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `stock` (
                `s_i_id` int(11) NOT NULL,
                `s_w_id` int(11) NOT NULL,
                `s_quantity` int(11) DEFAULT NULL,
                `s_dist_01` char(24) DEFAULT NULL,
                `s_dist_02` char(24) DEFAULT NULL,
                `s_dist_03` char(24) DEFAULT NULL,
                `s_dist_04` char(24) DEFAULT NULL,
                `s_dist_05` char(24) DEFAULT NULL,
                `s_dist_06` char(24) DEFAULT NULL,
                `s_dist_07` char(24) DEFAULT NULL,
                `s_dist_08` char(24) DEFAULT NULL,
                `s_dist_09` char(24) DEFAULT NULL,
                `s_dist_10` char(24) DEFAULT NULL,
                `s_ytd` int(11) DEFAULT NULL,
                `s_order_cnt` int(11) DEFAULT NULL,
                `s_remote_cnt` int(11) DEFAULT NULL,
                `s_data` varchar(50) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                """
            )
            print("Table stock Created!")


def create_item_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'item';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table item Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `item` (
                `i_id` int(11) NOT NULL,
                `i_im_id` int(11) DEFAULT NULL,
                `i_name` varchar(24) DEFAULT NULL,
                `i_price` decimal(5,2) DEFAULT NULL,
                `i_data` varchar(50) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table item Created!")

            
def create_nation_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'nation';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table nation Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `nation` (
                `N_NATIONKEY` bigint(20) NOT NULL,
                `N_NAME` char(25) NOT NULL,
                `N_REGIONKEY` bigint(20) NOT NULL,
                `N_COMMENT` varchar(152) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
                """
            )
            print("Table nation Created!")

def create_region_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'region';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Table region Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE `region` (
                `R_REGIONKEY` bigint(20) NOT NULL,
                `R_NAME` char(25) NOT NULL,
                `R_COMMENT` varchar(152) DEFAULT NULL
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;
                """
            )
            print("Table region Created!")



if __name__ == "__main__":
    create_warehouse_table()
    create_customer_table()
    create_district_table()
    create_history_table()
    create_item_table()
    create_nation_table()
    create_new_order_table()
    create_orderline_table()
    create_orders_table()
    create_region_table()
    create_stock_table()
    create_supplier_table() 