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
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Warehouse';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Warehouse Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE Warehouse (
                    W_ID INT PRIMARY KEY,
                    W_NAME VARCHAR(10),
                    W_STREET_1 VARCHAR(20),
                    W_STREET_2 VARCHAR(20),
                    W_CITY VARCHAR(20),
                    W_STATE CHAR(2),
                    W_ZIP CHAR(9),
                    W_TAX DECIMAL(4,4),
                    W_YTD DECIMAL(12,2)
                );
                """
            )
            print("Warehouse Created!")


def create_distinct_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Distinct';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Distinct Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE District (
                    D_ID INT PRIMARY KEY,
                    D_W_ID INT,
                    D_NAME VARCHAR(10),
                    D_STREET_1 VARCHAR(20),
                    D_STREET_2 VARCHAR(20),
                    D_CITY VARCHAR(20),
                    D_STATE CHAR(2),
                    D_ZIP CHAR(9),
                    D_TAX DECIMAL(4,4),
                    D_YTD DECIMAL(12,2),
                    D_NEXT_O_ID INT,
                    FOREIGN KEY (D_W_ID) REFERENCES Warehouse(W_ID)
                );
                """
            )
            print("Distinct Created!")




def create_customer_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Customer';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Customer Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE Customer (
                    C_ID INT PRIMARY KEY,
                    C_D_ID INT,
                    C_W_ID INT,
                    C_FIRST VARCHAR(16),
                    C_MIDDLE CHAR(2),
                    C_LAST VARCHAR(16),
                    C_STREET_1 VARCHAR(20),
                    C_STREET_2 VARCHAR(20),
                    C_CITY VARCHAR(20),
                    C_STATE CHAR(2),
                    C_ZIP CHAR(9),
                    C_PHONE CHAR(16),
                    C_SINCE TIMESTAMP,
                    C_CREDIT CHAR(2),
                    C_CREDIT_LIM DECIMAL(12,2),
                    C_DISCOUNT DECIMAL(4,4),
                    C_BALANCE DECIMAL(12,2),
                    C_YTD_PAYMENT DECIMAL(12,2),
                    C_PAYMENT_CNT INT,
                    C_DELIVERY_CNT INT,
                    FOREIGN KEY (C_D_ID, C_W_ID) REFERENCES District(D_ID, D_W_ID)
                );
                """
            )
            print("Customer Created!")

def create_history_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'History';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("History Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE History (
                    H_C_ID INT,
                    H_C_D_ID INT,
                    H_C_W_ID INT,
                    H_D_ID INT,
                    H_W_ID INT,
                    H_DATE TIMESTAMP,
                    H_AMOUNT DECIMAL(6,2),
                    H_DATA VARCHAR(24),
                    PRIMARY KEY (H_C_ID, H_C_D_ID, H_C_W_ID, H_D_ID, H_W_ID)
                );
                """
            )
            print("History Created!")


def create_orders_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Orders';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Orders Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE Orders (
                    O_ID INT PRIMARY KEY,
                    O_C_ID INT,
                    O_D_ID INT,
                    O_W_ID INT,
                    O_ENTRY_D TIMESTAMP,
                    O_CARRIER_ID INT,
                    O_OL_CNT INT,
                    O_ALL_LOCAL INT,
                    FOREIGN KEY (O_C_ID, O_D_ID, O_W_ID) REFERENCES Customer(C_ID, C_D_ID, C_W_ID)
                );
                """
            )
            print("Orders Created!")


def create_orderline_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'OrderLine';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("OrderLine Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE OrderLine (
                    OL_O_ID INT,
                    OL_D_ID INT,
                    OL_W_ID INT,
                    OL_NUMBER INT,
                    OL_I_ID INT,
                    OL_SUPPLY_W_ID INT,
                    OL_DELIVERY_D TIMESTAMP,
                    OL_QUANTITY DECIMAL(2,0),
                    OL_AMOUNT DECIMAL(6,2),
                    OL_DIST_INFO VARCHAR(24),
                    PRIMARY KEY (OL_O_ID, OL_D_ID, OL_W_ID, OL_NUMBER),
                    FOREIGN KEY (OL_O_ID, OL_D_ID, OL_W_ID) REFERENCES Orders(O_ID, O_D_ID, O_W_ID)
                );
                """
            )
            print("OrderLine Created!")


def create_stock_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Stock';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Stock Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE Stock (
                    S_I_ID INT,
                    S_W_ID INT,
                    S_QUANTITY DECIMAL(4,0),
                    S_DIST_01 VARCHAR(24),
                    S_DIST_02 VARCHAR(24),
                    S_DIST_03 VARCHAR(24),
                    S_DIST_04 VARCHAR(24),
                    S_DIST_05 VARCHAR(24),
                    S_DIST_06 VARCHAR(24),
                    S_DIST_07 VARCHAR(24),
                    S_DIST_08 VARCHAR(24),
                    S_DIST_09 VARCHAR(24),
                    S_DIST_10 VARCHAR(24),
                    S_YTD DECIMAL(8,0),
                    S_ORDER_CNT INT,
                    S_REMOTE_CNT INT,
                    S_DATA VARCHAR(50),
                    PRIMARY KEY (S_I_ID, S_W_ID),
                    FOREIGN KEY (S_W_ID) REFERENCES Warehouse(W_ID)
                );
                """
            )
            print("Stock Created!")


def create_item_table():
  config = Config()
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:         
          cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'Item';".format(config.TIDB_DB_NAME))

          if len(cur.fetchall()) > 0:
              print("Item Exists. Exiting...")
          else:
            cur.execute(
                """
                CREATE TABLE Item (
                    I_ID INT PRIMARY KEY,
                    I_IM_ID INT,
                    I_NAME VARCHAR(24),
                    I_PRICE DECIMAL(5,2),
                    I_DATA VARCHAR(50)
                );
                """
            )
            print("Item Created!")

            
