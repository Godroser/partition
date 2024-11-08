import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

import random
import string


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



# tiup自动生成的负载里
# warehouse_id: 1-4
# district_id: 1-10
# customer_id: 1-3000
# stock_id: 1-100000
# orders_id: 1-3223
# order_line_id: 1-3233
# new_order_id: 2260-3223
# item_id: 1-100000
# nation_id: 0-24
# supplier_id: 1-10000
# region_id: 0-4
# 日期相关的目前范围是2024-10-23 17:03:47

class Workload_Parameter:
  def __init__(self):
    self.max_w_id = 4
    self.max_d_id = 10
    self.max_c_id = 3000
    self.max_stock_id = 100000
    self.max_o_id = 3223
    self.max_item = 100000
    self.max_nation = 24
    self.max_supplier_id = 10000
    self.max_region_id = 4
    self.max_ol_cnt = 15 # max item cnt in an order
    self.max_ol_quantity = 10 #max item quatity in an order

    self.local_new_order_ratio = 0.9 #default 0.9
    self.lcoal_payment_ratio = 0.85 #default 0.85


class Workload_Genrator:
  def __init__(self, ratio, warehouse_number, total_number_txn):
    self.ratio = ratio  #tp-ap ratio
    self.warehouse_number = warehouse_number  
    self.total_number_txn = total_number_txn  #total number of transaction/query


  def generate_new_order():
    
    wl_param = Workload_Parameter()

    # New-Order
    d_id = random.randint(1, wl_param.max_d_id)
    w_id = random.randint(1, wl_param.max_w_id)
    c_id = random.randint(1, wl_param.max_c_id)
    o_ol_cnt = random.randint(1,wl_param.o_ol_cnt)  # order item count
    ol_i_id = [0] * o_ol_cnt  # item id
    ol_supply_w_id = [0] * o_ol_cnt  # supply warehouse id
    ol_quantity = [0] * o_ol_cnt  # item quantity
    o_all_local = wl_param.local_new_order_ratio


    for i in range(o_ol_cnt):
      ol_i_id[i] = random.randint(1, wl_param.max_item)


    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            cur.execute("start transaction;")

            sql_new_order1 = """
              SET @d_next_o_id :=
              SELECT d_next_o_id 
              FROM district 
              WHERE d_id = {} AND d_w_id = {};
            """.format(d_id, w_id)

            sql_new_order2=""""
              UPDATE district 
              SET d_next_o_id = d_next_o_id + 1 
              WHERE d_id = {} AND d_w_id = {};
            """.format(d_id, w_id)

            sql_new_order3="""
              INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
              VALUES (@d_next_o_id, {}, {}, {}, NOW(), NULL, {}, 1);
            """.format(d_id, w_id, c_id, o_ol_cnt)

            sql_new_order4="""
              INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
              VALUES (@d_next_o_id, {}, {});
            """.format(d_id, w_id)

            cur.execute(sql_new_order1)        
            cur.execute(sql_new_order2) 
            cur.execute(sql_new_order3) 
            cur.execute(sql_new_order4)

            for i in range(o_ol_cnt):
              sql_new_order5 = """
                # SET @s_quantity, @s_data, @s_ytd, @s_order_cnt, @s_remote_cn:=
                # SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt
                # FROM stock
                # WHERE s_w_id = {} AND s_i_id = {};         
                SET @s_quantity = (SELECT s_quantity FROM stock WHERE s_w_id = {} AND s_i_id = {});
              """.format(ol_supply_w_id[i], ol_i_id[i])

              cur.execute(sql_new_order5)

              sql_new_order6 = "select @s_quantity;"

              cur.execute(sql_new_order6)
              s_quantity = cur.fetchall()[0][0]


              if s_quantity >= ol_quantity[i]:
                s_quantity = s_quantity - ol_quantity[i]
              else:
                s_quantity = s_quantity + 100 - ol_quantity[i]

              if random.random() < o_all_local: ## local warehouse
                sql_new_order7 = """
                  UPDATE stock
                  SET s_quantity = {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1
                  WHERE s_w_id = {} AND s_i_id = {};
                """.format(s_quantity, ol_quantity[i], ol_supply_w_id[i], ol_i_id[i])
              else:
                sql_new_order7 = """
                  UPDATE stock
                  SET s_quantity = {}, s_ytd = s_ytd + {}, s_order_cnt = s_order_cnt + 1, s_remote_cnt = s_remote_cnt + 1
                  WHERE s_w_id = {} AND s_i_id = {};
                """.format(s_quantity, ol_quantity[i], ol_supply_w_id[i], ol_i_id[i])

              cur.execute(sql_new_order7)

              sql_new_order8 = """
                SET @i_price =
                SELECT i_price
                FROM item
                WHERE i_id = {};              
              """.format(ol_i_id)
              cur.execute(sql_new_order8)
              
              sql_new_order8 = "select @i_price;"
              i_price = cur.fetchall()[0][0]


              sql_new_order9 = """
              INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
              VALUES (@d_next_o_id, {}, {}, {}, {}, {}, NULL, {}, {}, {});
              """.format(d_id, w_id, i+1, ol_i_id[i], ol_supply_w_id[i], ol_quantity[i], i_price * ol_quantity[i], ''.join(random.choices(string.ascii_uppercase, k=24)))

              cur.execute(sql_new_order9)

              cur.execute("COMMIT;")



-- 假设事务输入的参数
-- w_id: 仓库ID, d_id: 区域ID, c_id: 客户ID
-- o_ol_cnt: 订单中商品项的数量, o_all_local: 是否全部商品来自本地
-- item_data: 包含每个商品项的数据，包括 ol_i_id（商品ID）、 ol_supply_w_id（供应仓库ID）、 ol_quantity（商品数量）

START TRANSACTION;

-- Step 1: 获取仓库税率
SELECT w_tax INTO @w_tax FROM warehouse WHERE w_id = w_id;

-- Step 2: 获取地区税率和下一个订单ID
SELECT d_tax, d_next_o_id
INTO @d_tax, @d_next_o_id
FROM district
WHERE d_w_id = w_id AND d_id = d_id;

-- 更新下一个订单ID
UPDATE district
SET d_next_o_id = d_next_o_id + 1
WHERE d_w_id = w_id AND d_id = d_id;

-- Step 3: 获取客户折扣和信用信息
SELECT c_discount, c_credit, c_last
INTO @c_discount, @c_credit, @c_last
FROM customer
WHERE c_w_id = w_id AND c_d_id = d_id AND c_id = c_id;

-- Step 4: 插入订单
INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
VALUES (@d_next_o_id, d_id, w_id, c_id, NOW(), NULL, o_ol_cnt, o_all_local);

-- Step 5: 插入新订单标识
INSERT INTO new_order (no_o_id, no_d_id, no_w_id)
VALUES (@d_next_o_id, d_id, w_id);

-- Step 6: 为每个订单项处理商品库存、价格，并插入订单项记录
-- 遍历每个商品项
SET @total_amount = 0;

-- 伪代码形式，实际需要在应用层遍历 item_data 并依次执行以下语句
FOR EACH item IN item_data DO
    SET @ol_number = item.ol_number;
    SET @ol_i_id = item.ol_i_id;
    SET @ol_supply_w_id = item.ol_supply_w_id;
    SET @ol_quantity = item.ol_quantity;

    -- 检查商品信息
    SELECT i_price, i_name, i_data
    INTO @i_price, @i_name, @i_data
    FROM item
    WHERE i_id = @ol_i_id;

    -- 获取库存信息
    SELECT s_quantity, s_data, s_ytd, s_order_cnt, s_remote_cnt
    INTO @s_quantity, @s_data, @s_ytd, @s_order_cnt, @s_remote_cnt
    FROM stock
    WHERE s_w_id = @ol_supply_w_id AND s_i_id = @ol_i_id;

    -- 更新库存数量
    IF @s_quantity > @ol_quantity THEN
        SET @s_quantity = @s_quantity - @ol_quantity;
    ELSE
        SET @s_quantity = @s_quantity + 91 - @ol_quantity;
    END IF;

    -- 更新 stock 表
    UPDATE stock
    SET s_quantity = @s_quantity,
        s_ytd = s_ytd + @ol_quantity,
        s_order_cnt = s_order_cnt + 1,
        s_remote_cnt = IF(@ol_supply_w_id != w_id, s_remote_cnt + 1, s_remote_cnt)
    WHERE s_w_id = @ol_supply_w_id AND s_i_id = @ol_i_id;

    -- 计算订单项金额
    SET @ol_amount = @ol_quantity * @i_price;
    SET @total_amount = @total_amount + @ol_amount;

    -- 插入订单项记录
    INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
    VALUES (@d_next_o_id, d_id, w_id, @ol_number, @ol_i_id, @ol_supply_w_id, NULL, @ol_quantity, @ol_amount, SUBSTRING(s_data, 1, 24));
END FOR;

-- Step 7: 计算总金额并应用折扣
SET @total_amount = @total_amount * (1 - @c_discount) * (1 + @w_tax + @d_tax);

COMMIT;



"""
-- 获取区域信息
SELECT d_next_o_id 
FROM district 
WHERE d_id = ? AND d_w_id = ?;

-- 更新区域的下一个订单 ID
UPDATE district 
SET d_next_o_id = d_next_o_id + 1 
WHERE d_id = ? AND d_w_id = ?;

-- 插入订单信息
INSERT INTO orders (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local)
VALUES (?, ?, ?, ?, ?, ?, ?, ?);

-- 插入订单行信息（循环执行）
INSERT INTO order_line (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);

-- 减少库存
UPDATE stock 
SET s_quantity = s_quantity - ?
WHERE s_i_id = ? AND s_w_id = ?;
"""

  def generate_payment():
  
  def generate_order_status():
  
  def generate_delivery():
  
  def genearte_stock_level():



  def generate_tp():
    # Default
    # New-Order: 45%
    # Payment: 43%
    # Order-Status: 4%
    # Delivery: 4%
    # Stock-Level: 4%    
    ratio_tp = [0.45, 0.43, 0.04, 0.04, 0.04]   

    seed = random.randint(1,100)
    if seed <= 45:
      generate_new_order()
    else if seed <= 88:
      generate_payment()
    else if seed <= 92:
      generate_order_status()
    else if seed <= 96:
      generate_delivery()
    else if seed <= 100:
      generate_stock_level()
    




  def generate_ap():


  def generate_workload():

