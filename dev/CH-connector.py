import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

import random


# TIDB_HOST='127.0.0.1'
# TIDB_PORT='4000'
# TIDB_USER='root'
# TIDB_PASSWORD=''
# TIDB_DB_NAME='tpch'
# ca_path = ''



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


def create_orderline_range_partition(key):
  with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur:
          # ##cur.execute("DROP TABLE IF EXISTS players;")
          # print(                """
          #     ALTER TABLE order_line PARTITION BY RANGE(DAY({}))
          #     (PARTITION ol_p0 VALUES LESS THAN (1),
          #     PARTITION ol_p1 VALUES LESS THAN (24),
          #     PARTITION ol_p2 VALUES LESS THAN (26),
          #     PARTITION ol_p3 VALUES LESS THAN (MAXVALUE));
          #     """.format(key))
          # ##ol_delivery_d 最小2024-11-01 15:12:06 最大2024-11-01 15:15:05
          cur.execute(
              """
              CREATE TABLE order_line (
                  ol_o_id INT(11) NOT NULL,                  #-- 订单 ID
                  ol_d_id INT(11) NOT NULL,                  #-- 分店 ID
                  ol_w_id INT(11) NOT NULL,                  #-- 仓库 ID
                  ol_number INT(11) NOT NULL,                #-- 订单行号
                  ol_i_id INT(11) NOT NULL,                  #-- 商品 ID
                  ol_supply_w_id INT(11) DEFAULT NULL,       #-- 供应商仓库 ID
                  ol_delivery_d DATETIME NOT NULL,       #-- 交付日期
                  ol_quantity INT(11) DEFAULT NULL,          #-- 数量
                  ol_amount DECIMAL(6,2) DEFAULT NULL,       #-- 金额
                  ol_dist_info CHAR(24) DEFAULT NULL,        #-- 配送信息
                  
                  PRIMARY KEY (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_delivery_d)  #-- 复合主键，包含 `ol_o_id`, `ol_d_id`, `ol_w_id`, `ol_number`
              )
              PARTITION BY RANGE (DAY(ol_delivery_d))
              (
                  PARTITION ol_p0 VALUES LESS THAN (2),
                  PARTITION ol_p1 VALUES LESS THAN (24),
                  PARTITION ol_p2 VALUES LESS THAN (26),
                  PARTITION ol_p3 VALUES LESS THAN (MAXVALUE)
              );
              """.format(key)
            )  





def partition_orderline_range(key) -> None:
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            ##cur.execute("DROP TABLE IF EXISTS players;")
            print(                """
                ALTER TABLE order_line PARTITION BY RANGE(DAY({}))
                (PARTITION ol_p0 VALUES LESS THAN (1),
                PARTITION ol_p1 VALUES LESS THAN (24),
                PARTITION ol_p2 VALUES LESS THAN (26),
                PARTITION ol_p3 VALUES LESS THAN (MAXVALUE));
                """.format(key))
            ##ol_delivery_d 最小2024-11-01 15:12:06 最大2024-11-01 15:15:05
            cur.execute(
                """
                ALTER TABLE order_line PARTITION BY RANGE(DAY({}))
                (PARTITION ol_p0 VALUES LESS THAN (1),
                PARTITION ol_p1 VALUES LESS THAN (24),
                PARTITION ol_p2 VALUES LESS THAN (26),
                PARTITION ol_p3 VALUES LESS THAN (MAXVALUE));
                """.format(key)
            )

def partition_orderline_hash(key) -> None:
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            ##cur.execute("DROP TABLE IF EXISTS players;")
            if key == 'ol_delivery_d':
              cur.execute(
                  """
                  ALTER TABLE order_line PARTITION BY HASH(DAY(ol_delivery_d)) PARTITIONS 4;
                  """
              )              
            else:
              cur.execute(
                  """
                  ALTER TABLE order_line PARTITION BY HASH('{}') PARTITIONS 4;
                  """.format(key)
              )


def create_table_range():
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS players;")
            cur.execute(
                """
                CREATE TABLE players (
                    `id` INTEGER,
                    `coins` INTEGER,
                    `goods` INTEGER, PRIMARY KEY (`id`)
                ) 
                
                PARTITION BY RANGE(id) (
                    PARTITION p0 VALUES LESS THAN (25000),
                    PARTITION p1 VALUES LESS THAN (50000),
                    PARTITION p2 VALUES LESS THAN (75000),
                    PARTITION p3 VALUES LESS THAN (100000),
                    PARTITION p4 VALUES LESS THAN (125000),
                    PARTITION p5 VALUES LESS THAN (150000),
                    PARTITION p6 VALUES LESS THAN (175000),
                    PARTITION p7 VALUES LESS THAN (MAXVALUE)    
                );

                """
            )    

def insert_table(N) -> None:
    for i in range(N):
        random_coins = random.randint(1, 10000) 
        random_goods = random.randint(1, 10000) 
        player = (i, random_coins, random_goods)
        with get_connection(autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute("INSERT INTO players (id, coins, goods) VALUES (%s, %s, %s)", player)    


def repartition_range_to_hash() -> None:
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur: 
            cur.execute(
                """
                CREATE TABLE new_players (
                    `id` INTEGER,
                    `coins` INTEGER,
                    `goods` INTEGER, PRIMARY KEY (`id`)
                )

                PARTITION BY HASH(id)
                PARTITIONS 8;
                """
            )

            cur.execute("INSERT INTO new_players SELECT * FROM players;")

            cur.execute("DROP TABLE players;")

            cur.execute("ALTER TABLE new_players RENAME TO players;")
                        

def repartition_hash_to_range() -> None:
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur: 
            cur.execute(
                """
                CREATE TABLE new_players (
                    `id` INTEGER,
                    `coins` INTEGER,
                    `goods` INTEGER, PRIMARY KEY (`id`)
                ) 
                
                PARTITION BY RANGE(id) (
                    PARTITION p0 VALUES LESS THAN (5000),
                    PARTITION p1 VALUES LESS THAN (10000),
                    PARTITION p2 VALUES LESS THAN (15000),
                    PARTITION p3 VALUES LESS THAN (20000),
                    PARTITION p4 VALUES LESS THAN (25000),
                    PARTITION p5 VALUES LESS THAN (30000),
                    PARTITION p6 VALUES LESS THAN (40000),
                    PARTITION p7 VALUES LESS THAN (50000)    
                );
                """
            )

            cur.execute("INSERT INTO new_players SELECT * FROM players;")

            cur.execute("DROP TABLE players;")

            cur.execute("ALTER TABLE new_players RENAME TO players;")


def add_tiflash_replica():
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur: 
            cur.execute(    
                "ALTER TABLE players SET TIFLASH REPLICA 1;"
            )

def check_replica_status():
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur: 
            cur.execute(    
                "SELECT * FROM information_schema.tiflash_replica WHERE TABLE_SCHEMA = 'tpch' and TABLE_NAME = 'players';"
            )    
            print(cur.fetchall())



def db_exec() -> tuple:
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            #cur.execute("explain analyze select * from players where id < 20000;")
            
            #cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'tpch' AND table_name = 'player';")

            config = Config()
            cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = 'players';".format(config.TIDB_DB_NAME))

            if len(cur.fetchall()) > 0:
                print("exist")
            else:
                print("no")

            # cur.execute(
            #     """
            #     SELECT      TABLE_NAME,     PARTITION_NAME,     PARTITION_ORDINAL_POSITION,     PARTITION_METHOD,     SUBPARTITION_METHOD,     PARTITION_EXPRESSION,     SUBPARTITION_EXPRESSION,     PARTITION_DESCRIPTION FROM      INFORMATION_SCHEMA.PARTITIONS WHERE      TABLE_NAME = 'players';
            #     """
            # )
            
            tables = cur.fetchall()  # 处理所有结果
            print(tables)
            print(type(tables))
            print(len(tables))
            #print(tables[0])
            #for table in tables:
            #    print(table)

if __name__ == "__main__":
    #db_exec()
    #create_orderline_range_partition('ol_delivery_d')
    #partition_orderline_range('ol_delivery_d')
    create_table_range()
    #create_table_hash()
    insert_table(200000)
    #repartition()
    #repartition_range_to_hash()
    #repartition_hash_to_range()
    #add_tiflash_replica()
    #check_replica_status()
