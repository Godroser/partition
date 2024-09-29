import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

import random


TIDB_HOST='127.0.0.1'
TIDB_PORT='4000'
TIDB_USER='root'
TIDB_PASSWORD=''
TIDB_DB_NAME='tpch'
ca_path = ''


def get_connection(autocommit: bool = True) -> MySQLConnection:
    db_conf = {
        "host": TIDB_HOST,
        "port": TIDB_PORT,
        "user": TIDB_USER,
        "password": TIDB_PASSWORD,
        "database": TIDB_DB_NAME,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True
    }

    if ca_path:
        db_conf["ssl_verify_cert"] = True
        db_conf["ssl_verify_identity"] = True
        db_conf["ssl_ca"] = ca_path
    return mysql.connector.connect(**db_conf)

def create_table() -> None:
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

                PARTITION BY HASH(id)
                PARTITIONS 8;

                """
            )


def insert_table(N) -> None:
    for i in range(N):
        random_coins = random.randint(1, 1000) 
        random_goods = random.randint(1, 1000) 
        player = (i, random_coins, random_goods)
        with get_connection(autocommit=True) as connection:
            with connection.cursor() as cur:
                cur.execute("INSERT INTO players (id, coins, goods) VALUES (%s, %s, %s)", player)    




def db_exec() -> tuple:
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            #cur.execute("explain analyze select * from players where id < 500;")
            cur.execute(
                """
                SELECT      TABLE_NAME,     PARTITION_NAME,     PARTITION_ORDINAL_POSITION,     PARTITION_METHOD,     SUBPARTITION_METHOD,     PARTITION_EXPRESSION,     SUBPARTITION_EXPRESSION,     PARTITION_DESCRIPTION FROM      INFORMATION_SCHEMA.PARTITIONS WHERE      TABLE_NAME = 'players';
                """
            )
            tables = cur.fetchall()  # 处理所有结果
            print(tables)
            print(type(tables))
            print(len(tables))
            #print(tables[0])
            #for table in tables:
            #    print(table)

if __name__ == "__main__":
    db_exec()
    #create_table()
    #insert_table(5000)
