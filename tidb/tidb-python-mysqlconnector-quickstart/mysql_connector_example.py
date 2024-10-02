# Copyright 2023 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor

from config import Config


def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.tidb_host,
        "port": config.tidb_port,
        "user": config.tidb_user,
        "password": config.tidb_password,
        "database": config.tidb_db_name,
        "autocommit": autocommit,
        # mysql-connector-python will use C extension by default,
        # to make this example work on all platforms more easily,
        # we choose to use pure python implementation.
        "use_pure": True,
    }

    if config.ca_path:
        db_conf["ssl_verify_cert"] = True
        db_conf["ssl_verify_identity"] = True
        db_conf["ssl_ca"] = config.ca_path
    return mysql.connector.connect(**db_conf)


def recreate_table() -> None:
    with get_connection(autocommit=False) as connection:
        with connection.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS players;")
            cur.execute(
                """
                CREATE TABLE players (
                    `id` VARCHAR(36),
                    `coins` INTEGER,
                    `goods` INTEGER, PRIMARY KEY (`id`)
                );
            """
            )


def create_player(cursor: MySQLCursor, player: tuple) -> None:
    cursor.execute("INSERT INTO players (id, coins, goods) VALUES (%s, %s, %s)", player)


def get_player(cursor: MySQLCursor, player_id: str) -> dict:
    cursor.execute("SELECT id, coins, goods FROM players WHERE id = %s", (player_id,))
    return cursor.fetchone()


def get_players_with_limit(cursor: MySQLCursor, limit: int) -> tuple:
    cursor.execute("SELECT id, coins, goods FROM players LIMIT %s", (limit,))
    return cursor.fetchall()


def bulk_create_player(cursor: MySQLCursor, players: list[tuple]) -> None:
    cursor.executemany("INSERT INTO players (id, coins, goods) VALUES (%s, %s, %s)", players)


def get_count(cursor: MySQLCursor) -> int:
    cursor.execute("SELECT count(*) FROM players")
    return cursor.fetchone()["count(*)"]


def generate_random_players(amount: int) -> list[tuple]:
    players = []
    for i in range(amount):
        players.append((f"test{i}", 10000, 10000))
    return players


def simple_example() -> None:
    with get_connection(autocommit=True) as connection:
        with connection.cursor(dictionary=True) as cur:
            # create a player, who has a coin and a goods.
            create_player(cur, ("test", 1, 1))

            # get this player, and print it.
            test_player = get_player(cur, "test")
            print(test_player)

            # create players with bulk inserts.
            # insert 1919 players totally, with 114 players per batch.
            # all players have random uuid
            player_list = generate_random_players(1919)
            for idx in range(0, len(player_list), 114):
                bulk_create_player(cur, player_list[idx : idx + 114])

            # print the number of players
            count = get_count(cur)
            print(f"number of players: {count}")

            # print 3 players.
            three_players = get_players_with_limit(cur, 3)
            for player in three_players:
                print(player)


def trade(connection: MySQLConnection, sell_id: str, buy_id: str, amount: int, price: int) -> None:
    # This function should be called in a transaction.
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT coins, goods FROM players WHERE id = %s FOR UPDATE", (sell_id,))
        seller = cursor.fetchone()
        if seller["goods"] < amount:
            print(f"sell player {sell_id} goods not enough")
            connection.rollback()
            return

        cursor.execute("SELECT coins, goods FROM players WHERE id = %s FOR UPDATE", (buy_id,))
        buyer = cursor.fetchone()
        if buyer["coins"] < price:
            print(f"buy player {buy_id} coins not enough")
            connection.rollback()
            return

        try:
            update_player_sql = "UPDATE players set goods = goods + %s, coins = coins + %s WHERE id = %s"
            # deduct the goods of seller, and raise his/her the coins
            cursor.execute(update_player_sql, (-amount, price, sell_id))
            # deduct the coins of buyer, and raise his/her the goods
            cursor.execute(update_player_sql, (amount, -price, buy_id))
        except Exception as err:
            connection.rollback()
            print(f"something went wrong: {err}")
        else:
            connection.commit()
            print("trade success")


def trade_example() -> None:
    with get_connection(autocommit=False) as conn:
        # If autocommit mode is disabled within a session with SET autocommit = 0,
        # the session always has a transaction open.
        with conn.cursor(dictionary=True) as cur:
            # create two players
            # player 1: id is "1", has only 100 coins.
            # player 2: id is "2", has 114514 coins, and 20 goods.
            create_player(cur, ("1", 100, 0))
            create_player(cur, ("2", 114514, 20))
            conn.commit()

        # player 1 wants to buy 10 goods from player 2.
        # it will cost 500 coins, but player 1 cannot afford it.
        # so this trade will fail, and nobody will lose their coins or goods
        trade(conn, sell_id="2", buy_id="1", amount=10, price=500)

        # then player 1 has to reduce the incoming quantity to 2.
        # this trade will successful
        trade(conn, sell_id="2", buy_id="1", amount=2, price=100)

        # let's take a look for player 1 and player 2 currently
        with conn.cursor(dictionary=True) as cur:
            print(get_player(cur, "1"))
            print(get_player(cur, "2"))


if __name__ == "__main__":
    recreate_table()
    simple_example()
    trade_example()
