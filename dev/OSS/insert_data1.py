import mysql.connector
from mysql.connector import Error

# 配置数据库连接
def get_connection(autocommit: bool = True):
    db_conf = {
        "host": "tidb.lvmtrh1uh8at.clusters.staging.tidb-cloud.com",
        "port": 4000,
        "user": "root",
        "password": "pi314159",
        "database": "gharchive_dev",
        "autocommit": autocommit,
        "use_pure": True
    }
    return mysql.connector.connect(**db_conf)

# 分批插入函数
def batch_insert(start_id, end_id, step=1000000):
    with get_connection(autocommit=True) as connection:
        with connection.cursor() as cur:
            try:
                current_start = start_id
                while current_start < end_id:
                    current_end = min(current_start + step, end_id)
                    print(f"Inserting id from {current_start} to {current_end}...")

                    insert_sql = f"""
                        INSERT INTO github_users_partitioned
                        SELECT * FROM github_users
                        WHERE id >= {current_start} AND id < {current_end};
                    """

                    try:
                        cur.execute(insert_sql)
                        print(f"✔️ Inserted range {current_start} - {current_end}")
                    except Error as e:
                        print(f"❌ Error inserting range {current_start} - {current_end}: {e}")
                        break  # 也可以选择跳过 continue

                    current_start = current_end
                print("✅ All batches completed.")

            except Error as e:
                print("❌ Database connection error:", e)


# 运行示例：迁移 repo_id 从 30,000,000 到 40,000,000
if __name__ == "__main__":
    batch_insert(start_id=0, end_id=115332585)
