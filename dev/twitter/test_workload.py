import mysql.connector
from mysql.connector import MySQLConnection
import random
import threading
import time
from collections import defaultdict
from statistics import mean
import os
import sys
sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from config import Config

# 配置参数
CONFIG = {
    "host": "127.0.0.1",
    "port": 4000,
    "user": "root",
    "password": "",
    "ap_queries": 100,  # 控制AP查询数量
    "ap_ratios": [0.2, 0.4, 0.6, 0.8],  # 测试不同的AP占比
    "ap_distribution": [0.2, 0.15, 0.15, 0.2, 0.15, 0.15],  # AP六种查询的比例
    "tp_distribution": [0.5, 0.3, 0.2],  # TP三种事务的比例
    "threads": 4
}

lock = threading.Lock()
ap_latencies = []
tp_count = 0
ap_count = 0

def get_connection(autocommit: bool = True) -> MySQLConnection:
  config = Config()
  db_conf = {
    "host": config.TIDB_HOST,
    "port": config.TIDB_PORT,
    "user": config.TIDB_USER,
    "password": config.TIDB_PASSWORD,
    "database": "tw_advisor", #config.TIDB_DB_NAME,
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

# SQL 模板
def run_ap_query(cur, qtype):
    uid = random.randint(1, 10000)
    start = time.time()
    if qtype == 0:
        # cur.execute("SELECT * FROM tweets WHERE user_id = %s ORDER BY created_at DESC LIMIT 10", (uid,))
        cur.execute("SELECT * FROM tweets WHERE user_id = %s", (uid,))
    elif qtype == 1:
        cur.execute("SELECT u.* FROM follows f JOIN users u ON f.follower_id = u.user_id WHERE f.followee_id = %s", (uid,))
    elif qtype == 2:
        cur.execute("SELECT u.* FROM follows f JOIN users u ON f.followee_id = u.user_id WHERE f.follower_id = %s", (uid,))
    elif qtype == 3:
        cur.execute("""SELECT t.* FROM tweets t
                       WHERE t.user_id IN (SELECT followee_id FROM follows WHERE follower_id = %s)""", (uid,))
                    #    ORDER BY t.created_at DESC LIMIT 10""", (uid,))
    elif qtype == 4:
        tweet_id = random.randint(1, CONFIG["ap_queries"])
        cur.execute("SELECT t.*, u.name FROM tweets t JOIN users u ON t.user_id = u.user_id WHERE t.tweet_id = %s", (tweet_id,))
    elif qtype == 5:
        uid2 = random.randint(1, 10000)
        cur.execute("SELECT 1 FROM follows WHERE follower_id = %s AND followee_id = %s", (uid, uid2))
    _ = cur.fetchall()
    latency = (time.time() - start) * 1000
    with lock:
        ap_latencies.append(latency)

def run_tp_transaction(cur, ttype):
    uid = random.randint(1, 10000)
    uid2 = random.randint(1, 10000)
    tweet_id = random.randint(1, CONFIG["ap_queries"])
    try:
        if ttype == 0:  # 发推
            content = f"Generated tweet {random.randint(1, 1_000_000)}"
            cur.execute("START TRANSACTION")
            cur.execute("INSERT INTO tweets (tweet_id, user_id, content) VALUES (%s, %s, %s)",
                        (tweet_id, uid, content))
            cur.execute("COMMIT")
        elif ttype == 1:  # 关注
            cur.execute("START TRANSACTION")
            cur.execute("INSERT IGNORE INTO follows (follower_id, followee_id) VALUES (%s, %s)",
                        (uid, uid2))
            cur.execute("COMMIT")
        elif ttype == 2:  # 取消关注
            cur.execute("START TRANSACTION")
            cur.execute("DELETE FROM follows WHERE follower_id = %s AND followee_id = %s", (uid, uid2))
            cur.execute("COMMIT")
        with lock:
            global tp_count
            tp_count += 1
    except:
        cur.execute("ROLLBACK")

def worker(ap_ratio, ap_queries_per_thread):
    with get_connection(autocommit=False) as connection:
      with connection.cursor() as cur: 
        ap_queries_executed = 0
        while ap_queries_executed < ap_queries_per_thread:
            if random.random() < ap_ratio:
                # 选择一个 AP 查询
                qtype = random.choices(range(6), weights=CONFIG["ap_distribution"])[0]
                run_ap_query(cur, qtype)
                with lock:
                    global ap_count
                    ap_count += 1
                ap_queries_executed += 1
            else:
                # 选择一个 TP 事务
                ttype = random.choices(range(3), weights=CONFIG["tp_distribution"])[0]
                run_tp_transaction(cur, ttype)

def run_benchmark(ap_ratio):
    global ap_latencies, tp_count, ap_count
    ap_latencies = []
    tp_count = 0
    ap_count = 0
    
    threads = []
    ap_queries_per_thread = CONFIG["ap_queries"] // CONFIG["threads"]
    start = time.time()
    
    for _ in range(CONFIG["threads"]):
        t = threading.Thread(target=worker, args=(ap_ratio, ap_queries_per_thread))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    duration = time.time() - start
    return duration, ap_count, tp_count, ap_latencies

if __name__ == "__main__":
    print("===== Benchmark Results =====")
    print(f"AP Queries per test: {CONFIG['ap_queries']}")
    print(f"Threads: {CONFIG['threads']}")
    print()
    
    results = []
    for ap_ratio in CONFIG["ap_ratios"]:
        print(f"Testing AP ratio: {ap_ratio:.1f}")
        duration, ap_count, tp_count, latencies = run_benchmark(ap_ratio)
        
        result = {
            "ap_ratio": ap_ratio,
            "duration": duration,
            "ap_count": ap_count,
            "tp_count": tp_count,
            "avg_latency": mean(latencies) if latencies else 0,
            "throughput": tp_count / duration if duration > 0 else 0
        }
        results.append(result)
        
        print(f"  Duration: {duration:.2f} seconds")
        print(f"  AP Queries: {ap_count}")
        print(f"  TP Transactions: {tp_count}")
        print(f"  AP Avg Latency: {result['avg_latency']:.2f} ms")
        print(f"  TP Throughput: {result['throughput']:.2f} txn/sec")
        print()
    
    # 总结报告
    print("===== Summary Report =====")
    print(f"{'AP Ratio':<10} {'Duration(s)':<12} {'AP Count':<10} {'TP Count':<10} {'AP Latency(ms)':<15} {'TP Throughput':<15}")
    print("-" * 80)
    for result in results:
        print(f"{result['ap_ratio']:<10.1f} {result['duration']:<12.2f} {result['ap_count']:<10} {result['tp_count']:<10} {result['avg_latency']:<15.2f} {result['throughput']:<15.2f}")
