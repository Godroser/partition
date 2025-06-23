import os
import sys
import json
import re

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from tools.repartitioning import *
from tools.repartitioning import get_create_table_sql
from tools.repartitioning import generate_partition_sql
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

# 导入主键类
from estimator.ch_columns_ranges_meta import Customer_columns, Warehouse_columns, Supplier_columns, Stock_columns, Region_columns, Orders_columns, Order_line_columns, New_order_columns, Nation_columns, Item_columns, History_columns, District_columns

def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": '50proteus', #指定测试库
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

# 定义表名到主键类的映射
table_to_class = {
    'customer': Customer_columns,
    'warehouse': Warehouse_columns,
    'supplier': Supplier_columns,
    'stock': Stock_columns,
    'region': Region_columns,
    'orders': Orders_columns,
    'order_line': Order_line_columns,
    'new_order': New_order_columns,
    'nation': Nation_columns,
    'item': Item_columns,
    'history': History_columns,
    'district': District_columns
}

def split_table_sql(table_name, replica_columns, partition_keys, replica_partition_keys):
    original_sql = get_create_table_sql(table_name)
    if not original_sql:
        raise ValueError(f"No create table SQL found for table {table_name}")

    # 获取主键
    primary_keys = table_to_class[table_name]().primary_keys

    # replica_columns去掉包含的主键
    replica_columns_without_primary_keys = [col for col in replica_columns if col not in primary_keys]    

    # 提取列定义部分
    columns_part = original_sql.split('(', 1)[1].rsplit(')', 1)[0]
    columns = columns_part.splitlines()
    
    # 生成子表1的列定义
    sub_table1_columns = [col for col in columns if not any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
    sub_table1_columns = [col.strip().rstrip(',') for col in sub_table1_columns if col.strip()]  # 去掉空行并去除多余空格和逗号
    # sub_table1_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表2的列定义
    sub_table2_columns = [col for col in columns if any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
    # sub_table2_columns.extend([f"{key} int(11) NOT NULL" for key in primary_keys])
    sub_table2_columns.extend(col for col in columns if any(replica_col in col for replica_col in primary_keys))
    sub_table2_columns = [col.strip().rstrip(',') for col in sub_table2_columns if col.strip()]  # 去掉空行并去除多余空格和逗号
    # sub_table2_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表1的建表语句
    sub_table1_sql = f"CREATE TABLE `{table_name}_part1` (\n" + ",\n".join(sub_table1_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

    # 生成子表2的建表语句
    sub_table2_sql = f"CREATE TABLE `{table_name}_part2` (\n" + ",\n".join(sub_table2_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin"

    # 加上分区的创建语句
    # print("table_name:",table_name)
    # print("replica_columns:",replica_columns) 
    if partition_keys:
        sub_table1_sql = comnbine_repartition_sql([sub_table1_sql], generate_partition_sql([table_name], [partition_keys]))[0]   ## 使用generate_partition_sql时, 需要确保config设置成ch_bak数据库
    if replica_partition_keys:
        sub_table2_sql = comnbine_repartition_sql([sub_table2_sql],generate_partition_sql([table_name], [replica_partition_keys]))[0]   
    # print("sub_table1_sql, sub_table2_sql", sub_table1_sql, sub_table2_sql)

    output_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data/"
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"{table_name}.schema.sql"), 'w') as file:    
        file.write(sub_table1_sql)
        file.write("\n")
        file.write(sub_table2_sql)
        file.write("\n")

    return sub_table1_sql, sub_table2_sql

def split_table_data(table_name, replica_columns):
    input_dir = "/data3/dzh/CH-data/50ch"
    output_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data/50proteus"
    os.makedirs(output_dir, exist_ok=True)

    # 获取主键
    primary_keys = table_to_class[table_name]().primary_keys
    primary_key_indices = [table_to_class[table_name]().columns.index(key) for key in primary_keys]

    # replica_columns去掉包含的主键
    replica_columns_without_primary_keys = [col for col in replica_columns if col not in primary_keys]    
    replica_columns_without_primary_keys_indices = [table_to_class[table_name]().columns.index(key) for key in replica_columns_without_primary_keys]

    # 查找对应的 .sql 文件
    sql_files = [f for f in os.listdir(input_dir) if f.startswith(f"50ch.{table_name}.") and f.endswith(".sql")]

    # 生成的.sql文件名
    files = []

    # 清除.sql文件内容
    for sql_file in sql_files:
        with open(os.path.join(output_dir, f"{table_name}_part1_{sql_file}"), 'w') as file:
            file.write('')
        with open(os.path.join(output_dir, f"{table_name}_part2_{sql_file}"), 'w') as file:
            file.write('')    

    for sql_file in sql_files:
        with open(os.path.join(input_dir, sql_file), 'r') as file:
            lines = file.readlines()

        insert_statement = ""
        values_lines = []

        for line in lines:
            line = line.strip()
            if line.startswith("INSERT INTO"):
                # 如果insert_statement不为空, 说明有多个insert语句, 需要处理上一个insert语句
                if insert_statement:
                    insert_statement1 = insert_statement.replace(table_name, table_name + '_part1')
                    insert_statement2 = insert_statement.replace(table_name, table_name + '_part2')

                    part1_values = []
                    part2_values = []

                    for value in values_lines:
                        columns = value

                        part1_columns = [col for i, col in enumerate(columns) if i not in replica_columns_without_primary_keys_indices]
                        part2_columns = [col for i, col in enumerate(columns) if i in replica_columns_without_primary_keys_indices]
                        part2_columns.extend([columns[i] for i in primary_key_indices])

                        part1_values.append(",".join(part1_columns))
                        part2_values.append(",".join(part2_columns))

                    part1_sql = f"\n{insert_statement1.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part1_values) + ");"
                    part2_sql = f"\n{insert_statement2.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part2_values) + ");"

                    with open(os.path.join(output_dir, f"{table_name}_part1_{sql_file}"), 'a') as file:
                        file.write(part1_sql)
                    # files.append(f"{table_name}_part1_{sql_file}")

                    with open(os.path.join(output_dir, f"{table_name}_part2_{sql_file}"), 'a') as file:
                        file.write(part2_sql)
                    # files.append(f"{table_name}_part2_{sql_file}")


                insert_statement = line
                values_lines = []
            elif line.startswith("/*"):
                continue
            elif line.endswith(","):
                line = line[:-1]

                # 正则表达式模式，用于匹配每一个列的值
                matches = re.findall(r"'(.*?)'|(\d+\.\d+)|(\d+)|(NULL)", line)
                parsed_line = [
                    f"'{m[0]}'" if m[0] else m[1] if m[1] else m[2] if m[2] else m[3] for m in matches
                ]

                values_lines.append(parsed_line)
                # print(",:", parsed_line)
            elif line.endswith(";"):
                line = line[:-1]  # 去掉最后一行的分号

                # 正则表达式模式，用于匹配每一个列的值
                matches = re.findall(r"'(.*?)'|(\d+\.\d+)|(\d+)|(NULL)", line)
                parsed_line = [
                    f"'{m[0]}'" if m[0] else m[1] if m[1] else m[2] if m[2] else m[3] for m in matches
                ]

                values_lines.append(parsed_line)           
                # print(";,", parsed_line)
        # print("values_lines:", values_lines)

        insert_statement1 = insert_statement.replace(table_name, table_name + '_part1')
        insert_statement2 = insert_statement.replace(table_name, table_name + '_part2')

        part1_values = []
        part2_values = []

        for value in values_lines:
            columns = value
            # part1_columns = [col for col in columns if not any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
            # part2_columns = [col for col in columns if any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
            part1_columns = [col for i, col in enumerate(columns) if i not in replica_columns_without_primary_keys_indices]
            part2_columns = [col for i, col in enumerate(columns) if i in replica_columns_without_primary_keys_indices]
            part2_columns.extend([columns[i] for i in primary_key_indices])

            # print("part1_columns:", part1_columns)
            # print("part2_columns:", part2_columns)
            # part1_values.append(f"({','.join(part1_columns)})")
            # part2_values.append(f"({','.join(part2_columns)})")
            part1_values.append(",".join(part1_columns))
            part2_values.append(",".join(part2_columns))

        # print(part1_values[:5])
        # print(part2_values[:5])
        part1_sql = f"{insert_statement1.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part1_values) + ");"
        part2_sql = f"{insert_statement2.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part2_values) + ");"


        # part1_sql = f"{insert_statement1.split('VALUES')[0]} VALUES\n" + ",\n".join(part1_values) + ";"
        # part2_sql = f"{insert_statement2.split('VALUES')[0]} VALUES\n" + ",\n".join(part2_values) + ";"

        with open(os.path.join(output_dir, f"{table_name}_part1_{sql_file}"), 'a') as file:
            file.write(part1_sql)
        files.append(f"{table_name}_part1_{sql_file}")

        with open(os.path.join(output_dir, f"{table_name}_part2_{sql_file}"), 'a') as file:
            file.write(part2_sql)
        files.append(f"{table_name}_part2_{sql_file}")
    return files


def get_replica_column_indices(table_name, replica_columns):
    """
    根据表名和副本列名列表，获取这些列在对应表的 self.columns 列表中的位置索引。
    参数:
        table_name (str): 表名，例如 'customer'。
        replica_columns (list): 副本列名列表，例如 ['c_data', 'c_discount', 'c_balance']。
    返回:
        list: 副本列在 self.columns 列表中的位置索引列表。
    """
    # 获取对应的列类实例
    columns_class = table_to_class[table_name]()
    
    # 获取 self.columns 列表
    columns_list = columns_class.columns
    
    # 查找 replica_columns 中各列的位置索引
    replica_column_indices = [columns_list.index(col) for col in replica_columns if col in columns_list]
    
    return replica_column_indices


# 指定table和replica_columns partition_keys, 自动创建两个子表,实现分区创建,实现副本, 并且导入对应数据
def modify_table_data(table, replica_columns, partition_keys, replica_partition_keys):
    input_dir = "/data3/dzh/CH-data/50ch"
    data_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data/50proteus"
    os.makedirs(data_dir, exist_ok=True)
    
    if replica_columns:
        # 实现分表的建表语句, 加上分区的创建语句
        # part1_sql是没有列存的表, part2_sql是有列存的表
        # part1_sql, part2_sql = split_table_sql(table, replica_columns, partition_keys, replica_partition_keys)

        # print("part1_sql, part2_sql:", part1_sql, part2_sql)
        # 生成分表的.sql数据文件
        sql_files = split_table_data(table, replica_columns)
    
        # execute create two table_parts sql
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:     
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}'.format(table))
                    print("Drop table {};".format(table))

                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part1';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part1'.format(table))
                    print("Drop table {}_part1;".format(table))
                print("part1_sql:", part1_sql)
                cur.execute(part1_sql)
                print("Table {}_part1 is created!".format(table))     

                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part2';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part2;'.format(table))
                    print("Drop table {}_part2".format(table))               

                cur.execute(part2_sql)
                print("Table {}_part2 is created!".format(table))                

                # 设置副本
                set_replica_sql = f"ALTER TABLE `50proteus`.`{table}_part2` SET TIFLASH REPLICA 1;"
                cur.execute(set_replica_sql)
                print("Table {}_part2 replica is created!".format(table))
                
        # # load table_part data
        # for file in sql_files:
        #     # command = "mysql -h {} -u {} -P {} {} < {}/{}".format(config.TIDB_HOST, config.TIDB_USER, config.TIDB_PORT, config.TIDB_DB_NAME, data_dir, file)
        #     command = "mysql -h {} -u {} -P {} {} < {}/{}".format('10.77.110.144', 'root', '4000', 'proteus', data_dir, file)
        #     print(command)
        #     result = subprocess.run(command, shell=True, capture_output=True, text=True)
        #     #print(result)
        #     # 判断命令是否成功执行
        #     if result.returncode == 0:
        #         print("命令执行成功")
        #     else:
        #         print("命令执行失败")
        #         print("错误信息:", result.stderr)            
        # print("Table {} data loaded!".format(table))    
    
    else:
        # 如果 replica_columns 为空，则直接执行原表SQL
        original_sql = get_create_table_sql(table)
        if not original_sql:
            raise ValueError(f"No create table SQL found for table {table}")

        # 加上分区的创建语句
        if partition_keys:
            original_sql = comnbine_repartition_sql([original_sql], generate_partition_sql([table], [partition_keys]))[0]   ## 使用generate_partition_sql时, 需要确保config设置成ch_bak数据库      

        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part1';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part1'.format(table))
                    print("Drop table {}_part1;".format(table))

                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part2';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part2'.format(table))
                    print("Drop table {}_part2;".format(table))                    

                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}';".format('50proteus', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}'.format(table))
                    print("Drop table {};".format(table))
                cur.execute(original_sql)
                print("Table {} is created!".format(table)) 

        # 查找对应的 .sql 文件
        sql_files = [f for f in os.listdir(input_dir) if f.startswith(f"50ch.{table}.") and f.endswith(".sql")]   

        # # load table_part data
        # for file in sql_files:
        #     command = "mysql -h {} -u {} -P {} {} < {}/{}".format('10.77.110.144', 'root', '4000', 'proteus', input_dir, file)
        #     print(command)
        #     result = subprocess.run(command, shell=True, capture_output=True, text=True)
        #     #print(result)
        #     # 判断命令是否成功执行
        #     if result.returncode == 0:
        #         print("命令执行成功")
        #     else:
        #         print("命令执行失败")
        #         print("错误信息:", result.stderr)            
        # print("Table {} data loaded!".format(table))  

def load_candidate(file_path):
    with open(file_path, 'r') as file:
        candidate = json.load(file)
    return candidate

if __name__ == "__main__":
    # 加载candidate
    # candidate_file_path = "/data3/dzh/project/grep/dev/Output/best_advisor copy.txt"
    candidate_file_path = "/data3/dzh/project/grep/dev/Output/proteus_advisor.txt"
    candidate = load_candidate(candidate_file_path)

    # 修改tables顺序
    tables = [table_info['name'] for table_info in candidate]
    # tables = ['orders', 'region', 'stock', 'supplier', 'warehouse']
    # tables = ['orders']
    # tables = ['customer', 'district', 'item', 'new_order', 'stock', 'warehouse', 'history', 'nation', 'region', 'supplier']

    # 记录candidate里的replicas, partition_keys, replica_partition_keys
    replica_columns_dict = {table_info['name']: table_info['replicas'] for table_info in candidate}
    #replica_columns_dict = {'customer': [], 'warehouse': ['w_ytd']}
    partition_keys_dict = {table_info['name']: table_info['partition_keys'] for table_info in candidate}
    replica_partition_keys_dict = {table_info['name']: table_info['replica_partition_keys'] for table_info in candidate}

    for table in tables:
        replica_columns = replica_columns_dict[table]
        partition_keys = partition_keys_dict[table]
        replica_partition_keys = replica_partition_keys_dict[table]

        # 创建子表, 实现分区, 设置副本, 导入数据
        # 不需要修改config.py, 写死成proteus数据库
        modify_table_data(table, replica_columns, partition_keys, replica_partition_keys)