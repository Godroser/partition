import os
import sys

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev"))

from tools.repartitioning import *
from tools.repartitioning import get_create_table_sql
import mysql.connector
from mysql.connector import MySQLConnection
from mysql.connector.cursor import MySQLCursor
from config import Config

# 导入主键类
from estimator.ch_columns_ranges_meta import Customer_columns, Warehouse_columns, Supplier_columns, Stock_columns, Region_columns, Orders_columns, Order_line_columns, New_order_columns, Nation_columns, Item_columns, History_columns, District_columns

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

def split_table_sql(table_name, replica_columns):
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
    sub_table1_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表2的列定义
    sub_table2_columns = [col for col in columns if any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
    # sub_table2_columns.extend([f"{key} int(11) NOT NULL" for key in primary_keys])
    sub_table2_columns.extend(col for col in columns if any(replica_col in col for replica_col in primary_keys))
    sub_table2_columns = [col.strip().rstrip(',') for col in sub_table2_columns if col.strip()]  # 去掉空行并去除多余空格和逗号
    sub_table2_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表1的建表语句
    sub_table1_sql = f"CREATE TABLE `{table_name}_part1` (\n" + ",\n".join(sub_table1_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

    # 生成子表2的建表语句
    sub_table2_sql = f"CREATE TABLE `{table_name}_part2` (\n" + ",\n".join(sub_table2_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

    output_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data/"
    os.makedirs(output_dir, exist_ok=True)
    with open(os.path.join(output_dir, f"{table_name}.schema.sql"), 'w') as file:    
        file.write(sub_table1_sql)
        file.write("\n")
        file.write(sub_table2_sql)
        file.write("\n")

    return sub_table1_sql, sub_table2_sql

def split_table_data(table_name, replica_columns):
    input_dir = "/data3/dzh/CH-data/ch"
    output_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data"
    os.makedirs(output_dir, exist_ok=True)

    # 获取主键
    primary_keys = table_to_class[table_name]().primary_keys
    primary_key_indices = [table_to_class[table_name]().columns.index(key) for key in primary_keys]

    # replica_columns去掉包含的主键
    replica_columns_without_primary_keys = [col for col in replica_columns if col not in primary_keys]    
    replica_columns_without_primary_keys_indices = [table_to_class[table_name]().columns.index(key) for key in replica_columns_without_primary_keys]

    # 查找对应的 .sql 文件
    sql_files = [f for f in os.listdir(input_dir) if f.startswith(f"ch_bak.{table_name}.") and f.endswith(".sql")]

    # 生成的.sql文件名
    files = []

    for sql_file in sql_files:
        with open(os.path.join(input_dir, sql_file), 'r') as file:
            lines = file.readlines()

        insert_statement = ""
        values_lines = []

        for line in lines:
            line = line.strip()
            if line.startswith("INSERT INTO"):
                insert_statement = line
            elif line.startswith("/*"):
                continue
            elif line.endswith(","):
                values_lines.append(line[:-1])
            elif line.endswith(";"):
                values_lines.append(line[:-1])  # 去掉最后一行的分号

        insert_statement1 = insert_statement.replace(table_name, table_name + '_part1')
        insert_statement2 = insert_statement.replace(table_name, table_name + '_part2')

        part1_values = []
        part2_values = []

        for value in values_lines:
            columns = value.strip("()").split(',')
            # part1_columns = [col for col in columns if not any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
            # part2_columns = [col for col in columns if any(replica_col in col for replica_col in replica_columns_without_primary_keys)]
            part1_columns = [col for i, col in enumerate(columns) if i not in replica_columns_without_primary_keys_indices]
            part2_columns = [col for i, col in enumerate(columns) if i in replica_columns_without_primary_keys_indices]
            part2_columns.extend([columns[i] for i in primary_key_indices])

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

        with open(os.path.join(output_dir, f"{table_name}_part1_{sql_file}"), 'w') as file:
            file.write(part1_sql)
        files.append(f"{table_name}_part1_{sql_file}")

        with open(os.path.join(output_dir, f"{table_name}_part2_{sql_file}"), 'w') as file:
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

def get_connection(autocommit: bool = True) -> MySQLConnection:
    config = Config()
    db_conf = {
        "host": config.TIDB_HOST,
        "port": config.TIDB_PORT,
        "user": config.TIDB_USER,
        "password": config.TIDB_PASSWORD,
        "database": 'ch_test', #指定测试库
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

# 指定table和replica_columns, 自动创建两个子表, 并且导入对应数据
def modify_table_data(table, replica_columns):
    input_dir = "/data3/dzh/CH-data/ch"
    data_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data"
    os.makedirs(data_dir, exist_ok=True)
    
    if replica_columns:
        # 有副本列的表
        replica_columns = replica_columns_dict[table]
        part1_sql, part2_sql = split_table_sql(table, replica_columns)
        sql_files = split_table_data(table, replica_columns)
    
        # execute create two table_parts sql
        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:     
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part1';".format('ch_test', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part1'.format(table))
                    print("Drop table {}_part1;".format(table))
                cur.execute(part1_sql)
                print("Table {}_part1 is created!".format(table))     

                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}_part2';".format('ch_test', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}_part2;'.format(table))
                    print("Drop table {}_part2".format(table))               

                cur.execute(part2_sql)
                print("Table {}_part2 is created!".format(table))                

        # load table_part data
        for file in sql_files:
            # command = "mysql -h {} -u {} -P {} {} < {}/{}".format(config.TIDB_HOST, config.TIDB_USER, config.TIDB_PORT, config.TIDB_DB_NAME, data_dir, file)
            command = "mysql -h {} -u {} -P {} {} < {}/{}".format('10.77.110.144', 'root', '4000', 'ch_test', data_dir, file)
            print(command)
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            #print(result)
            # 判断命令是否成功执行
            if result.returncode == 0:
                print("命令执行成功")
            else:
                print("命令执行失败")
                print("错误信息:", result.stderr)            
        print("Table {} data loaded!".format(table))    
    
    else: 
    # 如果 replica_columns 为空，则直接执行原表SQL
        original_sql = get_create_table_sql(table)
        if not original_sql:
            raise ValueError(f"No create table SQL found for table {table}")

        with get_connection(autocommit=False) as connection:
            with connection.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = '{}' AND table_name = '{}';".format('ch_test', table))

                if len(cur.fetchall()) > 0:
                    cur.execute('DROP TABLE {}'.format(table))
                    print("Drop table {};".format(table))
                cur.execute(original_sql)
                print("Table {} is created!".format(table)) 

        # 查找对应的 .sql 文件
        sql_files = [f for f in os.listdir(input_dir) if f.startswith(f"ch_bak.{table}.") and f.endswith(".sql")]   

        # load table_part data
        for file in sql_files:
            command = "mysql -h {} -u {} -P {} {} < {}/{}".format('10.77.110.144', 'root', '4000', 'ch_test', input_dir, file)
            print(command)
            result = subprocess.run(command, shell=True, capture_output=True, text=True)
            #print(result)
            # 判断命令是否成功执行
            if result.returncode == 0:
                print("命令执行成功")
            else:
                print("命令执行失败")
                print("错误信息:", result.stderr)            
        print("Table {} data loaded!".format(table))            



if __name__ == "__main__":
    # # 示例：拆分customer表
    # replica_columns = ['c_data', 'c_discount', 'c_balance']
    # customer_part1_sql, customer_part2_sql = split_table_sql('customer', replica_columns)

    # print(customer_part1_sql)
    # print(customer_part2_sql)

    # # 示例：拆分customer表数据
    # replica_columns = [15, 16, 20]  # c_discount, c_balance,  c_data 在插入语句中的索引
    # split_table_data('customer', replica_columns)




    # 对其他表进行类似操作
    tables = ['customer', 'warehouse', 'supplier', 'stock', 'region', 'orders', 'order_line', 'new_order', 'nation', 'item', 'history', 'district']
    replica_columns_dict = {
        'customer': [], #['c_data', 'c_discount', 'c_balance'],  # c_discount, c_balance,  c_data 
        'warehouse': ['w_ytd'], #['w_name', 'w_street_1', 'w_street_2'],  # w_name, w_street_1, w_street_2
        'supplier': [], #['s_name', 's_address'],  # S_NAME, S_ADDRESS
        'stock': ["s_dist_06",
            "s_dist_07",
            "s_dist_05",
            "s_dist_02",
            "s_dist_01",
            "s_dist_08",
            "s_data",
            "s_dist_04",
            "s_dist_03",
            "s_dist_10",
            "s_dist_09",
            "s_order_cnt"],  # s_quantity, s_ytd
        'region': [], #["r_regionkey", "r_name",],  # R_NAME, R_COMMENT
        'orders': [], #["o_entry_d", "o_carrier_id"],  # o_entry_d, o_carrier_id
        'order_line': [ "ol_dist_info", "ol_number"],  # ol_delivery_d, ol_amount
        'new_order': [], #["no_d_id"],  # no_d_id
        'nation': [], #["n_regionkey", "n_comment"],  # N_NAME, N_COMMENT
        'item': [], #["i_data"],  # i_name, i_price
        'history': [], #["h_date", "h_amount", "h_data"],  # h_date, h_amount
        'district': [] #[ "d_zip", "d_tax", "d_ytd"]  # d_name, d_street_1, d_street_2
    }

    # for table in tables:
    #     replica_columns = replica_columns_dict[table]
    #     part1_sql, part2_sql = split_table_sql(table, replica_columns)
    #     print(part1_sql)
    #     print(part2_sql)

    # for table in tables:
    #     replica_columns = replica_columns_dict[table]
    #     split_table_data(table, replica_columns)

    for table in tables:
        replica_columns = replica_columns_dict[table]
        modify_table_data(table, replica_columns)