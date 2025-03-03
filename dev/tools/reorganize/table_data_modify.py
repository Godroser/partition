import os
import sys

sys.path.append(os.path.expanduser("/data3/dzh/project/grep/dev/tools"))

from repartitioning import *
from repartitioning import get_create_table_sql

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

    # 提取列定义部分
    columns_part = original_sql.split('(', 1)[1].rsplit(')', 1)[0]
    columns = columns_part.splitlines()
    
    # 生成子表1的列定义
    sub_table1_columns = [col for col in columns if not any(replica_col in col for replica_col in replica_columns)]
    sub_table1_columns = [col.strip().rstrip(',') for col in sub_table1_columns if col.strip()]  # 去掉空行并去除多余空格和逗号
    sub_table1_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表2的列定义
    sub_table2_columns = [col for col in columns if any(replica_col in col for replica_col in replica_columns)]
    sub_table2_columns = [col.strip().rstrip(',') for col in sub_table2_columns if col.strip()]  # 去掉空行并去除多余空格和逗号
    sub_table2_columns.extend([f"{key} int(11) NOT NULL" for key in primary_keys])
    sub_table2_columns.append(f"PRIMARY KEY ({', '.join(primary_keys)})")

    # 生成子表1的建表语句
    sub_table1_sql = f"CREATE TABLE `{table_name}_part1` (\n" + ",\n".join(sub_table1_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

    # 生成子表2的建表语句
    sub_table2_sql = f"CREATE TABLE `{table_name}_part2` (\n" + ",\n".join(sub_table2_columns) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;"

    return sub_table1_sql, sub_table2_sql

def split_table_data(table_name, replica_columns):
    input_dir = "/data3/dzh/CH-data/ch"
    output_dir = "/data3/dzh/project/grep/dev/tools/reorganize/data"
    os.makedirs(output_dir, exist_ok=True)

    # 获取主键
    primary_keys = table_to_class[table_name]().primary_keys
    primary_key_indices = [table_to_class[table_name]().columns.index(key) for key in primary_keys]

    # 查找对应的 .sql 文件
    sql_files = [f for f in os.listdir(input_dir) if f.startswith(f"ch_bak.{table_name}.") and f.endswith(".sql")]

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
            part1_columns = [col for i, col in enumerate(columns) if i not in replica_columns]
            part2_columns = [col for i, col in enumerate(columns) if i in replica_columns]
            part2_columns.extend([columns[i] for i in primary_key_indices])

            # part1_values.append(f"({','.join(part1_columns)})")
            # part2_values.append(f"({','.join(part2_columns)})")
            part1_values.append(",".join(part1_columns))
            part2_values.append(",".join(part2_columns))

        print(part1_values[:5])
        print(part2_values[:5])
        part1_sql = f"{insert_statement1.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part1_values) + ");"
        part2_sql = f"{insert_statement2.split('VALUES')[0]} VALUES\n(" + "),\n(".join(part2_values) + ");"


        # part1_sql = f"{insert_statement1.split('VALUES')[0]} VALUES\n" + ",\n".join(part1_values) + ";"
        # part2_sql = f"{insert_statement2.split('VALUES')[0]} VALUES\n" + ",\n".join(part2_values) + ";"

        with open(os.path.join(output_dir, f"{table_name}_part1_{sql_file}"), 'w') as file:
            file.write(part1_sql)

        with open(os.path.join(output_dir, f"{table_name}_part2_{sql_file}"), 'w') as file:
            file.write(part2_sql)

# 示例：拆分customer表
replica_columns = ['c_data', 'c_discount', 'c_balance']
customer_part1_sql, customer_part2_sql = split_table_sql('customer', replica_columns)

print(customer_part1_sql)
print(customer_part2_sql)

# 示例：拆分customer表数据
replica_columns = [15, 16, 20]  # c_discount, c_balance,  c_data 在插入语句中的索引
split_table_data('customer', replica_columns)




# # 对其他表进行类似操作
# tables = ['warehouse', 'supplier', 'stock', 'region', 'orders', 'order_line', 'new_order', 'nation', 'item', 'history', 'district']
# replica_columns_dict = {
#     'warehouse': [1, 2, 3],  # w_name, w_street_1, w_street_2
#     'supplier': [1, 2],  # S_NAME, S_ADDRESS
#     'stock': [2, 3],  # s_quantity, s_ytd
#     'region': [1, 2],  # R_NAME, R_COMMENT
#     'orders': [4, 5],  # o_entry_d, o_carrier_id
#     'order_line': [5, 6],  # ol_delivery_d, ol_amount
#     'new_order': [2],  # no_d_id
#     'nation': [1, 2],  # N_NAME, N_COMMENT
#     'item': [1, 2],  # i_name, i_price
#     'history': [5, 6],  # h_date, h_amount
#     'district': [1, 2, 3]  # d_name, d_street_1, d_street_2
# }

# for table in tables:
#     replica_columns = replica_columns_dict[table]
#     part1_sql, part2_sql = split_table_sql(table, replica_columns)
#     print(part1_sql)
#     print(part2_sql)

# for table in tables:
#     replica_columns = replica_columns_dict[table]
#     split_table_data(table, replica_columns)
