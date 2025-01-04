import re

# 示例输入：查询计划的字符串
sql_plan = """
| id                                         | estRows   | task      | access object                                          | operator info                                                                                                                                                                                                                                                                                                   |
+--------------------------------------------+-----------+-----------+--------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Projection_37                              | 20.59     | root      |                                                        | Column#61->Column#62, Column#59->Column#63, Column#60->Column#64                                                                                                                                                                                                                                                |
| └─Projection_59                            | 20.59     | root      |                                                        | Column#61, Column#59, Column#60, ch.customer.c_state                                                                                                                                                                                                                                                            |
|   └─Sort_38                                | 20.59     | root      |                                                        | Column#72                                                                                                                                                                                                                                                                                                       |
|     └─Projection_60                        | 20.59     | root      |                                                        | Column#61, Column#59, Column#60, ch.customer.c_state, substr(ch.customer.c_state, 1, 1)->Column#72                                                                                                                                                                                                              |
|       └─Projection_39                      | 20.59     | root      |                                                        | substr(ch.customer.c_state, 1, 1)->Column#61, Column#59, Column#60, ch.customer.c_state                                                                                                                                                                                                                         |
|         └─HashAgg_40                       | 20.59     | root      |                                                        | group by:Column#71, funcs:count(1)->Column#59, funcs:sum(Column#69)->Column#60, funcs:firstrow(Column#70)->ch.customer.c_state                                                                                                                                                                                  |
|           └─Projection_58                  | 3655.49   | root      |                                                        | ch.customer.c_balance->Column#69, ch.customer.c_state->Column#70, substr(ch.customer.c_state, 1, 1)->Column#71                                                                                                                                                                                                  |
|             └─IndexHashJoin_45             | 3655.49   | root      |                                                        | anti semi join, inner:IndexReader_42, outer key:ch.customer.c_id, ch.customer.c_w_id, ch.customer.c_d_id, inner key:ch.orders.o_c_id, ch.orders.o_w_id, ch.orders.o_d_id, equal cond:eq(ch.customer.c_d_id, ch.orders.o_d_id), eq(ch.customer.c_id, ch.orders.o_c_id), eq(ch.customer.c_w_id, ch.orders.o_w_id) |
|               ├─TableReader_53(Build)      | 4569.36   | root      |                                                        | data:Selection_52                                                                                                                                                                                                                                                                                               |
|               │ └─Selection_52             | 4569.36   | cop[tikv] |                                                        | gt(ch.customer.c_balance, 49556.891238), in(substr(ch.customer.c_phone, 1, 1), "1", "2", "3", "4", "5", "6", "7")                                                                                                                                                                                               |
|               │   └─TableFullScan_51       | 123351.00 | cop[tikv] | table:c                                                | keep order:false                                                                                                                                                                                                                                                                                                |
|               └─IndexReader_42(Probe)      | 190447.87 | root      |                                                        | index:IndexRangeScan_41                                                                                                                                                                                                                                                                                         |
|                 └─IndexRangeScan_41        | 190447.87 | cop[tikv] | table:o, index:idx_order(o_w_id, o_d_id, o_c_id, o_id) | range: decided by [eq(ch.orders.o_w_id, ch.customer.c_w_id) eq(ch.orders.o_d_id, ch.customer.c_d_id) eq(ch.orders.o_c_id, ch.customer.c_id)], keep order:false                                                                                                                                                  |
+--------------------------------------------+-----------+-----------+--------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
13 rows in set, 1 warning (0.15 sec)
"""


######################################
def replace_alias_with_table_name(explain_output, table_alias_map):
    for alias, table_name in table_alias_map.items():
        explain_output = re.sub(r'\b' + re.escape(alias) + r'\b', table_name, explain_output)
    return explain_output

# 示例：表的别名映射
table_alias_map = {
    'o': 'orders',
    'ol': 'order_line',
    'l1': 'order_line',
    'l2': 'order_line',
    'st': 'stock',
    'c': 'customer',
    's': 'supplier',
    'n1': 'nation',
    'n2': 'nation',  
    'n': 'nation', 
    'i': 'item'
}

# 示例：EXPLAIN 输出内容
explain_output = sql_plan

# 替换别名为原表名
updated_explain_output = replace_alias_with_table_name(explain_output, table_alias_map)
sql_plan = updated_explain_output  
#####################################


# 解析 SQL 计划的每一行
lines = sql_plan.strip().split('\n')

# 存储每行的相关数据
operators = []

# 正则表达式用于匹配算子的 ID 和访问的表
pattern_operator = r"(└─|├─)(\S+)"
pattern_table = r"table:(\S+)"

# 对应表名的处理逻辑
table_info = {
    "orders": {
        "tablescan": 125038,
        "tablescan_size": 36,
        "selection": 125038
    },
    "order_line": {
        "tablescan": 1250435,
        "tablescan_size": 65,
        "selection": 1250435
    },
    "customer": {
        "tablescan": 120000,
        "tablescan_size": 671,
        "selection": 120000
    },
    "new_order": {
        "tablescan": 36418,
        "tablescan_size": 12,
        "selection": 36418
    },
    "item": {
        "tablescan": 100000,
        "tablescan_size": 87,
        "selection": 100000
    },
    "stock": {
        "tablescan": 400000,
        "tablescan_size": 314,
        "selection": 400000
    },
    "supplier": {
        "tablescan": 10000,
        "tablescan_size": 202,
        "selection": 10000
    },
    "region": {
        "tablescan": 5,
        "tablescan_size": 185,
        "selection": 5
    },
    "nation": {
        "tablescan": 25,
        "tablescan_size": 193,
        "selection": 25
    },
    "warehouse": {
        "tablescan": 4,
        "tablescan_size": 95,
        "selection": 4
    },
    "history": {
        "tablescan": 124913,
        "tablescan_size": 57,
        "selection": 124913
    }
}

# 遍历每一行，查找符合条件的算子
for i, line in enumerate(lines):
    operator_match = re.search(pattern_operator, line)
    if operator_match:
        operator_name = operator_match.group(2)
        
        # 截断算子名称中的 "_" 及其后的部分
        operator_name = operator_name.split("_")[0]

        # 如果算子的名称包含 'TableFullScan'，记录这一行
        if "TableFullScan" in operator_name:
            # 查找这一行中访问的表
            #
            table_match = re.search(pattern_table, line)
            table_name = table_match.group(1) if table_match else None
            
            # 找到上一行的算子名称
            previous_operator = lines[i - 1] if i - 1 >= 0 else None
            previous_operator_match = re.search(pattern_operator, previous_operator)
            previous_operator_name = previous_operator_match.group(2) if previous_operator_match else "N/A"

            # 截断上一行算子名称中的 "_" 及其后的部分
            previous_operator_name = previous_operator_name.split("_")[0]            
            
            # 记录当前算子及其上一个算子的相关信息
            operators.append({
                'operator_name': operator_name,
                'table_name': table_name,
                'previous_operator': previous_operator_name
            })        
        
            #print(previous_operator_name)
            # 判断 previous_operator 是否为 Selection 或 TableReader，且 table_name 是否匹配
            if previous_operator_name == "Selection" and table_name in table_info:
                # 根据表名输出相关信息
                table_data = table_info[table_name]
                print(f"self.rows_tablescan_{table_name} = {table_data['tablescan']} ##tbd")
                print(f"self.rowsize_tablescan_{table_name} = {table_data['tablescan_size']}")
                print(f"self.rows_selection_{table_name} = {table_data['selection']} ##tbd")
            
            elif previous_operator_name == "TableReader" and table_name in table_info:
                # 输出前两行的相关信息
                table_data = table_info[table_name]
                print(f"self.rows_tablescan_{table_name} = {table_data['tablescan']} ##tbd")
                print(f"self.rowsize_tablescan_{table_name} = {table_data['tablescan_size']}")
