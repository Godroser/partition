import re

# 示例输入：查询计划的字符串
sql_plan = """
| id                                                 | estRows    | task      | access object  | operator info                                                                                                                                                                                                                                    |
+----------------------------------------------------+------------+-----------+----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Sort_30                                            | 1.00       | root      |                | ch.supplier.s_name                                                                                                                                                                                                                               |
| └─HashJoin_34                                      | 1.00       | root      |                | inner join, equal:[eq(ch.supplier.s_suppkey, Column#50)]                                                                                                                                                                                         |
|   ├─HashAgg_43(Build)                              | 1.00       | root      |                | group by:Column#63, funcs:firstrow(Column#62)->Column#50                                                                                                                                                                                         |
|   │ └─Projection_70                                | 0.80       | root      |                | mod(mul(ch.stock.s_i_id, ch.stock.s_w_id), 10000)->Column#62, mod(mul(ch.stock.s_i_id, ch.stock.s_w_id), 10000)->Column#63                                                                                                                       |
|   │   └─Selection_44                               | 0.80       | root      |                | gt(cast(mul(2, ch.stock.s_quantity), decimal(20,0) BINARY), Column#49)                                                                                                                                                                           |
|   │     └─HashAgg_45                               | 1.00       | root      |                | group by:Column#59, Column#60, Column#61, funcs:sum(Column#55)->Column#49, funcs:firstrow(Column#56)->ch.stock.s_i_id, funcs:firstrow(Column#57)->ch.stock.s_w_id, funcs:firstrow(Column#58)->ch.stock.s_quantity                                |
|   │       └─Projection_69                          | 0.00       | root      |                | cast(ch.order_line.ol_quantity, decimal(10,0) BINARY)->Column#55, ch.stock.s_i_id->Column#56, ch.stock.s_w_id->Column#57, ch.stock.s_quantity->Column#58, ch.stock.s_i_id->Column#59, ch.stock.s_w_id->Column#60, ch.stock.s_quantity->Column#61 |
|   │         └─Projection_46                        | 0.00       | root      |                | ch.stock.s_i_id, ch.stock.s_w_id, ch.stock.s_quantity, ch.order_line.ol_quantity                                                                                                                                                                 |
|   │           └─HashJoin_48                        | 0.00       | root      |                | inner join, equal:[eq(ch.stock.s_i_id, ch.item.i_id)]                                                                                                                                                                                            |
|   │             ├─HashJoin_50(Build)               | 0.00       | root      |                | inner join, equal:[eq(ch.order_line.ol_i_id, ch.stock.s_i_id)]                                                                                                                                                                                   |
|   │             │ ├─TableReader_53(Build)          | 0.00       | root      |                | data:Selection_52                                                                                                                                                                                                                                |
|   │             │ │ └─Selection_52                 | 0.00       | cop[tikv] |                | gt(ch.order_line.ol_delivery_d, 2024-12-23 12:00:00.000000)                                                                                                                                                                                      |
|   │             │ │   └─TableFullScan_51           | 1250435.00 | cop[tikv] | table:ol       | keep order:false                                                                                                                                                                                                                                 |
|   │             │ └─TableReader_56(Probe)          | 359998.40  | root      | partition:all  | data:Selection_55                                                                                                                                                                                                                                |
|   │             │   └─Selection_55                 | 359998.40  | cop[tikv] |                | not(isnull(mod(mul(ch.stock.s_i_id, ch.stock.s_w_id), 10000)))                                                                                                                                                                                   |
|   │             │     └─TableFullScan_54           | 449998.00  | cop[tikv] | table:s        | keep order:false                                                                                                                                                                                                                                 |
|   │             └─HashAgg_62(Probe)                | 29.07      | root      |                | group by:ch.item.i_id, funcs:firstrow(ch.item.i_id)->ch.item.i_id                                                                                                                                                                                |
|   │               └─TableReader_63                 | 29.07      | root      | partition:all  | data:HashAgg_57                                                                                                                                                                                                                                  |
|   │                 └─HashAgg_57                   | 29.07      | cop[tikv] |                | group by:ch.item.i_id,                                                                                                                                                                                                                           |
|   │                   └─Selection_61               | 29.26      | cop[tikv] |                | like(ch.item.i_data, "co%", 92)                                                                                                                                                                                                                  |
|   │                     └─TableFullScan_60         | 100000.00  | cop[tikv] | table:i        | keep order:false                                                                                                                                                                                                                                 |
|   └─HashJoin_37(Probe)                             | 10.00      | root      |                | inner join, equal:[eq(ch.nation.n_nationkey, ch.supplier.s_nationkey)]                                                                                                                                                                           |
|     ├─TableReader_40(Build)                        | 0.03       | root      | partition:all  | data:Selection_39                                                                                                                                                                                                                                |
|     │ └─Selection_39                               | 0.03       | cop[tikv] |                | eq(ch.nation.n_name, "GERMANY")                                                                                                                                                                                                                  |
|     │   └─TableFullScan_38                         | 25.00      | cop[tikv] | table:nation   | keep order:false, stats:pseudo                                                                                                                                                                                                                   |
|     └─TableReader_42(Probe)                        | 10000.00   | root      | partition:all  | data:TableFullScan_41                                                                                                                                                                                                                            |
|       └─TableFullScan_41                           | 10000.00   | cop[tikv] | table:supplier | keep order:false                                                                                                                                                                                                                                 |
+----------------------------------------------------+------------+-----------+----------------+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
27 rows in set (0.00 sec)
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
