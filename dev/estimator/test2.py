import re

def replace_alias_with_table_name(explain_output, table_alias_map):
    for alias, table_name in table_alias_map.items():
        explain_output = re.sub(r'\b' + re.escape(alias) + r'\b', table_name, explain_output)
    return explain_output

# 示例：表的别名映射
table_alias_map = {
    'o': 'orders',
    'ol': 'order_line',
    'st': 'stock',
    'c': 'customer',
    's': 'supplier',
    'n1': 'nation',
    'n2': 'nation',   
}

# 示例：EXPLAIN 输出内容
explain_output = """
"""

# 替换别名为原表名
updated_explain_output = replace_alias_with_table_name(explain_output, table_alias_map)
print(updated_explain_output)
