# import re

# def rewrite_sql(original_sql):
#     # Step 1: 提取原始查询中的表名、列名、WHERE 条件和 GROUP BY
#     # 匹配 SQL 查询中的 SELECT、FROM 和 WHERE 子句
#     select_pattern = r"SELECT\s+(.*?)\s+FROM\s+(\S+)\s+WHERE\s+(.*?)(\s+GROUP\s+BY\s+(.*?))?(\s+ORDER\s+BY\s+(.*))?"
#     match = re.search(select_pattern, original_sql, re.IGNORECASE | re.DOTALL)
    
#     if not match:
#         raise ValueError("原始 SQL 查询格式不正确")
    
#     # 提取 SELECT 子句中的列，FROM 子句中的表名，WHERE 子句和其他信息
#     select_cols = match.group(1)
#     from_table = match.group(2)
#     where_clause = match.group(3)
#     group_by_clause = match.group(5) or ""
#     order_by_clause = match.group(7) or ""
    
#     # Step 2: 处理原始查询中的聚合列，生成新的列名
#     # 替换原始查询中的聚合列以适应垂直分表后的表结构
#     select_cols = select_cols.replace("ol_number", "p1.ol_number")
#     select_cols = re.sub(r"sum\((.*?)\)", lambda m: f"SUM(p2.{m.group(1)})", select_cols)
#     select_cols = re.sub(r"avg\((.*?)\)", lambda m: f"AVG(p2.{m.group(1)})", select_cols)
#     select_cols = re.sub(r"count\(\*\)", "COUNT(*)", select_cols)

#     # Step 3: 生成新的 FROM 子句，连接拆分后的表
#     # 假设原始表拆分为 order_line_part1 和 order_line_part2，且需要连接 ol_number 字段
#     from_clause = "FROM order_line_part1 p1 JOIN order_line_part2 p2 ON p1.ol_number = p2.ol_number"

#     # Step 4: 保持 WHERE 子句，适应新表字段
#     where_clause = where_clause.replace("ol_delivery_d", "p2.ol_delivery_d")

#     # Step 5: 拼接最终 SQL 查询
#     new_sql = f"""
#     SELECT 
#         {select_cols}
#     {from_clause}
#     WHERE {where_clause}
#     {group_by_clause}
#     {order_by_clause};
#     """
    
#     return new_sql.strip()

# # 示例：原始 SQL 查询
# original_sql = """
# SELECT ol_number,  
#        sum(ol_quantity) as sum_qty,  
#        sum(ol_amount) as sum_amount,  
#        avg(ol_quantity) as avg_qty,  
#        avg(ol_amount) as avg_amount,  
#        count(*) as count_order 
# FROM order_line 
# WHERE ol_delivery_d > '2024-10-28 17:00:00' 
# GROUP BY ol_number 
# ORDER BY ol_number;
# """

# # 自动生成重写后的 SQL 查询
# new_sql = rewrite_sql(original_sql)

# # 输出重写后的 SQL 查询
# print("重写后的 SQL 查询：")
# print(new_sql)




import re

# 输入数据
# original_sql = """
# SELECT ol_number, 
#        SUM(ol_quantity) AS sum_qty, 
#        SUM(ol_amount) AS sum_amount, 
#        AVG(ol_quantity) AS avg_qty, 
#        AVG(ol_amount) AS avg_amount, 
#        COUNT(*) AS count_order 
# FROM order_line 
# WHERE ol_delivery_d > '2024-10-28 17:00:00' 
# GROUP BY ol_number 
# ORDER BY ol_number
# """
original_sql = """
SELECT su.s_suppkey, su.s_name, n.n_name, i.i_id, i.i_name, su.s_address, su.s_phone, su.s_comment FROM item AS i JOIN stock AS s ON i.i_id = s.s_i_id JOIN supplier AS su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey JOIN nation AS n ON su.s_nationkey = n.n_nationkey JOIN region AS r ON n.n_regionkey = r.r_regionkey JOIN (SELECT s_sub.s_i_id AS m_i_id, MIN(s_sub.s_quantity) AS m_s_quantity FROM stock AS s_sub JOIN supplier AS su_sub ON MOD((s_sub.s_w_id * s_sub.s_i_id), 10000) = su_sub.s_suppkey JOIN nation AS n_sub ON su_sub.s_nationkey = n_sub.n_nationkey JOIN region AS r_sub ON n_sub.n_regionkey = r_sub.r_regionkey WHERE r_sub.r_name LIKE 'EUROP%' GROUP BY s_sub.s_i_id) AS m ON i.i_id = m.m_i_id AND s.s_quantity = m.m_s_quantity WHERE i.i_data LIKE '%b' AND r.r_name LIKE 'EUROP%' ORDER BY n.n_name, su.s_name, i.i_id
"""

original_table_columns = {
    'order_line': ['ol_o_id', 'ol_d_id', 'ol_w_id', 'ol_number', 'ol_i_id', 
                   'ol_supply_w_id', 'ol_delivery_d', 'ol_quantity', 'ol_amount', 'ol_dist_info']
}

subtable_columns_1 = {
    'order_line_part1': ['ol_d_id', 'ol_w_id', 'ol_number', 'ol_i_id', 'ol_supply_w_id']
}

subtable_columns_2 = {
    'order_line_part2': ['ol_i_id', 'ol_delivery_d', 'ol_quantity', 'ol_amount', 'ol_dist_info']
}

primary_key = 'ol_i_id'

# 工具函数：提取SELECT语句中的列名和表名
def extract_all_select_from_recursive(sql):
    pattern = r"SELECT\s+(?:.*?\(.*?\))*.*?\s+FROM\s+([a-zA-Z0-9_]+|\(.*?\))"
    matches = re.finditer(pattern, sql, re.IGNORECASE | re.DOTALL)

    results = []
    for match in matches:
        columns_part = match.group(0).split("FROM")[0].replace("SELECT", "").strip()
        columns = [col.strip() for col in columns_part.split(",")]
        table_name = match.group(1).strip()

        # 如果表名是子查询，递归提取
        if table_name.startswith("(") and table_name.endswith(")"):
            subquery = table_name[1:-1].strip()
            nested_results = extract_all_select_from_recursive(subquery)
            results.extend(nested_results)

        results.append((columns, table_name))

    return results

# 工具函数：提取SELECT语句中的列名和表名
def extract_select_columns(sql):
    # 改进正则表达式，匹配更多格式（允许换行、空格等）
    match = re.search(r"SELECT\s+(.*?)\s+FROM\s+([a-zA-Z0-9_]+)", sql, re.IGNORECASE | re.DOTALL)
    
    if not match:
        print("未能匹配到SELECT和FROM部分")
        return None, None
    
    columns = match.group(1).split(',')
    columns = [col.strip() for col in columns]
    table_name = match.group(2)
    
    return columns, table_name

# 工具函数：处理SQL中的WHERE、GROUP BY和ORDER BY等子句
def extract_clauses(sql):
    where_clause = re.search(r"WHERE\s+(.*?)(?=\s+GROUP BY|\s+ORDER BY|$)", sql, re.IGNORECASE | re.DOTALL)
    group_by_clause = re.search(r"GROUP BY\s+(.*?)(?=\s+ORDER BY|$)", sql, re.IGNORECASE | re.DOTALL)
    order_by_clause = re.search(r"ORDER BY\s+(.*?)(?=$)", sql, re.IGNORECASE | re.DOTALL)
    
    return where_clause, group_by_clause, order_by_clause

# 主函数：自动生成重写后的SQL
def rewrite_sql(original_sql, original_table_columns, subtable_columns_1, subtable_columns_2, primary_key):
    columns, table_name = extract_select_columns(original_sql)
    print("columns:", columns)
    print("table_name:", table_name)
    if not columns or not table_name:
        return "无法解析原始SQL"
    
    # 确定需要从哪些子表获取哪些列
    selected_columns = {
        'order_line_part1': [],
        'order_line_part2': []
    }
    
    for column in columns:
        if column in subtable_columns_1['order_line_part1']:
            selected_columns['order_line_part1'].append(column)
        elif column in subtable_columns_2['order_line_part2']:
            selected_columns['order_line_part2'].append(column)
        else:
            return f"列 '{column}' 不在拆分后的表中"
    
    # 提取WHERE、GROUP BY和ORDER BY子句
    where_clause, group_by_clause, order_by_clause = extract_clauses(original_sql)
    
    # 构建新的SQL
    sql_parts = []
    
    if selected_columns['order_line_part1']:
        sql_parts.append(f"SELECT {', '.join(selected_columns['order_line_part1'])} FROM order_line_part1")
    
    if selected_columns['order_line_part2']:
        if sql_parts:
            sql_parts.append(f"JOIN order_line_part2 ON order_line_part1.{primary_key} = order_line_part2.{primary_key}")
        else:
            sql_parts.append(f"SELECT {', '.join(selected_columns['order_line_part2'])} FROM order_line_part2")
    
    # 添加WHERE子句
    if where_clause:
        sql_parts.append(f"WHERE {where_clause.group(1)}")
    
    # 添加GROUP BY子句
    if group_by_clause:
        sql_parts.append(f"GROUP BY {group_by_clause.group(1)}")
    
    # 添加ORDER BY子句
    if order_by_clause:
        sql_parts.append(f"ORDER BY {order_by_clause.group(1)}")

    # 返回重写后的SQL
    rewritten_sql = " ".join(sql_parts)
    return rewritten_sql

# # 调用函数生成重写后的SQL
# rewritten_sql = rewrite_sql(original_sql, original_table_columns, subtable_columns_1, subtable_columns_2, primary_key)
# print("重写后的SQL:", rewritten_sql)

print(extract_all_select_from_recursive(original_sql))