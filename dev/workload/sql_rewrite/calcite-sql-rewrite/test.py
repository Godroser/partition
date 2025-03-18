# from sqlalchemy.sql import text
from sqlparse import parse

def rewrite_sql(original_sql, original_table, primary_key, split_tables):
    """
    重写 SQL 语句以适应垂直分表
    :param original_sql: 原始 SQL 语句
    :param original_table: 原始表名
    :param primary_key: 原始表的主键
    :param split_tables: 拆分后的表结构 {表名: [列名列表]}
    :return: 改写后的 SQL 语句
    """
    parsed = parse(original_sql)
    
    # 构造新的 FROM 子句
    join_conditions = []
    select_fields = []
    new_from_clause = ""
    
    first_table = list(split_tables.keys())[0]
    new_from_clause = first_table
    
    for table, columns in split_tables.items():
        if table != first_table:
            join_conditions.append(f"JOIN {table} ON {first_table}.{primary_key} = {table}.{primary_key}")
    
    # 解析 SELECT 字段
    for statement in parsed:
        tokens = statement.tokens
        for token in tokens:
            if token.ttype is None and token.value.startswith("SELECT"):
                select_fields = token.value.split("SELECT ")[1].split(" FROM")[0].split(",")
                select_fields = [field.strip() for field in select_fields]
    
    # 重新映射字段
    mapped_fields = []
    for field in select_fields:
        for table, columns in split_tables.items():
            if field in columns:
                mapped_fields.append(f"{table}.{field}")
                break
    
    rewritten_sql = f"SELECT {', '.join(mapped_fields)} FROM {new_from_clause} {' '.join(join_conditions)}"
    return rewritten_sql

# 示例用法
original_sql = "SELECT id, name, age FROM users WHERE age > 18"
original_table = "users"
primary_key = "id"
split_tables = {
    "users_info": ["id", "name"],
    "users_detail": ["id", "age"]
}

rewritten_sql = rewrite_sql(original_sql, original_table, primary_key, split_tables)
print(rewritten_sql)
