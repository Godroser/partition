import sqlparse
import re

# 读取 SQL 文件内容并分割为独立的 SQL 语句
def read_sql_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        sql_script = file.read()
    # 使用分号分割并去掉空白
    sql_statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
    return sql_statements

# 分析单条 SQL 语句
def analyze_sql(sql):
    parsed = sqlparse.parse(sql)[0]
    
    tables = set()
    join_tables = set()
    join_conditions = []
    projection_columns = set()
    predicate_columns = set()
    group_by_columns = set()
    order_by_columns = set()
    update_tables = set()
    update_columns = set()
    insert_tables = set()

    in_select = False
    in_from = False
    in_where = False
    in_group_by = False
    in_order_by = False
    in_join = False
    in_on = False
    in_update = False
    in_set = False
    in_insert_into = False

    for token in parsed.tokens:

        # 忽略空白符和注释
        if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
            continue
        
        # 检查 SELECT 部分的列
        if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
            in_select = True
            in_from = in_where = in_group_by = in_order_by = in_join = in_on = False
            continue
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
            in_from = True
            in_select = in_where = in_group_by = in_order_by = in_join = in_on = False
            continue
        elif token.value.upper().startswith("WHERE"):
            in_where = True
            in_select = in_from = in_group_by = in_order_by = in_join = in_on = False
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'GROUP BY':
            in_group_by = True
            in_select = in_from = in_where = in_order_by = in_join = in_on = False
            continue
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'ORDER BY':
            in_order_by = True
            in_select = in_from = in_where = in_group_by = in_join = in_on = False
            continue
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'JOIN':
            in_join = True
            in_select = in_from = in_where = in_group_by = in_order_by = in_on = False
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'ON':
            in_on = True
            in_join = False
        elif token.ttype is sqlparse.tokens.DML and token.value.upper() == 'UPDATE':
            in_update = True
            in_select = in_from = in_where = in_group_by = in_order_by = in_join = in_on = False
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'SET':
            in_set = True
            in_update = False
        elif token.ttype is sqlparse.tokens.DML and token.value.upper() == 'INSERT':
            in_insert_into = True
            in_select = in_from = in_where = in_group_by = in_order_by = in_join = in_on = False
        elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'INTO':
            in_insert_into = False

        # 获取投影列：包括普通列和聚合函数列（如 sum(ol_quantity)）
        if in_select:
            if isinstance(token, sqlparse.sql.IdentifierList):
                for identifier in token.get_identifiers():
                    projection_columns.add(str(identifier))  # 包括聚合函数等
            elif isinstance(token, sqlparse.sql.Identifier):
                projection_columns.add(str(token))  # 处理简单的列名
            elif isinstance(token, sqlparse.sql.Function):
                projection_columns.add(str(token))  # 处理聚合函数，如 sum(), avg()

        # 获取表名
        if in_from and isinstance(token, sqlparse.sql.Identifier):
            tables.add(token.get_real_name())

        # 获取 JOIN 表名和条件
        if in_join and isinstance(token, sqlparse.sql.Identifier):
            join_tables.add(token.get_real_name())
        elif in_on:
            join_conditions.append(str(token))

        # 获取谓词条件列：包括 WHERE 子句、GROUP BY 和 ORDER BY 列
        if in_where:
            predicate_columns.add(str(token))

        # 获取 group by 列
        if in_group_by and isinstance(token, sqlparse.sql.Identifier):
            group_by_columns.add(str(token))

        # 获取 order by 列
        if in_order_by and isinstance(token, sqlparse.sql.Identifier):
            order_by_columns.add(str(token))

        # 获取 UPDATE 表名和列
        if in_update and isinstance(token, sqlparse.sql.Identifier):
            update_tables.add(token.get_real_name())
        elif in_set and isinstance(token, sqlparse.sql.Assignment):
            update_columns.add(str(token.left))

        # 获取 INSERT INTO 表名
        if in_insert_into and isinstance(token, sqlparse.sql.Identifier):
            insert_tables.add(token.get_real_name())

    return {
        'tables': list(tables),
        'join_tables': list(join_tables),
        'join_conditions': join_conditions,
        'projection_columns': list(projection_columns),
        'predicate_columns': list(predicate_columns),
        'order_by': list(order_by_columns),
        'group_by': list(group_by_columns),
        'update_tables': list(update_tables),
        'update_columns': list(update_columns),
        'insert_tables': list(insert_tables)
    }

# 主函数：读取 SQL 文件并分析
if __name__ == '__main__':
    sql_statements = read_sql_file('workload/workload.bak.sql')

    cnt = 0
    for i, sql in enumerate(sql_statements, start=1):
        analysis = analyze_sql(sql)
        print(f"SQL Statement {i}:")
        print("Tables:", analysis['tables'])
        print("Join Tables:", analysis['join_tables'])
        print("Join Conditions:", analysis['join_conditions'])
        print("Projection Columns:", analysis['projection_columns'])
        print("Predicate Columns:", analysis['predicate_columns'])
        print("Order By Columns:", analysis['order_by'])
        print("Group By Columns:", analysis['group_by'])
        print("Update Tables:", analysis['update_tables'])
        print("Update Columns:", analysis['update_columns'])
        print("Insert Tables:", analysis['insert_tables'])
        print("-" * 40)
        cnt += 1
        if cnt >= 22:
            break