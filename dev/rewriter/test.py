import sqlglot

# 定义一个复杂的 SQL 语句
# complex_sql = """
# SELECT 
#     customers.customer_name, 
#     SUM(orders.order_amount) as total_amount
# FROM 
#     customers
# JOIN 
#     orders ON customers.customer_id = orders.customer_id
# WHERE 
#     customers.country = 'USA'
#     AND orders.order_date BETWEEN '2023-01-01' AND '2023-12-31'
# GROUP BY 
#     customers.customer_name
# HAVING 
#     total_amount > 1000
# ORDER BY 
#     total_amount DESC;
# """
complex_sql = """
SELECT su.s_suppkey, su.s_name, n.n_name, i.i_id, i.i_name, su.s_address, su.s_phone, su.s_comment FROM item AS i JOIN stock AS s ON i.i_id = s.s_i_id JOIN supplier AS su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey JOIN nation AS n ON su.s_nationkey = n.n_nationkey JOIN region AS r ON n.n_regionkey = r.r_regionkey JOIN (SELECT s_sub.s_i_id AS m_i_id, MIN(s_sub.s_quantity) AS m_s_quantity FROM stock AS s_sub JOIN supplier AS su_sub ON MOD((s_sub.s_w_id * s_sub.s_i_id), 10000) = su_sub.s_suppkey JOIN nation AS n_sub ON su_sub.s_nationkey = n_sub.n_nationkey JOIN region AS r_sub ON n_sub.n_regionkey = r_sub.r_regionkey WHERE r_sub.r_name LIKE 'EUROP%' GROUP BY s_sub.s_i_id) AS m ON i.i_id = m.m_i_id AND s.s_quantity = m.m_s_quantity WHERE i.i_data LIKE '%b' AND r.r_name LIKE 'EUROP%' ORDER BY n.n_name, su.s_name, i.i_id
"""

try:
    # 解析 SQL 语句
    parsed = sqlglot.parse_one(complex_sql)

    # 打印解析后的 AST（抽象语法树）
    print("解析后的 AST:")
    print(parsed)

    # 获取 SELECT 子句中的列名（不包含别名）
    select_columns = []
    for expression in parsed.find(sqlglot.exp.Select).expressions:
        if isinstance(expression, sqlglot.exp.Alias):
            # 如果是带别名的表达式，获取其原始表达式的名称
            original_expr = expression.this
            if isinstance(original_expr, sqlglot.exp.Column):
                select_columns.append(original_expr.name)
            elif isinstance(original_expr, sqlglot.exp.Func):
                # 处理函数表达式，例如 SUM(orders.order_amount)
                if original_expr.expressions:
                    first_arg = original_expr.expressions[0]
                    if isinstance(first_arg, sqlglot.exp.Column):
                        select_columns.append(first_arg.name)
        elif isinstance(expression, sqlglot.exp.Column):
            # 直接是列的情况
            select_columns.append(expression.name)

    print("\nSELECT 子句中的列名（不包含别名）:")
    print(select_columns)

    # 获取 JOIN 子句中的表名
    join_tables = []
    for join in parsed.find_all(sqlglot.exp.Join):
        join_tables.append(join.this.name)
    print("\nJOIN 子句中的表名:")
    print(join_tables)

    # 获取 WHERE 子句中的条件
    where_condition = parsed.find(sqlglot.exp.Where)
    if where_condition:
        print("\nWHERE 子句中的条件:")
        print(where_condition.this)
    else:
        print("\n没有 WHERE 子句。")

    # 格式化 SQL 语句
    formatted_sql = parsed.sql(pretty=True)
    print("\n格式化后的 SQL 语句:")
    print(formatted_sql)

except sqlglot.errors.ParseError as e:
    print(f"解析 SQL 语句时出错: {e}")