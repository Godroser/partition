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
  projection_columns = set()
  predicate_columns = set()

  group_by_columns = set()
  order_by_columns = set()

  # 遍历解析后的 token
  in_select = False
  in_from = False
  in_where = False
  in_group_by = False
  in_order_by = False

  for token in parsed.tokens:

    # 忽略空白符和注释
    if token.is_whitespace or token.ttype in sqlparse.tokens.Comment:
      continue
    
    # 检查 SELECT 部分的列
    if token.ttype is sqlparse.tokens.DML and token.value.upper() == 'SELECT':
      in_select = True
      in_from = in_where = in_group_by = in_order_by = False
      continue
    elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'FROM':
      in_from = True
      in_select = in_where = in_group_by = in_order_by = False
      continue
    #elif token.ttype is sqlparse.tokens.Keyword and token.value.upper().startswith(" WHERE"): sb sqlparse居然不能把where识别成keyword
    elif token.value.upper().startswith("WHERE"):
      in_where = True
      in_select = in_from = in_group_by = in_order_by = False
    elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'GROUP BY':
      in_group_by = True
      in_select = in_from = in_where = in_order_by = False
      continue
    elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'ORDER BY':
      in_order_by = True
      in_select = in_from = in_where = in_group_by = False
      continue

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

    # 获取谓词条件列：包括 WHERE 子句、GROUP BY 和 ORDER BY 列
    # if in_where and isinstance(token, sqlparse.sql.Comparison):
    #   predicate_columns.add(str(token))  # 完整的条件语句
    #   print(str(token))


    if in_where:
      token = token.value.replace("where", "", 1).strip()
      predicate_columns.add(str(token)) 
      # tmp = token.value.replace("where", "", 1).strip() //无法识别Comparison
      # newtoken = sqlparse.sql.Token(token.ttype, tmp)
      # print("newtoken::::::", newtoken.ttype)
      # if isinstance(newtoken, sqlparse.sql.Comparison):
      #   print("add predicate")
      #   predicate_columns.add(str(token))  
      #   print(str(token))
      if in_where and isinstance(token, sqlparse.sql.Identifier):
        predicate_columns.add(str(token))  # 可能单纯的列名 
        #print(str(token))     

    # 获取 group by 列
    if in_group_by and isinstance(token, sqlparse.sql.Identifier):
      group_by_columns.add(str(token))
      #print(str(token))

    # 获取 order by 列
    if in_order_by and isinstance(token, sqlparse.sql.Identifier):
      order_by_columns.add(str(token))
      #print(str(token))

  # 合并不同部分的列信息
  #predicate_columns.update(group_by_columns)
  #predicate_columns.update(order_by_columns)

  return {
    'tables': list(tables),
    'projection_columns': list(projection_columns),
    'predicate_columns': list(predicate_columns),
    'order_by': list(order_by_columns),
    'group_by': list(group_by_columns)
  }


# 主函数：读取 SQL 文件并分析
if __name__ == '__main__':
  sql_statements = read_sql_file('workload/workload.bak.sql')

  cnt = 0
  for i, sql in enumerate(sql_statements, start=1):
    analysis = analyze_sql(sql)
    print(f"SQL Statement {i}:")
    #print(sql)
    print("Tables:", analysis['tables'])
    print("Projection Columns:", analysis['projection_columns'])
    print("Predicate Columns:", analysis['predicate_columns'])
    print("Order By Columns:", analysis['order_by'])
    print("Group By Columns:", analysis['group_by'])
    print("-" * 40)
    cnt += 1
    if cnt >= 22:
      break






  # # 示例 SQL 语句
  # sql = "SELECT name, age FROM users WHERE age > 25 AND city = 'New York'"

  # # 解析 SQL
  # parsed = sqlparse.parse(sql)

  # # 遍历解析后的 SQL 语句
  # for stmt in parsed:
  #     for token in stmt.tokens:
  #         print(token)
  #         # 查找 WHERE 子句
  #         if token.value.upper().startswith("WHERE"):
  #             # 提取 WHERE 后的条件部分
  #             condition = token.value[5:].strip()
  #             print("Condition extracted:", condition)
  