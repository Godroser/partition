import re


def extract_columns(sql):
    # 移除注释
    sql = re.sub(r'(/\*([^*]|[\r\n]|(\*+([^*/]|[\r\n])))*\*+/)|(--.*)', '', sql)

    # 提取SELECT子句中的列
    select_pattern = r'SELECT\s+(.*?)\s+FROM'
    select_match = re.search(select_pattern, sql, re.IGNORECASE)
    select_columns = []
    if select_match:
        columns_str = select_match.group(1)
        # 只匹配字母和下划线组成的单词
        select_columns = re.findall(r'\b[A-Za-z_]+\b', columns_str)

    # 提取WHERE子句中的列
    where_pattern = r'WHERE\s+(.*)'
    where_match = re.search(where_pattern, sql, re.IGNORECASE)
    where_columns = []
    if where_match:
        conditions = where_match.group(1)
        # 只匹配字母和下划线组成的单词
        where_columns = re.findall(r'\b[A-Za-z_]+\b', conditions)

    # 合并所有列并去重
    all_columns = list(set(select_columns + where_columns))
    return all_columns


#sql = "explain analyze select ol_i_id from order_line where ol_o_id<10;"
sql = "SELECT ol.ol_quantity FROM order_line ol JOIN item i ON ol.ol_i_id = i.i_id WHERE ol.ol_o_id >= 1 AND ol.ol_o_id <= 2 AND i.i_price BETWEEN 1 AND 400000"
columns = extract_columns(sql)
print(columns)