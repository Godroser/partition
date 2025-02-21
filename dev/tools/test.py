import sqlparse


def extract_column_names(sql):
    parsed = sqlparse.parse(sql)[0]
    column_names = []

    def traverse(token):
        if isinstance(token, sqlparse.sql.Identifier):
            parts = str(token).split('.')
            if len(parts) > 1:
                column_names.append(parts[-1])
            else:
                column_names.append(str(token))
        elif isinstance(token, sqlparse.sql.IdentifierList):
            for identifier in token.get_identifiers():
                traverse(identifier)
        elif isinstance(token, sqlparse.sql.Statement):
            for sub_token in token.tokens:
                traverse(sub_token)
        elif isinstance(token, sqlparse.sql.Function):
            for param in token.tokens:
                if isinstance(param, sqlparse.sql.Identifier):
                    traverse(param)

    traverse(parsed)
    unique_column_names = list(set(column_names))
    return unique_column_names


sql = """
SELECT su.s_suppkey, su.s_name, n.n_name, i.i_id, i.i_name, su.s_address, su.s_phone, su.s_comment 
FROM item AS i 
JOIN stock AS s ON i.i_id = s.s_i_id 
JOIN supplier AS su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey 
JOIN nation AS n ON su.s_nationkey = n.n_nationkey 
JOIN region AS r ON n.n_regionkey = r.r_regionkey 
JOIN (
    SELECT s_sub.s_i_id AS m_i_id, MIN(s_sub.s_quantity) AS m_s_quantity 
    FROM stock AS s_sub 
    JOIN supplier AS su_sub ON MOD((s_sub.s_w_id * s_sub.s_i_id), 10000) = su_sub.s_suppkey 
    JOIN nation AS n_sub ON su_sub.s_nationkey = n_sub.n_nationkey 
    JOIN region AS r_sub ON n_sub.n_regionkey = r_sub.r_regionkey 
    WHERE r_sub.r_name LIKE 'EUROP%' 
    GROUP BY s_sub.s_i_id
) AS m ON i.i_id = m.m_i_id AND s.s_quantity = m.m_s_quantity 
WHERE i.i_data LIKE '%b' AND r.r_name LIKE 'EUROP%' 
ORDER BY n.n_name, su.s_name, i.i_id
"""

columns = extract_column_names(sql)
print("SQL 语句中出现的列名：")
for column in columns:
    print(column)