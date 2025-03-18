import sqlparse

sql = "SELECT SUBSTR(c.c_state, 1, 1) AS country,COUNT(*) AS numcust, SUM(c.c_balance) AS totacctbal FROM customer c WHERE SUBSTR(c.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7') AND c.c_balance > (SELECT AVG(c2.c_balance) FROM customer c2 WHERE c2.c_balance > 0.00 AND SUBSTR(c2.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7'))AND NOT EXISTS (SELECT * FROM orders o WHERE o.o_c_id = c.c_id AND o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id) GROUP BY SUBSTR(c.c_state, 1, 1) ORDER BY SUBSTR(c.c_state, 1, 1);"
parsed = sqlparse.parse(sql)
for token in parsed[0].tokens:
    print(token.ttype, token.value)