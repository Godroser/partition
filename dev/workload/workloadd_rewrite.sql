-- 1
SELECT olp1.ol_number, SUM(olp1.ol_quantity) AS sum_qty, SUM(olp2.ol_amount) AS sum_amount, AVG(olp1.ol_quantity) AS avg_qty, AVG(olp2.ol_amount) AS avg_amount, COUNT(*) AS count_order
FROM order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_o_id = olp2.ol_o_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_w_id = olp2.ol_w_id AND olp1.ol_number = olp2.ol_number
WHERE olp2.ol_delivery_d > '2024-10-28 17:00:00'
GROUP BY olp1.ol_number
ORDER BY olp1.ol_number;

-- 2
SELECT su.s_suppkey, su.s_name, np2.n_name, ip1.i_id, ip1.i_name, su.s_address, su.s_phone, su.s_comment
FROM item_part1 ip1
JOIN stock_part1 sp1 ON ip1.i_id = sp1.s_i_id
JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
JOIN supplier su ON MOD((sp1.s_w_id * sp1.s_i_id), 10000) = su.s_suppkey
JOIN nation_part1 np1 ON su.s_nationkey = np1.n_nationkey
JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
JOIN region r ON np1.n_regionkey = r.r_regionkey
JOIN (SELECT s_sub.s_i_id AS m_i_id, MIN(s_sub.s_quantity) AS m_s_quantity
      FROM stock_part1 s_sub
      JOIN supplier su_sub ON MOD((s_sub.s_w_id * s_sub.s_i_id), 10000) = su_sub.s_suppkey
      JOIN nation_part1 np_sub ON su_sub.s_nationkey = np_sub.n_nationkey
      JOIN region r_sub ON np_sub.n_regionkey = r_sub.r_regionkey
      WHERE r_sub.r_name LIKE 'EUROP%'
      GROUP BY s_sub.s_i_id) AS m ON ip1.i_id = m.m_i_id AND sp1.s_quantity = m.m_s_quantity
WHERE ip2.i_data LIKE '%b' AND r.r_name LIKE 'EUROP%'
ORDER BY np2.n_name, su.s_name, ip1.i_id;

-- 3
SELECT olp2.ol_o_id, olp2.ol_w_id, olp2.ol_d_id, SUM(olp2.ol_amount) AS revenue, op2.o_entry_d
FROM customer_part1 cp1
JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_d_id = cp2.c_d_id AND cp1.c_w_id = cp2.c_w_id
JOIN orders_part2 op2 ON op2.o_id = op2.o_id AND cp1.c_id = op2.o_c_id AND cp1.c_w_id = op2.o_w_id AND cp1.c_d_id = op2.o_d_id
JOIN new_order no ON op2.o_id = no.no_o_id AND op2.o_w_id = no.no_w_id AND op2.o_d_id = no.no_d_id
JOIN order_line_part2 olp2 ON op2.o_id = olp2.ol_o_id AND op2.o_w_id = olp2.ol_w_id AND op2.o_d_id = olp2.ol_d_id
WHERE cp2.c_state LIKE 'A%' AND op2.o_entry_d > '2024-10-28 17:00:00'
GROUP BY olp2.ol_o_id, olp2.ol_w_id, olp2.ol_d_id, op2.o_entry_d
ORDER BY revenue DESC, op2.o_entry_d;

-- 4
SELECT op2.o_ol_cnt, COUNT(*) AS order_count
FROM orders_part2 op2
JOIN order_line_part2 olp2 ON op2.o_id = olp2.ol_o_id AND op2.o_w_id = olp2.ol_w_id AND op2.o_d_id = olp2.ol_d_id
WHERE op2.o_entry_d >= '2024-10-28 17:00:00' AND op2.o_entry_d < '2025-10-23 17:00:00'
GROUP BY op2.o_ol_cnt
ORDER BY op2.o_ol_cnt;

-- 5
select n_name,  sum(ol_amount) as revenue from customer_part2 cp2, orders_part2 op2, order_line_part2 olp2, stock_part2 stp2, supplier, nation_part2 np2, nation_part1 np1, region where cp2.c_id = op2.o_c_id  and cp2.c_w_id = op2.o_w_id  and cp2.c_d_id = op2.o_d_id  and olp2.ol_o_id = op2.o_id  and olp2.ol_w_id = op2.o_w_id  and olp2.ol_d_id=op2.o_d_id  and olp2.ol_w_id = stp2.s_w_id  and olp2.ol_i_id = stp2.s_i_id  and mod((stp2.s_w_id * stp2.s_i_id),10000) = s_suppkey  and ascii(substr(cp2.c_state,1,1)) = s_nationkey  and s_nationkey = np2.n_nationkey  and np1.n_regionkey = r_regionkey and np1.n_nationkey = np2.n_nationkey and r_name = 'EUROPE'  and op2.o_entry_d >= '2024-10-28 17:00:00' group by np2.n_name order by revenue desc;

-- 6.
SELECT 
    SUM(olp2.ol_amount) AS revenue 
FROM 
    order_line_part1 olp1
JOIN 
    order_line_part2 olp2 ON olp1.ol_o_id = olp2.ol_o_id AND olp1.ol_w_id = olp2.ol_w_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_number = olp2.ol_number
WHERE 
    olp2.ol_delivery_d >= '2024-10-23 17:00:00' 
    AND olp2.ol_delivery_d < '2024-10-28 17:00:00' 
    AND olp1.ol_quantity BETWEEN 1 AND 100000;

-- 7
SELECT su.s_nationkey AS supp_nation, SUBSTR(cp2.c_state, 1, 1) AS cust_nation, EXTRACT(YEAR FROM op2.o_entry_d) AS l_year, SUM(olp2.ol_amount) AS revenue
FROM order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_o_id = olp2.ol_o_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_w_id = olp2.ol_w_id AND olp1.ol_number = olp2.ol_number
JOIN stock_part1 sp1 ON olp1.ol_supply_w_id = sp1.s_w_id AND olp2.ol_i_id = sp1.s_i_id
JOIN orders_part1 op1 ON olp1.ol_o_id = op1.o_id
JOIN orders_part2 op2 ON op1.o_id = op2.o_id AND olp1.ol_d_id = op2.o_d_id AND olp1.ol_w_id = op2.o_w_id
JOIN customer_part1 cp1 ON op2.o_c_id = cp1.c_id AND op2.o_w_id = cp1.c_w_id AND op2.o_d_id = cp1.c_d_id
JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_d_id = cp2.c_d_id AND cp1.c_w_id = cp2.c_w_id
JOIN supplier su ON MOD((sp1.s_w_id * sp1.s_i_id), 10000) = su.s_suppkey
JOIN nation_part2 np22 ON su.s_nationkey = np22.n_nationkey
JOIN nation_part2 np2 ON ASCII(SUBSTR(cp2.c_state, 1, 1)) = np2.n_nationkey
WHERE MOD((sp1.s_w_id * sp1.s_i_id), 10000) = su.s_suppkey AND ((np22.n_name = 'GERMANY' AND np2.n_name = 'CANADA') OR (np22.n_name = 'CANADA' AND np2.n_name = 'GERMANY')) AND op2.o_entry_d BETWEEN '2024-10-28 17:00:00' AND '2025-10-23 17:00:00'
GROUP BY su.s_nationkey, SUBSTR(cp2.c_state, 1, 1), EXTRACT(YEAR FROM op2.o_entry_d)
ORDER BY su.s_nationkey, cust_nation, l_year;

-- 8
SELECT 
    EXTRACT(YEAR FROM o2.o_entry_d) AS l_year,
    SUM(CASE WHEN n2p2.n_name = 'GERMANY' THEN olp2.ol_amount ELSE 0 END) / SUM(olp2.ol_amount) AS mkt_share
FROM 
    item_part1 ip1, 
    item_part2 ip2, 
    supplier, 
    stock_part1 sp1, 
    stock_part2 sp2, 
    order_line_part1 olp1, 
    order_line_part2 olp2, 
    orders_part1 op1, 
    orders_part2 o2, 
    customer_part1 cp1, 
    customer_part2 cp2, 
    nation_part1 np1, 
    nation_part2 np2, 
    nation_part2 n2p2, 
    region
WHERE 
    -- item表关联
    ip1.i_id = ip2.i_id
    AND ip1.i_id = sp1.s_i_id
    AND ip1.i_id = olp2.ol_i_id
    -- supplier和stock关联
    AND sp1.s_i_id = sp2.s_i_id
    AND sp1.s_w_id = sp2.s_w_id
    AND olp1.ol_supply_w_id = sp1.s_w_id
    AND MOD((sp1.s_w_id * sp1.s_i_id), 10000) = s_suppkey
    -- order_line和orders关联
    AND olp1.ol_w_id = o2.o_w_id
    AND olp1.ol_d_id = o2.o_d_id
    AND olp1.ol_o_id = op1.o_id
    AND olp2.ol_w_id = o2.o_w_id
    AND olp2.ol_d_id = o2.o_d_id
    AND olp2.ol_o_id = op1.o_id
    -- orders和customer关联
    AND op1.o_id = o2.o_id
    AND o2.o_c_id = cp1.c_id
    AND o2.o_w_id = cp1.c_w_id
    AND o2.o_d_id = cp1.c_d_id
    AND cp1.c_id = cp2.c_id
    AND cp1.c_w_id = cp2.c_w_id
    AND cp1.c_d_id = cp2.c_d_id
    -- nation和customer关联
    AND np1.n_nationkey = ASCII(SUBSTR(cp2.c_state, 1, 1))
    AND np1.n_regionkey = r_regionkey
    -- 其他条件
    AND olp2.ol_i_id < 1000
    AND r_name = 'EUROPE'
    AND s_nationkey = n2p2.n_nationkey
    AND o2.o_entry_d BETWEEN '2024 - 10 - 23 17:00:00' AND '2024 - 10 - 28 17:00:00'
    AND ip2.i_data LIKE '%b'
GROUP BY 
    EXTRACT(YEAR FROM o2.o_entry_d)
ORDER BY 
    l_year;

-- 9
SELECT 
    np2.n_name,
    EXTRACT(YEAR FROM o2.o_entry_d) AS l_year,
    SUM(olp2.ol_amount) AS sum_profit
FROM 
    item_part1 ip1
JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
JOIN stock_part1 sp1 ON ip1.i_id = sp1.s_i_id
JOIN stock_part2 sp2 ON sp1.s_i_id = sp2.s_i_id AND sp1.s_w_id = sp2.s_w_id
JOIN supplier ON MOD((sp1.s_w_id * sp1.s_i_id), 10000) = s_suppkey 
JOIN order_line_part1 olp1 ON sp1.s_w_id = olp1.ol_supply_w_id
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_o_id = olp2.ol_o_id AND ip1.i_id = olp2.ol_i_id
JOIN orders_part1 op1 ON olp1.ol_o_id = op1.o_id
JOIN orders_part2 o2 ON op1.o_id = o2.o_id
JOIN nation_part1 np1 ON s_nationkey = np1.n_nationkey
JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
WHERE 
    ip2.i_data LIKE '%BB'
GROUP BY 
    np2.n_name, EXTRACT(YEAR FROM o2.o_entry_d)
ORDER BY 
    np2.n_name, l_year DESC;

-- 10
SELECT 
    cp1.c_id,
    cp1.c_last,
    SUM(olp2.ol_amount) AS revenue,
    cp1.c_city,
    cp1.c_phone,
    np2.n_name
FROM 
    customer_part1 cp1
JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_w_id = cp2.c_w_id AND cp1.c_d_id = cp2.c_d_id
-- 修正orders_part1和orders_part2的连接
JOIN orders_part2 op2 ON cp1.c_id = op2.o_c_id 
    AND cp1.c_w_id = op2.o_w_id 
    AND cp1.c_d_id = op2.o_d_id
-- 修正order_line_part1与orders相关表的连接
JOIN order_line_part1 olp1 ON op2.o_w_id = olp1.ol_w_id 
    AND op2.o_d_id = olp1.ol_d_id 
    AND op2.o_id = olp1.ol_o_id
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
    AND olp1.ol_d_id = olp2.ol_d_id 
    AND olp1.ol_o_id = olp2.ol_o_id
JOIN nation_part1 np1 ON np1.n_nationkey = ASCII(SUBSTR(cp2.c_state, 1, 1))
JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
WHERE 
    op2.o_entry_d >= '2024 - 10 - 28 17:00:00'
    AND op2.o_entry_d <= olp2.ol_delivery_d
GROUP BY 
    cp1.c_id, cp1.c_last, cp1.c_city, cp1.c_phone, np2.n_name
ORDER BY 
    revenue DESC;

-- 11
WITH subquery AS (
    SELECT 
        sp1.s_i_id,
        SUM(sp1.s_order_cnt) AS ordercount
    FROM 
        stock_part1 sp1
    JOIN stock_part2 sp2 ON sp1.s_i_id = sp2.s_i_id AND sp1.s_w_id = sp2.s_w_id
    JOIN supplier ON MOD((sp1.s_w_id * sp1.s_i_id), 10000) = s_suppkey
    JOIN nation_part1 np1 ON s_nationkey = np1.n_nationkey
    JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
    WHERE 
        np2.n_name = 'GERMANY'
    GROUP BY 
        sp1.s_i_id
)
SELECT 
    s_i_id,
    ordercount
FROM 
    subquery
WHERE 
    ordercount > (
        SELECT 
            SUM(sp1.s_order_cnt) * 0.005
        FROM 
            stock_part1 sp1
        JOIN stock_part2 sp2 ON sp1.s_i_id = sp2.s_i_id AND sp1.s_w_id = sp2.s_w_id
        JOIN supplier ON MOD((sp1.s_w_id * sp1.s_i_id), 10000) = s_suppkey
        JOIN nation_part1 np1 ON s_nationkey = np1.n_nationkey
        JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
        WHERE 
            np2.n_name = 'GERMANY'
    )
ORDER BY 
    ordercount DESC;

-- 12
SELECT 
    op2.o_ol_cnt,
    SUM(CASE WHEN op1.o_carrier_id = 1 OR op1.o_carrier_id = 2 THEN 1 ELSE 0 END) AS high_line_count,
    SUM(CASE WHEN op1.o_carrier_id <> 1 AND op1.o_carrier_id <> 2 THEN 1 ELSE 0 END) AS low_line_count
FROM 
    orders_part1 op1
JOIN orders_part2 op2 ON op1.o_id = op2.o_id
JOIN order_line_part1 olp1 ON op2.o_w_id = olp1.ol_w_id AND op2.o_d_id = olp1.ol_d_id AND op1.o_id = olp1.ol_o_id
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_o_id = olp2.ol_o_id
WHERE 
    op2.o_entry_d <= olp2.ol_delivery_d
    AND olp2.ol_delivery_d < '2025 - 10 - 23 17:00:00'
GROUP BY 
    op2.o_ol_cnt
ORDER BY 
    op2.o_ol_cnt;

-- 13
WITH c_orders AS (
    SELECT 
        cp1.c_id,
        COUNT(op1.o_id) AS c_count
    FROM 
        customer_part1 cp1
    JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_w_id = cp2.c_w_id AND cp1.c_d_id = cp2.c_d_id
    LEFT JOIN orders_part2 op2 ON cp1.c_id = op2.o_c_id AND cp1.c_w_id = op2.o_w_id AND cp1.c_d_id = op2.o_d_id
    LEFT JOIN orders_part1 op1 ON op1.o_id = op2.o_id WHERE op1.o_carrier_id > 8
    GROUP BY 
        cp1.c_id
)
SELECT 
    c_count,
    COUNT(*) AS custdist
FROM 
    c_orders
GROUP BY 
    c_count
ORDER BY 
    custdist DESC, c_count DESC;

-- 14
SELECT 
    100.00 * SUM(CASE WHEN ip2.i_data LIKE 'PR%' THEN olp2.ol_amount ELSE 0 END) / NULLIF(SUM(olp2.ol_amount), 0) AS promo_revenue
FROM 
    order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
    AND olp1.ol_d_id = olp2.ol_d_id 
    AND olp1.ol_o_id = olp2.ol_o_id 
    AND olp1.ol_number = olp2.ol_number
JOIN item_part1 ip1 ON olp2.ol_i_id = ip1.i_id
JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
WHERE 
    olp2.ol_delivery_d >= '2024 - 10 - 28 17:00:00'
    AND olp2.ol_delivery_d < '2025 - 10 - 23 17:00:00';

-- 15
WITH revenue AS (
    SELECT 
        MOD((sp1.s_w_id * sp1.s_i_id), 10000) AS supplier_no,
        SUM(olp2.ol_amount) AS total_revenue
    FROM 
        order_line_part1 olp1
    JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
        AND olp1.ol_d_id = olp2.ol_d_id 
        AND olp1.ol_o_id = olp2.ol_o_id 
        AND olp1.ol_number = olp2.ol_number
    JOIN stock_part1 sp1 ON olp2.ol_i_id = sp1.s_i_id AND olp1.ol_supply_w_id = sp1.s_w_id
    JOIN stock_part2 sp2 ON sp1.s_i_id = sp2.s_i_id AND sp1.s_w_id = sp2.s_w_id
    WHERE 
        olp2.ol_delivery_d >= '2024 - 10 - 28 17:00:00'
    GROUP BY 
        MOD((sp1.s_w_id * sp1.s_i_id), 10000)
)
SELECT 
    s.s_suppkey, 
    s.s_name, 
    s.s_address, 
    s.s_phone, 
    r.total_revenue
FROM 
    supplier s
JOIN revenue r ON s.s_suppkey = r.supplier_no
WHERE 
    r.total_revenue = (SELECT MAX(total_revenue) FROM revenue)
ORDER BY 
    s.s_suppkey;

-- 16
SELECT 
    ip1.i_name,
    SUBSTR(ip2.i_data, 1, 3) AS brand,
    ip1.i_price,
    COUNT(DISTINCT MOD((sp1.s_w_id * sp1.s_i_id), 10000)) AS supplier_cnt
FROM 
    stock_part1 sp1
JOIN stock_part2 sp2 ON sp1.s_i_id = sp2.s_i_id AND sp1.s_w_id = sp2.s_w_id
JOIN item_part1 ip1 ON sp1.s_i_id = ip1.i_id
JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
WHERE 
    ip2.i_data NOT LIKE 'zz%'
    AND MOD((sp1.s_w_id * sp1.s_i_id), 10000) NOT IN (
        SELECT s.s_suppkey 
        FROM supplier s 
        WHERE s.s_comment LIKE '%bad%'
    )
GROUP BY 
    ip1.i_name, 
    SUBSTR(ip2.i_data, 1, 3), 
    ip1.i_price
ORDER BY 
    supplier_cnt DESC;

-- 17
WITH subquery AS (
    SELECT 
        ip1.i_id, 
        AVG(olp1.ol_quantity) AS a
    FROM 
        order_line_part1 olp1
    JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
        AND olp1.ol_d_id = olp2.ol_d_id 
        AND olp1.ol_o_id = olp2.ol_o_id 
        AND olp1.ol_number = olp2.ol_number
    JOIN item_part1 ip1 ON olp2.ol_i_id = ip1.i_id
    JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
    GROUP BY 
        ip1.i_id
)
SELECT 
    SUM(olp2.ol_amount) / 2.0 AS avg_yearly
FROM 
    order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
    AND olp1.ol_d_id = olp2.ol_d_id 
    AND olp1.ol_o_id = olp2.ol_o_id 
    AND olp1.ol_number = olp2.ol_number
JOIN subquery t ON olp2.ol_i_id = t.i_id
WHERE 
    olp1.ol_quantity < t.a;

-- 18
SELECT 
    cp1.c_last,
    o2.o_id,
    o2.o_entry_d,
    o2.o_ol_cnt,
    SUM(olp2.ol_amount)
FROM 
    customer_part1 cp1
JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_w_id = cp2.c_w_id AND cp1.c_d_id = cp2.c_d_id
JOIN orders_part2 o2 ON cp1.c_id = o2.o_c_id AND cp1.c_w_id = o2.o_w_id AND cp1.c_d_id = o2.o_d_id
JOIN order_line_part2 olp2 ON o2.o_w_id = olp2.ol_w_id AND o2.o_d_id = olp2.ol_d_id AND o2.o_id = olp2.ol_o_id
GROUP BY 
    o2.o_id, o2.o_w_id, o2.o_d_id, cp1.c_id, cp1.c_last, o2.o_entry_d, o2.o_ol_cnt
HAVING 
    SUM(olp2.ol_amount) > 200
ORDER BY 
    SUM(olp2.ol_amount) DESC, o2.o_entry_d;

-- 19
SELECT 
    SUM(olp2.ol_amount) AS revenue
FROM 
    order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_w_id = olp2.ol_w_id 
    AND olp1.ol_d_id = olp2.ol_d_id 
    AND olp1.ol_o_id = olp2.ol_o_id 
    AND olp1.ol_number = olp2.ol_number
JOIN item_part1 ip1 ON olp2.ol_i_id = ip1.i_id
JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
WHERE 
    olp1.ol_quantity >= 1 
    AND olp1.ol_quantity <= 10 
    AND ip1.i_price BETWEEN 1 AND 400000 
    AND (
        (ip2.i_data LIKE '%a' AND olp1.ol_w_id IN (1, 2, 3)) 
        OR (ip2.i_data LIKE '%b' AND olp1.ol_w_id IN (1, 2, 4)) 
        OR (ip2.i_data LIKE '%c' AND olp1.ol_w_id IN (1, 5, 3))
    );

-- 20
WITH subquery AS (
    SELECT 
        MOD(sp1.s_i_id * sp1.s_w_id, 10000) AS supplier_key
    FROM 
        stock_part1 sp1
    JOIN order_line_part2 olp2 ON sp1.s_i_id = olp2.ol_i_id
    JOIN order_line_part1 olp1 ON olp1.ol_w_id = olp2.ol_w_id 
        AND olp1.ol_d_id = olp2.ol_d_id 
        AND olp1.ol_o_id = olp2.ol_o_id 
        AND olp1.ol_number = olp2.ol_number
    JOIN item_part1 ip1 ON sp1.s_i_id = ip1.i_id
    JOIN item_part2 ip2 ON ip1.i_id = ip2.i_id
    WHERE 
        ip2.i_data LIKE 'co%'
        AND olp2.ol_delivery_d > '2024-10-28 17:00:00'
    GROUP BY 
        sp1.s_i_id, sp1.s_w_id, sp1.s_quantity
    HAVING 
        2 * sp1.s_quantity > SUM(olp1.ol_quantity)
)
SELECT 
    s.s_name, 
    s.s_address
FROM 
    supplier s
JOIN nation_part1 np1 ON s.s_nationkey = np1.n_nationkey
JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
WHERE 
    s.s_suppkey IN (SELECT supplier_key FROM subquery)
    AND np2.n_name = 'GERMANY'
ORDER BY 
    s.s_name;

-- 21
WITH filtered_order_lines AS (
    SELECT 
        l1p2.ol_o_id, 
        l1p2.ol_w_id, 
        l1p2.ol_d_id, 
        l1p2.ol_delivery_d
    FROM 
        order_line_part2 l1p2
)
SELECT 
    s.s_name, 
    COUNT(*) AS numwait
FROM 
    supplier s
JOIN stock_part1 stp1 ON s.s_suppkey = MOD((stp1.s_w_id * stp1.s_i_id), 10000)
JOIN order_line_part2 l1p2 ON stp1.s_i_id = l1p2.ol_i_id AND stp1.s_w_id = l1p2.ol_w_id
JOIN orders_part2 op2 ON l1p2.ol_o_id = op2.o_id
JOIN nation_part1 np1 ON s.s_nationkey = np1.n_nationkey
JOIN nation_part2 np2 ON np1.n_nationkey = np2.n_nationkey
JOIN filtered_order_lines fol ON fol.ol_o_id = l1p2.ol_o_id 
    AND fol.ol_w_id = l1p2.ol_w_id 
    AND fol.ol_d_id = l1p2.ol_d_id
WHERE 
    l1p2.ol_delivery_d > op2.o_entry_d
    AND np2.n_name = 'GERMANY'
GROUP BY 
    s.s_name
ORDER BY 
    numwait DESC, s.s_name;

-- 22
WITH subquery AS (
    SELECT 
        AVG(c2p1.c_balance) AS avg_balance
    FROM 
        customer_part1 c2p1
    JOIN customer_part2 c2p2 ON c2p1.c_id = c2p2.c_id AND c2p1.c_w_id = c2p2.c_w_id AND c2p1.c_d_id = c2p2.c_d_id
    WHERE 
        c2p1.c_balance > 0.00 
        AND SUBSTR(c2p1.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')
)
SELECT 
    SUBSTR(cp2.c_state, 1, 1) AS country,
    COUNT(*) AS numcust,
    SUM(cp1.c_balance) AS totacctbal
FROM 
    customer_part1 cp1
JOIN customer_part2 cp2 ON cp1.c_id = cp2.c_id AND cp1.c_w_id = cp2.c_w_id AND cp1.c_d_id = cp2.c_d_id
JOIN subquery sq ON TRUE
WHERE 
    SUBSTR(cp1.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')
    AND cp1.c_balance > sq.avg_balance
    AND NOT EXISTS (
        SELECT *
        FROM orders_part2 op2
        WHERE 
            op2.o_c_id = cp1.c_id 
            AND op2.o_w_id = cp1.c_w_id 
            AND op2.o_d_id = cp1.c_d_id
    )
GROUP BY 
    SUBSTR(cp2.c_state, 1, 1)
ORDER BY 
    SUBSTR(cp2.c_state, 1, 1);