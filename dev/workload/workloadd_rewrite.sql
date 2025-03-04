SELECT olp2.ol_number,
       SUM(olp1.ol_quantity) AS sum_qty,
       SUM(olp1.ol_amount) AS sum_amount,
       AVG(olp1.ol_quantity) AS avg_qty,
       AVG(olp1.ol_amount) AS avg_amount,
       COUNT(*) AS count_order
FROM order_line_part1 olp1
JOIN order_line_part2 olp2 ON olp1.ol_o_id = olp2.ol_o_id AND olp1.ol_d_id = olp2.ol_d_id AND olp1.ol_w_id = olp2.ol_w_id
WHERE olp1.ol_delivery_d > '2024-10-28 17:00:00'
GROUP BY olp2.ol_number
ORDER BY olp2.ol_number;

SELECT su.s_suppkey,
       su.s_name,
       n.n_name,
       i.i_id,
       i.i_name,
       su.s_address,
       su.s_phone,
       su.s_comment
FROM item i
JOIN stock_part1 s ON i.i_id = s.s_i_id
JOIN supplier su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey
JOIN nation n ON su.s_nationkey = n.n_nationkey
JOIN region r ON n.n_regionkey = r.r_regionkey
JOIN (
         SELECT s_sub.s_i_id AS m_i_id,
                MIN(s_sub.s_quantity) AS m_s_quantity
         FROM stock_part1 s_sub
         JOIN supplier su_sub ON MOD((s_sub.s_w_id * s_sub.s_i_id), 10000) = su_sub.s_suppkey
         JOIN nation n_sub ON su_sub.s_nationkey = n_sub.n_nationkey
         JOIN region r_sub ON n_sub.n_regionkey = r_sub.r_regionkey
         WHERE r_sub.r_name LIKE 'EUROP%'
         GROUP BY s_sub.s_i_id
     ) m ON i.i_id = m.m_i_id AND s.s_quantity = m.m_s_quantity
WHERE i.i_data LIKE '%b' AND r.r_name LIKE 'EUROP%'
ORDER BY n.n_name, su.s_name, i.i_id;

SELECT olp1.ol_o_id,
       olp1.ol_w_id,
       olp1.ol_d_id,
       SUM(olp1.ol_amount) AS revenue,
       o.o_entry_d
FROM customer c
JOIN new_order no ON c.c_w_id = no.no_w_id AND c.c_d_id = no.no_d_id
JOIN orders o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id AND no.no_w_id = o.o_w_id AND no.no_d_id = o.o_d_id AND no.no_o_id = o.o_id
JOIN order_line_part1 olp1 ON o.o_w_id = olp1.ol_w_id AND o.o_d_id = olp1.ol_d_id AND o.o_id = olp1.ol_o_id
WHERE c.c_state LIKE 'A%' AND o.o_entry_d > '2024-10-28 17:00:00'
GROUP BY olp1.ol_o_id, olp1.ol_w_id, olp1.ol_d_id, o.o_entry_d
ORDER BY revenue DESC, o.o_entry_d;

SELECT o.o_ol_cnt,
       COUNT(*) AS order_count
FROM orders o
JOIN order_line_part1 olp1 ON o.o_id = olp1.ol_o_id AND o.o_w_id = olp1.ol_w_id AND o.o_d_id = olp1.ol_d_id AND olp1.ol_delivery_d >= o.o_entry_d
WHERE o.o_entry_d >= '2024-10-28 17:00:00' AND o.o_entry_d < '2025-10-23 17:00:00'
GROUP BY o.o_ol_cnt
ORDER BY o.o_ol_cnt;

SELECT n.n_name,
       SUM(olp1.ol_amount) AS revenue
FROM customer c
JOIN orders o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id
JOIN order_line_part1 olp1 ON o.o_w_id = olp1.ol_w_id AND o.o_d_id = olp1.ol_d_id AND o.o_id = olp1.ol_o_id
JOIN stock_part1 s ON olp1.ol_w_id = s.s_w_id AND olp1.ol_i_id = s.s_i_id
JOIN supplier su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey
JOIN nation n ON su.s_nationkey = n.n_nationkey AND ASCII(SUBSTR(c.c_state, 1, 1)) = n.n_nationkey
JOIN region r ON n.n_regionkey = r.r_regionkey
WHERE r.r_name = 'EUROPE' AND o.o_entry_d >= '2024-10-28 17:00:00'
GROUP BY n.n_name
ORDER BY revenue DESC;

SELECT SUM(olp1.ol_amount) AS revenue
FROM order_line_part1 olp1
WHERE olp1.ol_delivery_d >= '2024-10-23 17:00:00' AND olp1.ol_delivery_d < '2024-10-28 17:00:00' AND olp1.ol_quantity BETWEEN 1 AND 100000;

SELECT s.s_nationkey AS supp_nation,
       SUBSTR(c.c_state, 1, 1) AS cust_nation,
       EXTRACT(YEAR FROM o.o_entry_d) AS l_year,
       SUM(olp1.ol_amount) AS revenue
FROM order_line_part1 olp1
JOIN stock_part1 st ON olp1.ol_supply_w_id = st.s_w_id AND olp1.ol_i_id = st.s_i_id
JOIN orders o ON olp1.ol_w_id = o.o_w_id AND olp1.ol_d_id = o.o_d_id AND olp1.ol_o_id = o.o_id
JOIN customer c ON o.o_c_id = c.c_id AND o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id
JOIN supplier s ON MOD((st.s_w_id * st.s_i_id), 10000) = s.s_suppkey
JOIN nation n1 ON s.s_nationkey = n1.n_nationkey
JOIN nation n2 ON ASCII(SUBSTR(c.c_state, 1, 1)) = n2.n_nationkey
WHERE MOD((st.s_w_id * st.s_i_id), 10000) = s.s_suppkey
  AND ((n1.n_name = 'GERMANY' AND n2.n_name = 'CANADA') OR (n1.n_name = 'CANADA' AND n2.n_name = 'GERMANY'))
  AND o.o_entry_d BETWEEN '2024-10-28 17:00:00' AND '2025-10-23 17:00:00'
GROUP BY s.s_nationkey, SUBSTR(c.c_state, 1, 1), EXTRACT(YEAR FROM o.o_entry_d)
ORDER BY s.s_nationkey, cust_nation, l_year;

SELECT EXTRACT(YEAR FROM o.o_entry_d) AS l_year,
       SUM(CASE
               WHEN n2.n_name = 'GERMANY' THEN olp1.ol_amount
               ELSE 0
               END) / SUM(olp1.ol_amount) AS mkt_share
FROM item i
JOIN stock_part1 s ON i.i_id = s.s_i_id
JOIN order_line_part1 olp1 ON s.s_i_id = olp1.ol_i_id AND s.s_w_id = olp1.ol_supply_w_id
JOIN orders o ON olp1.ol_w_id = o.o_w_id AND olp1.ol_d_id = o.o_d_id AND olp1.ol_o_id = o.o_id
JOIN customer c ON o.o_c_id = c.c_id AND o.o_w_id = c.c_w_id AND o.o_d_id = c.c_d_id
JOIN supplier su ON MOD((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey
JOIN nation n1 ON ASCII(SUBSTR(c.c_state, 1, 1)) = n1.n_nationkey
JOIN nation n2 ON su.s_nationkey = n2.n_nationkey
JOIN region r ON n1.n_regionkey = r.r_regionkey
WHERE olp1.ol_i_id < 1000 AND r.r_name = 'EUROPE'
  AND o.o_entry_d BETWEEN '2024-10-23 17:00:00' AND '2024-10-28 17:00:00'
  AND i.i_data LIKE '%b' AND i.i_id = olp1.ol_i_id
GROUP BY EXTRACT(YEAR FROM o.o_entry_d)
ORDER BY l_year;

select 
    n.n_name, 
    extract(year from o.o_entry_d) as l_year, 
    sum(olp1.ol_amount) as sum_profit 
from 
    item i, 
    stock_part1 s, 
    supplier su, 
    order_line_part1 olp1, 
    orders o, 
    nation n 
where 
    olp1.ol_i_id = s.s_i_id 
    and olp1.ol_supply_w_id = s.s_w_id 
    and mod((s.s_w_id * s.s_i_id), 10000) = su.s_suppkey 
    and olp1.ol_w_id = o.o_w_id 
    and olp1.ol_d_id = o.o_d_id 
    and olp1.ol_o_id = o.o_id 
    and olp1.ol_i_id = i.i_id 
    and su.s_nationkey = n.n_nationkey 
    and i.i_data like '%BB' 
group by 
    n.n_name, extract(year from o.o_entry_d) 
order by 
    n.n_name, l_year desc;

select 
    c.c_id, 
    c.c_last, 
    sum(olp1.ol_amount) as revenue, 
    c.c_city, 
    c.c_phone, 
    n.n_name 
from 
    customer c, 
    orders o, 
    order_line_part1 olp1, 
    nation n 
where 
    c.c_id = o.o_c_id 
    and c.c_w_id = o.o_w_id 
    and c.c_d_id = o.o_d_id 
    and olp1.ol_w_id = o.o_w_id 
    and olp1.ol_d_id = o.o_d_id 
    and olp1.ol_o_id = o.o_id 
    and o.o_entry_d >= '2024-10-28 17:00:00' 
    and o.o_entry_d <= olp1.ol_delivery_d 
    and n.n_nationkey = ascii(substr(c.c_state,1,1)) 
group by 
    c.c_id, c.c_last, c.c_city, c.c_phone, n.n_name 
order by 
    revenue desc;

select 
    s.s_i_id, 
    sum(sp2.s_order_cnt) as ordercount 
from 
    stock_part1 s, 
    supplier su, 
    nation n,
    stock_part2 sp2
where 
    s.s_i_id = sp2.s_i_id
    and s.s_w_id = sp2.s_w_id
    and mod((s.s_w_id * s.s_i_id),10000) = su.s_suppkey 
    and su.s_nationkey = n.n_nationkey 
    and n.n_name = 'GERMANY' 
group by 
    s.s_i_id 
having 
    sum(sp2.s_order_cnt) > (
        select 
            sum(sp2_sub.s_order_cnt) * 0.005 
        from 
            stock_part1 s_sub, 
            supplier su_sub, 
            nation n_sub,
            stock_part2 sp2_sub
        where 
            s_sub.s_i_id = sp2_sub.s_i_id
            and s_sub.s_w_id = sp2_sub.s_w_id
            and mod((s_sub.s_w_id * s_sub.s_i_id),10000) = su_sub.s_suppkey 
            and su_sub.s_nationkey = n_sub.n_nationkey 
            and n_sub.n_name = 'GERMANY'
    ) 
order by 
    ordercount desc;

select 
    o.o_ol_cnt, 
    sum(case when o.o_carrier_id = 1 or o.o_carrier_id = 2 then 1 else 0 end) as high_line_count, 
    sum(case when o.o_carrier_id <> 1 and o.o_carrier_id <> 2 then 1 else 0 end) as low_line_count 
from 
    orders o, 
    order_line_part1 olp1 
where 
    olp1.ol_w_id = o.o_w_id 
    and olp1.ol_d_id = o.o_d_id 
    and olp1.ol_o_id = o.o_id 
    and o.o_entry_d <= olp1.ol_delivery_d 
    and olp1.ol_delivery_d < '2025-10-23 17:00:00' 
group by 
    o.o_ol_cnt 
order by 
    o.o_ol_cnt;

SELECT 
    c_count, 
    COUNT(*) AS custdist 
FROM (
    SELECT 
        c.c_id, 
        COUNT(o.o_id) AS c_count 
    FROM 
        customer c 
        LEFT JOIN orders o ON c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id AND o.o_carrier_id > 8 
    GROUP BY 
        c.c_id
) AS c_orders 
GROUP BY 
    c_count 
ORDER BY 
    custdist DESC, c_count DESC;

SELECT 
    100.00 * SUM(CASE WHEN i.i_data LIKE 'PR%' THEN olp1.ol_amount ELSE 0 END) / NULLIF(SUM(olp1.ol_amount), 0) AS promo_revenue 
FROM 
    order_line_part1 olp1 
    JOIN item i ON olp1.ol_i_id = i.i_id 
WHERE 
    olp1.ol_delivery_d >= '2024-10-28 17:00:00' 
    AND olp1.ol_delivery_d < '2025-10-23 17:00:00';

WITH revenue AS (
    SELECT 
        MOD((st.s_w_id * st.s_i_id), 10000) AS supplier_no,
        SUM(olp1.ol_amount) AS total_revenue 
    FROM 
        order_line_part1 olp1 
        JOIN stock_part1 st ON olp1.ol_i_id = st.s_i_id AND olp1.ol_supply_w_id = st.s_w_id 
    WHERE 
        olp1.ol_delivery_d >= '2024-10-28 17:00:00' 
    GROUP BY 
        MOD((st.s_w_id * st.s_i_id), 10000)
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

SELECT 
    i.i_name,
    SUBSTR(i.i_data, 1, 3) AS brand,
    i.i_price,
    COUNT(DISTINCT MOD((st.s_w_id * st.s_i_id), 10000)) AS supplier_cnt 
FROM 
    stock_part1 st 
    JOIN item i ON i.i_id = st.s_i_id 
WHERE 
    i.i_data NOT LIKE 'zz%' 
    AND MOD((st.s_w_id * st.s_i_id), 10000) NOT IN (
        SELECT s.s_suppkey 
        FROM supplier s 
        WHERE s.s_comment LIKE '%bad%'
    ) 
GROUP BY 
    i.i_name, 
    SUBSTR(i.i_data, 1, 3), 
    i.i_price 
ORDER BY 
    supplier_cnt DESC;

SELECT 
    SUM(olp1.ol_amount) / 2.0 AS avg_yearly 
FROM 
    order_line_part1 olp1 
    JOIN (
        SELECT 
            i.i_id, 
            AVG(olp1.ol_quantity) AS a 
        FROM 
            order_line_part1 olp1 
            JOIN item i ON olp1.ol_i_id = i.i_id 
        GROUP BY 
            i.i_id
    ) AS t ON olp1.ol_i_id = t.i_id 
WHERE 
    olp1.ol_quantity < t.a;

SELECT 
    c.c_last, 
    c.c_id, 
    o.o_id, 
    o.o_entry_d, 
    o.o_ol_cnt, 
    sum(olp1.ol_amount) 
FROM 
    customer c, 
    orders o, 
    order_line_part1 olp1 
WHERE 
    c.c_id = o.o_c_id 
    AND c.c_w_id = o.o_w_id 
    AND c.c_d_id = o.o_d_id 
    AND olp1.ol_w_id = o.o_w_id 
    AND olp1.ol_d_id = o.o_d_id 
    AND olp1.ol_o_id = o.o_id 
GROUP BY 
    o.o_id, 
    o.o_w_id, 
    o.o_d_id, 
    c.c_id, 
    c.c_last, 
    o.o_entry_d, 
    o.o_ol_cnt 
HAVING 
    sum(olp1.ol_amount) > 200 
ORDER BY 
    sum(olp1.ol_amount) DESC, 
    o.o_entry_d;

SELECT 
    SUM(olp1.ol_amount) AS revenue 
FROM 
    order_line_part1 olp1 
    JOIN item i ON olp1.ol_i_id = i.i_id 
WHERE 
    olp1.ol_quantity >= 1 
    AND olp1.ol_quantity <= 10 
    AND i.i_price BETWEEN 1 AND 400000 
    AND (
        (i.i_data LIKE '%a' AND olp1.ol_w_id IN (1, 2, 3)) 
        OR (i.i_data LIKE '%b' AND olp1.ol_w_id IN (1, 2, 4)) 
        OR (i.i_data LIKE '%c' AND olp1.ol_w_id IN (1, 5, 3))
    );

SELECT 
    s.s_name, 
    s.s_address 
FROM 
    supplier s, 
    nation n 
WHERE 
    s.s_suppkey IN (
        SELECT 
            mod(s.s_i_id * s.s_w_id, 10000) 
        FROM 
            stock_part1 s 
            JOIN order_line_part1 olp1 ON s.s_i_id = olp1.ol_i_id 
        WHERE 
            s.s_i_id IN (
                SELECT 
                    i.i_id 
                FROM 
                    item i 
                WHERE 
                    i.i_data LIKE 'co%'
            ) 
            AND olp1.ol_delivery_d > '2024-10-28 17:00:00' 
        GROUP BY 
            s.s_i_id, 
            s.s_w_id, 
            s.s_quantity 
        HAVING 
            2 * s.s_quantity > SUM(olp1.ol_quantity)
    ) 
    AND s.s_nationkey = n.n_nationkey 
    AND n.n_name = 'GERMANY' 
ORDER BY 
    s.s_name;

SELECT 
    s.s_name, 
    COUNT(*) AS numwait 
FROM 
    supplier s 
    JOIN stock_part1 st ON s.s_suppkey = MOD((st.s_w_id * st.s_i_id), 10000) 
    JOIN order_line_part1 l1 ON l1.ol_i_id = st.s_i_id AND l1.ol_w_id = st.s_w_id 
    JOIN orders o ON l1.ol_o_id = o.o_id 
    JOIN nation n ON s.s_nationkey = n.n_nationkey 
WHERE 
    l1.ol_delivery_d > o.o_entry_d 
    AND NOT EXISTS (
        SELECT * 
        FROM order_line_part1 l2 
        WHERE 
            l2.ol_o_id = l1.ol_o_id 
            AND l2.ol_w_id = l1.ol_w_id 
            AND l2.ol_d_id = l1.ol_d_id 
            AND l2.ol_delivery_d > l1.ol_delivery_d
    ) 
    AND n.n_name = 'GERMANY' 
GROUP BY 
    s.s_name 
ORDER BY 
    numwait DESC, 
    s.s_name;

SELECT 
    SUBSTR(c.c_state, 1, 1) AS country,
    COUNT(*) AS numcust, 
    SUM(c.c_balance) AS totacctbal 
FROM 
    customer c 
WHERE 
    SUBSTR(c.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7') 
    AND c.c_balance > (
        SELECT 
            AVG(c2.c_balance) 
        FROM 
            customer c2 
        WHERE 
            c2.c_balance > 0.00 
            AND SUBSTR(c2.c_phone, 1, 1) IN ('1', '2', '3', '4', '5', '6', '7')
    )
    AND NOT EXISTS (
        SELECT * 
        FROM orders o 
        WHERE 
            o.o_c_id = c.c_id 
            AND o.o_w_id = c.c_w_id 
            AND o.o_d_id = c.c_d_id
    ) 
GROUP BY 
    SUBSTR(c.c_state, 1, 1) 
ORDER BY 
    SUBSTR(c.c_state, 1, 1);