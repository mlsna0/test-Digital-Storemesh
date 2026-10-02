SELECT 
    c.customer_id,
    c.full_name, 
    COUNT(o.order_id) AS total_orders_placed,
    COALESCE(SUM(o.usd_amount), 0) AS lifetime_value_usd,
    strftime('%Y-%m', c.signup_date) AS customer_cohort
FROM dim_customers c
LEFT JOIN fct_orders o 
    ON c.customer_id = o.customer_id
GROUP BY 
    c.customer_id,
    c.full_name,
    customer_cohort

-- 1	Alice Smith	3	1686.0	2023-06
-- 2	Bob Jones	2	275.0	2023-09
-- 3	Charlie Brown	1	25000.0	2023-03
-- 4	Diana Prince	2	389.5	2023-04
-- 5	Evan Wright	2	219.99	2023-04
-- 6	Fiona Gallagher	2	525.0	2023-05
-- 7	George Costanza	2	55.99	2023-06
-- 8	Hannah Abbott	1	89.0	2023-07
-- 9	Ian Malcolm	0	0	2023-08
-- 10	Jane Doe	0	0	2023-09

