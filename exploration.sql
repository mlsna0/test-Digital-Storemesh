
-- SELECT *
-- FROM raw_customers;
-- 
-- SELECT *
-- FROM raw_orders;

-- import sqlite3

-- conn = sqlite3.connect("shopdata.db")

-- cursor = conn.cursor()

-- cursor.execute("""
-- SELECT name, type
-- FROM sqlite_master
-- WHERE type IN ('table', 'view')
-- """)

-- for row in cursor.fetchall():
--     print(row)

-- cursor.execute("""
-- SELECT name
-- FROM sqlite_master
-- WHERE type = 'view'
-- """)

-- views = cursor.fetchall()

-- print(views)

-- [('vw_raw_customers',), ('vw_exchange_rates',), ('vw_raw_orders',)]

-- cursor.execute("PRAGMA table_info(vw_raw_customers)")
-- 
-- for row in cursor.fetchall():
--     print(row)

-- (0, 'customer_id', 'INTEGER', 0, None, 0)
-- (1, 'full_name', 'TEXT', 0, None, 0)
-- (2, 'email', 'TEXT', 0, None, 0)
-- (3, 'phone', 'TEXT', 0, None, 0)
-- (4, 'signup_date', 'TEXT', 0, None, 0)

-- cursor.execute("SELECT * FROM vw_raw_customers LIMIT 5")

-- for row in cursor.fetchall():
--     print(row)
-- (1, 'Alice Smith', 'alice@example.com', '+1 (555) 123-4567', '2023-01-15')
-- (2, 'Bob Jones', None, '555-987-6543', '2023-02-20')
-- (3, 'Charlie Brown', 'charlie.b@example.com', '1234567890', '2023-03-05')
-- (1, 'Alice Smith', 'alice.smith@example.com', '15551234567', '2023-06-01')
-- (4, 'Diana Prince', 'diana@example.com', 'Ext 444', '2023-04-10')

-- cursor.execute("SELECT * FROM vw_raw_orders LIMIT 5")

-- for row in cursor.fetchall():
--     print(row)

-- (101, 1, '2023-05-01', 150.0, 'USD', 'COMPLETED')
-- (102, 2, '2023-05-01', 200.0, 'EUR', 'COMPLETED')
-- (103, 3, '2023-05-01', -50.0, 'USD', 'SYSTEM_ERROR')
-- (104, 1, '2023-05-02', 300.0, 'EUR', 'COMPLETED')
-- (105, 4, '2023-05-03', 10000.0, 'JPY', 'COMPLETED')

-- cursor.execute("SELECT * FROM vw_exchange_rates LIMIT 5")

-- for row in cursor.fetchall():
--     print(row)
-- (101, 1, '2023-05-01', 150.0, 'USD', 'COMPLETED')
-- (102, 2, '2023-05-01', 200.0, 'EUR', 'COMPLETED')
-- (103, 3, '2023-05-01', -50.0, 'USD', 'SYSTEM_ERROR')
-- (104, 1, '2023-05-02', 300.0, 'EUR', 'COMPLETED')
-- (105, 4, '2023-05-03', 10000.0, 'JPY', 'COMPLETED')



