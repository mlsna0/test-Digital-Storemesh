task 1 
ปัญหาแรกที่เจอคือ ใน raw_customers มีคนทที่ซ้ำกันอยู่ 2 คน
อย่างที่สอง คือ  phone มีบางคนที่รูปแบบไม่ตรง
อย่างที่สามคือ ทั้งใน raw_customers และ raw_orders มีค่าใน อยู่ใน fields


task 2 
 
 เริ่มจากการ python เพื่อเริ่มทำงาน python 
 หลัง จาก นั้น import sqlite3 เพื่อจะทำการเชื่อมข้อมูลใน data base 
 แล้ว ใช้คำสั่ง conn = sqlite3.connect("shopdata.db") เพื่อเชื่อม data base
 จากนั้นใช้ คำสั่ง cursor = conn.cursor() เพื่อส่งคำสั่ง sql ไปยัง data base แล้วเพื่อนำข้อมูลมาใช้บน python 
 จากนั้นใช้ คำสั่ง cursor.execute("""
SELECT name, type
FROM sqlite_master
WHERE type IN ('table', 'view')
""")
เพื่อดึงชื่อ table ใน database ทั้งหมด ที่เป็น type table และ view ออกมา

จากนั้นใช้คำสั่ง 
for row in cursor.fetchall():
    print(row) 
    เพื่อแสดงผลลัพธ์ออกมา

ผลลัพธ์ที่ได้จะเป็น
('raw_customers', 'table')
('vw_raw_customers', 'view')
('exchange_rates', 'table')
('vw_exchange_rates', 'view')
('raw_orders', 'table')
('vw_raw_orders', 'view')

จากนั้นจะใช้คำสั่งที่เหมือนกัน
 cursor.execute("""
 SELECT name
 FROM sqlite_master
 WHERE type = 'view'
""")

แต่รอบนี้รอบเราจะเอาชื่อ table  ที่เป็น view เท่านั้นเพื่อออกมาเช็ค 
จะใช้คำสั่งเดิมเพื่อแสดงผลลัพธ์
ส่วนผลลัพธ์ที่ได้จะเป็น
 [('vw_raw_customers',), ('vw_exchange_rates',), ('vw_raw_orders',)]

จากนั้นจะใช้คำสั่ง cursor.execute("PRAGMA table_info(vw_raw_customers)") เพื่อดูโครงสร้างภายใน table ที่เราต้องการ
เช่น vw_raw_customers  
ส่วนแสดงผลลัพธ์จะใช้คำสั่งเดิม 
for row in cursor.fetchall():
 print(row)
 ส่วนผลลัพธ์ ที่ได้ จะเป็นตามนี้ 
 (0, 'customer_id', 'INTEGER', 0, None, 0)
(1, 'full_name', 'TEXT', 0, None, 0)
(2, 'email', 'TEXT', 0, None, 0)
(3, 'phone', 'TEXT', 0, None, 0)
(4, 'signup_date', 'TEXT', 0, None, 0)


จากนั้นใช้คำสั่ง cursor.execute("SELECT * FROM vw_raw_orders LIMIT 5") ในการดึงข้อมูลใน  table ออกมาดู
ส่วนแสดงผลลัพธ์จะใช้คำสั่งเดิม 
 for row in cursor.fetchall():
     print(row)
     ผลลัพธ์ที่ได้
 (1, 'Alice Smith', 'alice@example.com', '+1 (555) 123-4567', '2023-01-15')
 (2, 'Bob Jones', None, '555-987-6543', '2023-02-20')
(3, 'Charlie Brown', 'charlie.b@example.com', '1234567890', '2023-03-05')
(1, 'Alice Smith', 'alice.smith@example.com', '15551234567', '2023-06-01')
 (4, 'Diana Prince', 'diana@example.com', 'Ext 444', '2023-04-10')

จากนั้นเช็คอีก 2 table 
 cursor.execute("SELECT * FROM vw_raw_orders LIMIT 5")

 for row in cursor.fetchall():
     print(row)

 (101, 1, '2023-05-01', 150.0, 'USD', 'COMPLETED')
 (102, 2, '2023-05-01', 200.0, 'EUR', 'COMPLETED')
 (103, 3, '2023-05-01', -50.0, 'USD', 'SYSTEM_ERROR')
 (104, 1, '2023-05-02', 300.0, 'EUR', 'COMPLETED')
 (105, 4, '2023-05-03', 10000.0, 'JPY', 'COMPLETED')

 cursor.execute("SELECT * FROM vw_exchange_rates LIMIT 5")

 for row in cursor.fetchall():
    print(row)
 (101, 1, '2023-05-01', 150.0, 'USD', 'COMPLETED')
 (102, 2, '2023-05-01', 200.0, 'EUR', 'COMPLETED')
 (103, 3, '2023-05-01', -50.0, 'USD', 'SYSTEM_ERROR')
 (104, 1, '2023-05-02', 300.0, 'EUR', 'COMPLETED')
 (105, 4, '2023-05-03', 10000.0, 'JPY', 'COMPLETED')

จากนั้นจึงสร้าง pipeline.py เพื่อทำการ สร้าง Data Pipeline ด้วย Python และ Prefect
โดยการใช้ คำสั่งเหล่านี้
import sqlite3
import logging
import re
import pandas as pd
from prefect import task, flow

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SOURCE_DB = "shopdata.db"
TARGET_DB = "analytics.db"

คำสั่งเหล่านี้จะทำการเรียกใช้เครื่องมือ(Libraries)ที่จำเป็นทั้งหมดเข้ามาก่อน เช่น เครื่องมือเชื่อมต่อฐานข้อมูล(sqlite3), เครื่องมือจัดการข้อมูลแบบตาราง(pandas),เครื่องมือคุมการทำงานของ Pipeline (prefect),และเครื่องมือจัดการข้อความ (re)
จากนั้นจะทำการเปิดระบบจดบันทึก(logging)เพื่อให้โปรแกรมสามารถพิมพ์รายงานสถานะการทำงานหรือแจ้งเตือนเวลาเกิด Error ให้เราเห็นบนหน้าจอได้ และสุดท้ายคือการตั้งชื่อไฟล์ฐานข้อมูลต้นทาง(shopdata.db)และฐานข้อมูลปลายทาง(analytics.db)เก็บไว้เป็นตัวแปร
จากนั้นใช้คำสั่ง
@task
def extract():
    try:conn = sqlite3.connect(SOURCE_DB)
        customers = pd.read_sql("SELECT * FROM vw_raw_customers", conn)
        orders = pd.read_sql("SELECT * FROM vw_raw_orders", conn)
        rates = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)
        conn.close()
        logger.info("success")
        return customers, orders, rates
    except Exception:
        logger.exception("fail")
        raise
โดยคำสั่งนี้จะทำการ
- เปิดฐานข้อมูล แล้วสูบข้อมูล 3 ตาราง (ลูกค้า, คำสั่งซื้อ, ค่าเงิน) ออกมาเก็บไว้
- ปิดฐานข้อมูล แจ้งเตือนว่าสำเร็จ ("success") และส่งข้อมูลทั้ง 3 ก้อนนี้ไปรอทำความสะอาด
- ถ้าดึงข้อมูลพังหรือมี Error กลางทาง ระบบจะฟ้องว่าล้มเหลว ("fail") และหยุดการทำงาน

ต่อไปจะใช้สั่งเหล่านี้ 
1 @task
def clean_customers(df):
    try:
        df["signup_date"] = pd.to_datetime(df["signup_date"])
        df = df.sort_values("signup_date").drop_duplicates(
            "customer_id", keep="last")
        df["phone"] = df["phone"].fillna("").astype(str).apply(
            lambda x: re.sub(r"\D", "", x))
        df["email"] = df["email"].fillna("unknown@domain.com")
        logger.info("clean")
        return df
    except Exception:
        logger.exception("faile")
        raise
โดยคำสั่งนี้จะทำการ 
- แปลงวันที่สมัคร เรียงลำดับ แล้วเก็บเฉพาะข้อมูลล่าสุดของลูกค้าแต่ละคน
- ลบสัญลักษณ์และตัวอักษรออก ให้เหลือเฉพาะตัวเลขล้วนๆ
- ถ้าอีเมลว่างเปล่า จะใส่ "unknown@domain.com" แทน
- ถ้าทำความสะอาดสำเร็จจะส่งข้อมูลกลับไปและบันทึกว่าเรียบร้อย แต่ถ้าเกิด Error จะแจ้งล้มเหลวแล้วหยุดทำงาน

2 @task
def clean_orders(df, rates):
    try:
        df["total_amount"] = pd.to_numeric(
            df["total_amount"], errors="coerce")
        df = df[df["total_amount"] > 0].copy()
        df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
        rates["date"] = pd.to_datetime(rates["date"])
        df["currency"] = df["currency"].fillna("USD")
        rates = rates.rename(columns={
            "date": "rate_date",
            "rate_to_usd": "exchange_rate"})
        rates.loc[ rates["currency"] == "USD", "exchange_rate"] = 1
        df = df.merge(
            rates[["rate_date", "currency", "exchange_rate"]],
            left_on=["order_date", "currency"],
            right_on=["rate_date", "currency"],
            how="left")
        df["exchange_rate"] = df["exchange_rate"].fillna(1)
        df["usd_amount"] = ( df["total_amount"] * df["exchange_rate"])
        df.drop(columns=["rate_date"], inplace=True)
        logger.info("clean")
        return df
    except Exception:
        logger.exception("fail")
        raise
ซึ่งเราจะรวม 2 table เพื่อดึงตารางมารวมกันมาแปะไว้ข้างๆจะได้จับคูณเปลี่ยนเป็นเงินดอลลาร์ได้ง่ายๆ
โดยคำสั่งนี้จะทำงาน 
- แปลงตัวเลขแล้วตัดออเดอร์ที่ยอดเงินน้อยกว่าหรือเท่ากับ 0 ทิ้งไป (Error ของระบบ)
- นำวันที่สั่งซื้อและสกุลเงินไปดึงค่าเปลี่ยนเงินจากตาราง rates (ถ้าไม่มีข้อมูลให้ถือเป็น 1 หรือ USD)
- เอายอดเงินคูณอัตราแลกเปลี่ยน ได้ผลลัพธ์เป็นคอลัมน์ usd_amount
- ทำสำเร็จจะบันทึก "clean" แล้วส่งข้อมูลกลับ แต่ถ้ามี Error จะแจ้งล้มเหลวแล้วหยุดทำงาน

3 @task
def load(customers, orders):
    try:
        conn = sqlite3.connect(TARGET_DB)
        customers.to_sql("dim_customers", conn, if_exists="replace", index=False)
        orders.to_sql("fct_orders", conn, if_exists="replace", index=False)
        conn.close()
        logger.info("success")
    except Exception:
        logger.exception("fail")
        raise
โดยคำสั่งนี้จะทำงาน 
- เชื่อมต่อไปยังไฟล์ analytics.db
- นำข้อมูลลูกค้าไปเซฟลงตารางชื่อ dim_customers
- นำข้อมูลคำสั่งซื้อไปเซฟลงตารางชื่อ fct_orders(ถ้ามีตารางเดิมอยู่แล้ว จะรันทับของเก่า)
- ปิดการเชื่อมต่อ แล้วบันทึกบอกว่าเรียบร้อย ("success")
- ถ้าเซฟไม่ผ่าน จะแจ้งล้มเหลว ("fail") แล้วหยุดทำงาน

ส่วน@taskแต่ละอัน คือการติดป้ายบอกว่า นี่คืองานย่อย 1 ชิ้นในระบบจัดการ Pipeline 

สุดท้ายจะใช้คำสั่ง
@flow
def pipeline():
    customers, orders, rates = extract()
    customers = clean_customers(customers)
    orders = clean_orders(orders, rates)
    load(customers, orders)
if __name__ == "__main__":
    pipeline()

โดยคำสั่งนี้จะทำการ
วางลำดับการทำงานให้เรียบร้อยตามขั้นตอน
- ดึงข้อมูลขึ้นมา (extract)
- คลีนข้อมูลลูกค้า (clean_customers)
- คลีนข้อมูลออเดอร์ (clean_orders)
- เซฟข้อมูลลงฐานข้อมูล (load)
ตัวสุดท้ายจะเป็นคำสั่งให้รันฟังก์ชัน pipeline() ทันทีเมื่อกดสั่งรันไฟล์นี้

task 3 
ผมจะใช่คำสั่ง สำหรับ test โดยการ ใช้ pytest 
import pytest
import pandas as pd
from pipeline import clean_customers, clean_orders
def test_clean_customers():
    raw_customers = pd.DataFrame({
        "customer_id": [1, 1, 2],
        "signup_date": ["2023-01-01", "2023-06-15", "2023-03-10"], 
        "phone": ["+1 (555) 123-4567", "081-234-5678", "+66 89 000 1111"],
        "email": ["old@domain.com", None, "user2@domain.com"]
    })
    result = clean_customers.fn(raw_customers)
    assert len(result) == 2
    cust_1 = result[result["customer_id"] == 1].iloc[0]
    assert cust_1["phone"] == "0812345678"
    assert cust_1["email"] == "unknown@domain.com"
def test_clean_orders():
    raw_orders = pd.DataFrame({
        "order_id": [101, 102, 103, 104],
        "customer_id": [1, 2, 3, 4],
        "order_date": ["2023-10-01", "2023-10-01", "2023-10-01", "2023-10-01"],
        "total_amount": [100.0, 0.0, -50.0, 3000.0], 
        "currency": ["USD", "THB", "THB", "THB"]
    })
    raw_rates = pd.DataFrame({
        "date": ["2023-10-01"],
        "currency": ["THB"],
        "rate_to_usd": [0.027]
    })
    result = clean_orders.fn(raw_orders, raw_rates)
    assert len(result) == 2
    assert set(result["order_id"]) == {101, 104}
    order_104 = result[result["order_id"] == 104].iloc[0]
    assert pytest.approx(order_104["usd_amount"], 0.01) == 81.0

โดยคำสั่งนี้จะเป็นทำงาน 
1. test_clean_customers() (ทดสอบการคลีนข้อมูลลูกค้า):
- ใส่ลูกค้า ID 1 ซ้ำกัน, เบอร์โทรมีสัญลักษณ์ + - (), และอีเมลว่างเปล่า (None)
- ใช้ .fn เรียกฟังก์ชัน clean_customers มาทดสอบตรงๆ
- ตรวจสอบผลลัพธ์ (assert)
    - ต้องเหลือลูกค้าแค่ 2 รายการ (ลบตัวซ้ำออก)
    - เบอร์โทรของลูกค้า ID 1 ต้องถูกลบสัญลักษณ์เหลือแค่ตัวเลข "0812345678"
    - อีเมลที่เป็น None ต้องถูกเติมเป็น "unknown@domain.com"
2. test_clean_orders() (ทดสอบการกรองยอดเงินและแปลงค่าเงิน):
- ใส่ออเดอร์ที่มียอดเงินปกติ, ยอดเป็น 0, ยอดติดลบ และใส่ตารางเรทแลกเงิน (1 THB = 0.027 USD)
- สั่งรันคลีน: ส่งออเดอร์และเรทเงินเข้าไปคำนวณ
- ตรวจสอบผลลัพธ์ (assert)
    - ต้องกรองยอดเงินน้อยกว่าหรือเท่ากับ 0 ทิ้ง เหลือแค่ออเดอร์ 101 และ 104 (รวม 2 รายการ)
    - ตรวจเช็กการคำนวณยอดเงิน USD ของออเดอร์ 104 (3,000 THB $\times$ 0.027) ว่าได้ 81.0 USD ถูกต้อง

task 4
จะใช้คำสั่ง 
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

- โดยจะเอาตารางลูกค้า (dim_customers) เป็นหลัก แล้วนำประวัติการซื้อ (fct_orders) มาต่อข้างๆ เพื่อให้ได้ข้อมูลลูกค้าทุกคน แม้คนนั้นจะยังไม่เคยสั่งซื้อเลยก็ตาม
- สรุปข้อมูลให้ออกมาเป็น 1 บรรทัดต่อ 1 คน
ผลลัพธ์ที่ได้จะเป็นตามนี้ 
1	Alice Smith	3	1686.0	2023-06
2	Bob Jones	2	275.0	2023-09
3	Charlie Brown	1	25000.0	2023-03
4	Diana Prince	2	389.5	2023-04
5	Evan Wright	2	219.99	2023-04
6	Fiona Gallagher	2	525.0	2023-05
7	George Costanza	2	55.99	2023-06
8	Hannah Abbott	1	89.0	2023-07
9	Ian Malcolm	0	0	2023-08
10	Jane Doe	0	0	2023-09