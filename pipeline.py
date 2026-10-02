import sqlite3
import logging
import re
import pandas as pd
from prefect import task, flow
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
SOURCE_DB = "shopdata.db"
TARGET_DB = "analytics.db"
@task
def extract():
    try:
        conn = sqlite3.connect(SOURCE_DB)
        customers = pd.read_sql("SELECT * FROM vw_raw_customers", conn)
        orders = pd.read_sql("SELECT * FROM vw_raw_orders", conn)
        rates = pd.read_sql("SELECT * FROM vw_exchange_rates", conn)
        conn.close()
        logger.info("success")
        return customers, orders, rates
    except Exception:
        logger.exception("fail")
        raise
@task
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
        logger.exception("fail")
        raise
@task
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
@task
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
@flow
def pipeline():
    customers, orders, rates = extract()
    customers = clean_customers(customers)
    orders = clean_orders(orders, rates)
    load(customers, orders)
if __name__ == "__main__":
    pipeline()


