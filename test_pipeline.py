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