import polars as pl
import numpy as np
from faker import Faker
import random
from datetime import datetime
from typing import Dict, List, Any

fake = Faker()

def generate_bigcommerce_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    n_products = getattr(config, 'product_catalog_size', 50) or 50
    daily_orders = getattr(config, 'daily_order_count', 20) or 20
    status_map = {11: "Completed", 10: "Completed", 2: "Shipped"} #

    # 1. Products
    product_ids = np.arange(20000, 20000 + n_products)
    products = [{"id": int(pid), "name": fake.catch_phrase(), "price": round(random.uniform(10, 200), 2)} for pid in product_ids]
    
    # 2. Orders
    orders = []
    total_orders = int(daily_orders * days)
    for _ in range(total_orders):
        sid = random.choice([11, 10, 2])
        price = round(np.random.lognormal(np.log(120), 0.5), 2)
        orders.append({
            "id": random.randint(90000, 150000),
            "status_id": sid,
            "status": status_map[sid],
            "total_price": float(price),
            "currency": "USD",
            "customer_id": random.randint(500, 1500),
            "created_at": fake.date_time_this_month()
        })

    return {"products": products, "orders": orders}