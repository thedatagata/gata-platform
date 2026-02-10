import polars as pl
import numpy as np
from faker import Faker
import random
from datetime import datetime
from typing import Dict, List, Any

fake = Faker()

def generate_woocommerce_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    n_products = getattr(config, 'product_catalog_size', 50) or 50
    daily_orders = getattr(config, 'daily_order_count', 20) or 20
    
    # 1. Products
    product_ids = np.arange(1000, 1000 + n_products)
    products = [{"id": int(pid), "name": fake.catch_phrase(), "price": round(random.uniform(10, 200), 2), "created_at": fake.date_time_this_year()} for pid in product_ids]
    
    # 2. Orders
    orders = []
    total_orders = int(daily_orders * days)
    for _ in range(total_orders):
        price = round(np.random.lognormal(np.log(100), 0.5), 2)
        # Select random products for line items
        n_items = random.randint(1, 4)
        selected_products = random.choices(products, k=n_items)
        line_items = [
            {
                "product_id": p["id"],
                "name": p["name"],
                "quantity": random.randint(1, 3),
                "price": p["price"]
            }
            for p in selected_products
        ]

        orders.append({
            "id": random.randint(5000, 99999),
            "number": str(random.randint(10000, 50000)),
            "status": random.choice(["completed", "processing", "refunded"]),
            "total_price": float(price),
            "currency": "USD",
            "customer_id": random.randint(1, 1000),
            "billing_email": fake.ascii_email(),
            "line_items": line_items,
            "created_at": fake.date_time_this_month()
        })

    return {"products": products, "orders": orders}
