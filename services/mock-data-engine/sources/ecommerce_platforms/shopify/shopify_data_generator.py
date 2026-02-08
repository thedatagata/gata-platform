import polars as pl
from faker import Faker
import random
from datetime import datetime
from typing import Dict, List, Any

fake = Faker()

def generate_shopify_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    n_products = getattr(config, 'product_catalog_size', 50) or 50
    daily_orders = getattr(config, 'daily_order_count', 20) or 20
    
    product_ids = [random.randint(100000, 999999) for _ in range(n_products)]
    products = [{"id": pid, "title": fake.catch_phrase().title(), "price": float(round(random.uniform(10, 100), 2)), "created_at": fake.date_time_this_year()} for pid in product_ids]

    orders = []
    total_orders = int(daily_orders * days)
    for _ in range(total_orders):
        price = float(round(random.uniform(50.0, 500.0), 2))
        orders.append({
            "id": random.randint(1000000000, 9999999999),
            "name": f"#{random.randint(1000, 99999)}",
            "email": fake.email(),
            "total_price": price,
            "currency": "USD",
            "financial_status": "paid",
            "status": "fulfilled",
            "customer_id": random.randint(1, 1000),
            "customer_email": fake.email(), # Flattened key instead of object
            "created_at": fake.date_time_this_month(),
            "line_items": [{"id": random.randint(1000, 9999), "product_id": random.choice(product_ids)}]
        })

    return {"products": products, "orders": orders}