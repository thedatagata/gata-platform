
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_woocommerce_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates WooCommerce data.
    Objects: orders, products
    """
    n_products = config.product_catalog_size or 500
    daily_orders = config.daily_order_count or 100
    avg_order_val = config.avg_order_value or 120.0
    
    # 1. Products
    product_ids = np.arange(1000, 1000 + n_products)
    products_df = pl.DataFrame({
        "id": product_ids,
        "name": [fake.catch_phrase() for _ in range(n_products)],
        "sku": [f"woo-{fake.ean8()}" for _ in range(n_products)],
        "price": np.random.uniform(10, 200, n_products).round(2),
        "status": "publish"
    })
    
    # 2. Orders
    total_orders = daily_orders * days
    order_ids = np.arange(5000, 5000 + total_orders)
    
    orders_df = pl.DataFrame({
        "id": order_ids,
        "status": np.random.choice(["completed", "processing", "refunded"], total_orders, p=[0.8, 0.15, 0.05]),
        "total": np.random.lognormal(np.log(avg_order_val), 0.5, total_orders).round(2),
        "currency": "USD",
        "customer_id": np.random.randint(1, 1000, total_orders),
        "date_created_gmt": [fake.date_time_this_month().isoformat() for _ in range(total_orders)]
    })

    return {
        "orders": orders_df.to_dicts(),
        "products": products_df.to_dicts()
    }
