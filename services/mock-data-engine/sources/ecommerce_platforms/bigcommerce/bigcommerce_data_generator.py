import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_bigcommerce_data(tenant_slug: str, config: Any, stripe_charges: List[dict] = None, days: int = 30) -> Dict[str, List[dict]]:
    n_products = getattr(config, 'product_catalog_size', 500) or 500
    daily_orders = getattr(config, 'daily_order_count', 100) or 100
    avg_order_val = getattr(config, 'avg_order_value', 120.0) or 120.0
    
    product_ids = np.arange(20000, 20000 + n_products)
    products_df = pl.DataFrame({
        "id": product_ids,
        "name": [fake.catch_phrase() for _ in range(n_products)],
        "sku": [f"bc-{fake.ean8()}" for _ in range(n_products)],
        "price": np.random.uniform(10, 200, n_products).round(2),
        "availability": "available"
    })
    
    total_orders = int(daily_orders * days)
    order_ids = np.arange(90000, 90000 + total_orders)
    
    staff_notes = []
    if stripe_charges and len(stripe_charges) > 0:
        charge_ids = [c['id'] for c in stripe_charges]
        for _ in range(total_orders):
            if np.random.random() < 0.9:
                cid = np.random.choice(charge_ids)
                staff_notes.append(f"Stripe Charge ID: {cid}")
            else:
                staff_notes.append("")
    else:
         staff_notes = [""] * total_orders

    total_inc_tax = np.random.lognormal(np.log(avg_order_val), 0.5, total_orders).round(2)
    total_ex_tax = (total_inc_tax / 1.1).round(2)

    orders_df = pl.DataFrame({
        "id": order_ids,
        "status_id": np.random.choice([11, 10, 2], total_orders),
        "total_inc_tax": total_inc_tax,
        "total_ex_tax": total_ex_tax,
        "subtotal_ex_tax": total_ex_tax,
        "customer_id": np.random.randint(500, 1500, total_orders),
        "date_created": [fake.date_time_this_month().isoformat() for _ in range(total_orders)],
        "staff_notes": staff_notes
    })

    return {
        "orders": orders_df.to_dicts(),
        "products": products_df.to_dicts()
    }