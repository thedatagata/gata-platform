
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_woocommerce_data(tenant_slug: str, config: Any, stripe_charges: List[dict] = None, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates WooCommerce data.
    Objects: orders, products
    """
    n_products = config.product_count or config.product_catalog_size or 50
    daily_orders = config.daily_orders_mean or config.daily_order_count or 25.0
    
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
    total_orders = int(daily_orders * days)
    order_ids = np.arange(5000, 5000 + total_orders)
    
    # Generate random dates
    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    dates = []
    
    # Basic random dates distribution
    for _ in range(total_orders):
        delta = end_date - start_date
        random_days = np.random.randint(0, delta.days + 1)
        dates.append((start_date + timedelta(days=random_days)).isoformat())

    # Link to Stripe Charges if available
    # We'll assign a stripe charge ID to ~90% of orders if we have charges
    note_attributes = []
    if stripe_charges and len(stripe_charges) > 0:
        charge_ids = [c['id'] for c in stripe_charges]
        # We cycle through charge IDs if we have more orders than charges (or random sample)
        #Ideally we map 1:1 or logic, but for mock data random linkage is sufficient for the join test
        for _ in range(total_orders):
            if np.random.random() < 0.9:
                cid = np.random.choice(charge_ids)
                # Structure matching Shopify note_attributes for compatibility if needed, 
                # or just a direct meta_data field for Woo
                # The factory uses generic logic? 
                # Request said: "In the Shopify engine, extract the ID: raw_data_payload->'$.note_attributes'->0->>'value' as stripe_charge_id."
                # Does Woo engine use same macro? No, it has `engine_woocommerce_orders`.
                # But for now, let's just output it in a way we can use.
                # Since we haven't updated `engine_woocommerce_orders` macro in this session, 
                # and the Prompt 3 was specific to Shopify engine.
                # But we are running Stark Industries which uses WooCommerce.
                # We should probably add it to meta_data in Woo format.
                note_attributes.append([{"key": "stripe_charge_id", "value": cid}])
            else:
                note_attributes.append([])
    else:
        note_attributes = [[] for _ in range(total_orders)]

    orders_df = pl.DataFrame({
        "id": order_ids,
        "status": np.random.choice(["completed", "processing", "refunded"], total_orders, p=[0.8, 0.15, 0.05]),
        "total": np.random.lognormal(np.log(100), 0.5, total_orders).round(2),
        "currency": "USD",
        "customer_id": np.random.randint(1, 1000, total_orders),
        "date_created_gmt": dates,
        "meta_data": note_attributes # For Woo these are usually meta_data, but let's assume simple structure for now
    })

    return {
        "orders": orders_df.to_dicts(),
        "products": products_df.to_dicts()
    }
