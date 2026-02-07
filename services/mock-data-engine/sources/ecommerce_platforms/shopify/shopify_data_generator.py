import polars as pl
from faker import Faker
import random
from typing import Dict, List, Any

fake = Faker()

def generate_shopify_data(tenant_slug: str, config: Any, stripe_charges: List[dict] = None) -> Dict[str, List[dict]]:
    """
    Generates Shopify Orders with numeric price types. 
    Supports independent generation or 1:1 mapping with Stripe charges.
    """
    n_products = getattr(config, 'product_count', None) or getattr(config, 'product_catalog_size', 10) or 10
    
    # 1. Generate Products Catalog
    product_ids = [random.randint(1000000000, 9999999999) for _ in range(n_products)]
    products = pl.DataFrame({
        "id": product_ids,
        "title": [fake.catch_phrase().title() for _ in product_ids],
        "product_type": "Mock Product",
        "status": "active",
        "created_at": [fake.date_time_this_year() for _ in product_ids],
        "variants": [[{"id": random.randint(100,999), "price": 10.0}] for _ in product_ids] 
    })

    # 2. Generate Orders (Independent or Mapped)
    orders = []
    
    if stripe_charges:
        # Map 1:1 with Stripe logic
        customer_pool = [{"id": random.randint(1, 10000), "email": fake.email()} for _ in range(int(len(stripe_charges) * 0.7))]
        for charge in stripe_charges:
            customer = random.choice(customer_pool) if (customer_pool and random.random() > 0.3) else {"id": random.randint(10001, 99999), "email": fake.email()}
            price = round(charge.get('amount', 0) / 100.0, 2)
            created_at = charge.get('created', fake.date_time_this_year())
            
            orders.append({
                "id": random.randint(1000000000, 9999999999), "name": f"#{random.randint(1000, 99999)}",
                "email": customer['email'], "created_at": created_at, "processed_at": created_at, "updated_at": created_at,
                "total_price": price, "subtotal_price": price, "currency": "USD", "financial_status": "paid",
                "customer": customer, "note_attributes": [{"name": "stripe_charge_id", "value": charge.get('id', '')}],
                "line_items": [{"id": random.randint(1000,9999), "quantity": 1, "price": price}]
            })
    else:
        # Independent Mock Data Generation for Library Init
        n_orders = getattr(config, 'daily_order_count', 20) or 20
        for _ in range(n_orders):
            price = round(random.uniform(50.0, 500.0), 2)
            created_at = fake.date_time_this_year()
            orders.append({
                "id": random.randint(1000000000, 9999999999), "name": f"#{random.randint(1000, 99999)}",
                "email": fake.email(), "created_at": created_at, "processed_at": created_at, "updated_at": created_at,
                "total_price": price, "subtotal_price": price, "currency": "USD", "financial_status": "paid",
                "customer": {"id": random.randint(1, 1000), "email": fake.email()},
                "line_items": [{"id": random.randint(1000,9999), "quantity": 1, "price": price}]
            })

    return {
        "products": products.to_dicts(),
        "orders": orders
    }