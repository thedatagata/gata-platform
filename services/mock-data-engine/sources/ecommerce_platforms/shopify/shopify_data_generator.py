import polars as pl
from faker import Faker
import random
from typing import Dict, List, Any

fake = Faker()

def generate_shopify_data(tenant_slug: str, config: Any, stripe_charges: List[dict]) -> Dict[str, List[dict]]:
    """
    Generates Shopify Orders matched 1:1 with Stripe Charges.
    """
    # 1. Generate Products Catalog
    product_ids = [random.randint(1000000000, 9999999999) for _ in range(config.product_count)]
    products = pl.DataFrame({
        "id": product_ids,
        # FIXED: Replaced non-existent 'commerce_product_name' with 'catch_phrase'
        "title": [fake.catch_phrase().title() for _ in product_ids],
        "product_type": "Mock Product",
        "status": "active",
        "created_at": [fake.date_time_this_year() for _ in product_ids],
        "variants": [[{"id": random.randint(100,999), "price": "10.00"}] for _ in product_ids]
    })

    # 2. Generate Orders from Stripe Charges
    orders = []
    
    # Handle case where no charges exist to avoid errors
    if not stripe_charges:
        return {"products": products.to_dicts(), "orders": []}

    customer_pool = [{"id": random.randint(1, 10000), "email": fake.email()} for _ in range(int(len(stripe_charges) * 0.7))]
    
    for charge in stripe_charges:
        # 30% New Customer, 70% Returning (from pool)
        if customer_pool and random.random() > 0.3:
            customer = random.choice(customer_pool)
        else:
            customer = {"id": random.randint(10001, 99999), "email": fake.email()}
            customer_pool.append(customer)

        order_id = random.randint(1000000000, 9999999999)
        # Use .get() for safety, though keys should exist
        amount = charge.get('amount', 0)
        price_str = str(amount / 100)
        created_at = charge.get('created', fake.date_time_this_year())
        charge_id = charge.get('id', '')
        
        orders.append({
            "id": order_id,
            "name": f"#{random.randint(1000, 99999)}",
            "email": customer['email'],
            "created_at": created_at,
            "processed_at": created_at,
            "updated_at": created_at,
            "total_price": price_str,
            "subtotal_price": price_str,
            "currency": "USD",
            "financial_status": "paid",
            "payment_gateway_names": ["stripe"],
            "customer": customer,
            # LINKING KEY: This allows us to join Shopify -> Stripe
            "note_attributes": [{"name": "stripe_charge_id", "value": charge_id}], 
            "line_items": [{
                "id": random.randint(1000,9999),
                "product_id": random.choice(product_ids) if product_ids else None,
                "quantity": 1,
                "price": price_str
            }]
        })

    return {
        "products": products.to_dicts(),
        "orders": orders
    }