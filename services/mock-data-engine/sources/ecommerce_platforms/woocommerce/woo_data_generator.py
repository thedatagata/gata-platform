import polars as pl
from faker import Faker
import random
import json
from typing import Dict, List, Any

fake = Faker()

def generate_woocommerce_data(tenant_slug: str, config: Any, stripe_charges: List[dict]) -> Dict[str, List[dict]]:
    """
    Generates WooCommerce Orders matched 1:1 with Stripe Charges.
    Embeds stripe_charge_id in meta_data JSON string for robust joining.
    """
    # 1. Generate Products Catalog
    product_count = getattr(config, 'product_count', 20)
    product_ids = [random.randint(1000000000, 9999999999) for _ in range(product_count)]
    
    products = pl.DataFrame({
        "id": product_ids,
        "name": [fake.catch_phrase().title() for _ in product_ids],
        "slug": [fake.slug() for _ in product_ids],
        "type": "simple",
        "status": "publish",
        "price": "10.00",
        "regular_price": "12.00",
        "sku": [fake.ean8() for _ in product_ids],
        "stock_quantity": [random.randint(0, 100) for _ in product_ids],
        "categories": [json.dumps(["General"]) for _ in product_ids],
        "created_date_gmt": [fake.date_time_this_year() for _ in product_ids]
    })

    # 2. Generate Orders from Stripe Charges
    orders = []
    
    # Handle case where no charges exist
    if not stripe_charges:
        return {"raw_woocommerce_products": products.to_dicts(), "raw_woocommerce_orders": []}

    customer_pool = [{"id": random.randint(1, 10000), "email": fake.email(), "first": fake.first_name(), "last": fake.last_name()} for _ in range(int(len(stripe_charges) * 0.7))]
    
    for charge in stripe_charges:
        # 30% New Customer, 70% Returning (from pool)
        if customer_pool and random.random() > 0.3:
            cust = random.choice(customer_pool)
        else:
            cust = {"id": random.randint(10001, 99999), "email": fake.email(), "first": fake.first_name(), "last": fake.last_name()}
            customer_pool.append(cust)

        order_id = random.randint(1000000000, 9999999999)
        amount = charge.get('amount', 0)
        price_str = str(amount / 100)
        created_at = charge.get('created', fake.date_time_this_year())
        charge_id = charge.get('id', '')
        
        # Meta Data JSON String with Stripe Link
        meta_data_list = [
            {"key": "_stripe_charge_id", "value": charge_id},
            {"key": "_billing_phone", "value": fake.phone_number()}
        ]
        
        line_items = [{
            "id": random.randint(1000,9999),
            "product_id": random.choice(product_ids) if product_ids else None,
            "quantity": 1,
            "subtotal": price_str,
            "total": price_str
        }]

        orders.append({
            "id": order_id,
            "number": f"{random.randint(1000, 99999)}",
            "status": "completed",
            "currency": "USD",
            "total": price_str,
            "subtotal": price_str,
            "total_tax": "0.00",
            "payment_method": "stripe",
            "payment_method_title": "Credit Card (Stripe)",
            "date_created_gmt": created_at,
            "date_modified_gmt": created_at,
            "billing_email": cust['email'],
            "billing_first_name": cust['first'],
            "billing_last_name": cust['last'],
            "customer_id": cust['id'],
            "line_items": json.dumps(line_items),
            "meta_data": json.dumps(meta_data_list)
        })

    # Return with 'raw_{tenant}_{source}_{table}' naming convention handled by orchestrator? 
    # Or just returning raw dicts and orchestrator handles naming?
    # Checking prompt: "Table names: raw_{tenant_slug}_woocommerce_products, raw_{tenant_slug}_woocommerce_orders"
    # Usually generators return dict keys matching the desired suffixes or full keys?
    # Looking at shopify generator: it returns {"products": ..., "orders": ...}.
    # Orchestrator likely prefixes them.
    # I will return keys "products" and "orders" to match logical names.
    return {
        "products": products.to_dicts(),
        "orders": orders
    }
