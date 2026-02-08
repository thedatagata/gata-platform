import random
from faker import Faker
from datetime import datetime
from typing import Dict, List, Any

fake = Faker()

def generate_ga4_data(tenant_slug: str, config: Any, ecommerce_orders: List[dict], ad_campaigns: List[dict]) -> Dict[str, List[dict]]:
    """Generates GA4 events with manual flattening and dense defaults for DNA stability."""
    events = []
    
    def get_base_event(name, dt_obj):
        # DENSE DEFAULTS: Every key MUST be present in every row
        return {
            "event_name": name,
            "event_date": dt_obj.strftime("%Y%m%d"),
            "event_timestamp": int(dt_obj.timestamp() * 1_000_000),
            "user_pseudo_id": f"ga_{fake.uuid4()}",
            "user_id": f"u_{random.randint(1, 1000)}",
            "geo_country": "US",
            "geo_city": fake.city(),
            "traffic_source_source": "(direct)",
            "traffic_source_medium": "(none)",
            "traffic_source_campaign": "(direct)",
            "device_category": "mobile",
            "ga_session_id": str(random.randint(100, 999)),
            # Flattened fields use "N/A" or 0.0 to ensure type inference
            "ecommerce_transaction_id": "N/A",
            "ecommerce_value": 0.0,
            "ecommerce_currency": "USD"
        }

    # 1. High-volume noise
    for _ in range(50):
        events.append(get_base_event("session_start", datetime.now()))

    # 2. Purchases (Overwrites placeholders with real context)
    for order in ecommerce_orders:
        evt = get_base_event("purchase", order['created_at'])
        evt.update({
            "ecommerce_transaction_id": str(order['id']),
            "ecommerce_value": float(order['total_price']),
            "ecommerce_currency": "USD"
        })
        if ad_campaigns:
            evt.update({
                "traffic_source_source": "facebook",
                "traffic_source_medium": "cpc",
                "traffic_source_campaign": ad_campaigns[0].get('name', 'Direct')
            })
        events.append(evt)

    return {"events": events}