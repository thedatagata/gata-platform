# services/mock-data-engine/sources/digital_analytics/google_analytics/ga4_data_generator.py
import random
from faker import Faker
from datetime import timedelta, datetime
from typing import Dict, List, Any

fake = Faker()

def generate_ga4_data(tenant_slug: str, config: Any, shopify_orders: List[dict], ad_campaigns: List[dict]) -> Dict[str, List[dict]]:
    events = []
    
    # Lever retrieval with None protection
    organic_ratio = getattr(config, 'organic_traffic_ratio', 0.5)
    if organic_ratio is None: organic_ratio = 0.5
    
    # --- 1. Generate Successful Sessions (Attributed Orders) ---
    for order in shopify_orders:
        is_paid = random.random() > organic_ratio
        
        source = {}
        landing_url = f"https://{tenant_slug}.com/"
        
        if is_paid and ad_campaigns:
            camp = random.choice(ad_campaigns)
            source = {"source": "facebook", "medium": "cpc", "campaign": camp['name']}
            landing_url += f"?utm_source=facebook&utm_medium=cpc&utm_campaign={camp['name']}"
        else:
            source = {"source": "(direct)", "medium": "(none)", "campaign": "(direct)"}
            
        user_pseudo_id = f"ga_{fake.uuid4()}"
        ts_micros = int(order['created_at'].timestamp() * 1_000_000)
        date_str = order['created_at'].strftime("%Y%m%d")
        session_id = str(random.randint(100000, 999999))
        
        events.append({
            "event_name": "session_start",
            "event_date": date_str,
            "event_timestamp": ts_micros - 5000000,
            "user_pseudo_id": user_pseudo_id,
            "traffic_source": source,
            "geo": {"country": "United States", "city": fake.city()},
            "event_params": [{"key": "page_location", "value": landing_url}, {"key": "ga_session_id", "value": session_id}]
        })
        
        events.append({
            "event_name": "purchase",
            "event_date": date_str,
            "event_timestamp": ts_micros,
            "user_pseudo_id": user_pseudo_id,
            "traffic_source": source,
            "geo": {"country": "United States", "city": fake.city()},
            "ecommerce": {"transaction_id": str(order['id']), "value": float(order['total_price']), "currency": "USD"},
            "event_params": [{"key": "page_location", "value": f"{landing_url}/thank_you"}, {"key": "ga_session_id", "value": session_id}]
        })

    # --- 2. Generate 'Noise' Traffic ---
    conv_rate = getattr(config, 'conversion_rate', 0.05)
    if conv_rate is None or conv_rate == 0: conv_rate = 0.05
    
    total_sessions_needed = int(len(shopify_orders) / conv_rate) if shopify_orders else 100
    noise_sessions = max(total_sessions_needed - len(shopify_orders), 10)
    start_date = shopify_orders[0]['created_at'] if shopify_orders else datetime.now()
    
    for _ in range(noise_sessions):
        session_time = fake.date_time_between(start_date=start_date - timedelta(days=30), end_date="now")
        ts_micros = int(session_time.timestamp() * 1_000_000)
        
        is_paid = random.random() > organic_ratio
        if is_paid and ad_campaigns:
            camp = random.choice(ad_campaigns)
            source = {"source": "facebook", "medium": "cpc", "campaign": camp['name']}
        else:
            source = {"source": "google", "medium": "organic", "campaign": "(organic)"}
            
        events.append({
            "event_name": "session_start",
            "event_date": session_time.strftime("%Y%m%d"),
            "event_timestamp": ts_micros,
            "user_pseudo_id": f"ga_{fake.uuid4()}",
            "traffic_source": source,
            "geo": {"country": "United States", "city": fake.city()},
            "event_params": [{"key": "ga_session_id", "value": str(random.randint(100000, 999999))}]
        })
        
    return {"events": events}