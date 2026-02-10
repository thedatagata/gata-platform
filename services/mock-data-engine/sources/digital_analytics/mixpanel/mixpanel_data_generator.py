import polars as pl
import numpy as np
from faker import Faker
import random
from typing import Dict, List, Any

fake = Faker()

def generate_mixpanel_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    n_users = getattr(config, 'unique_user_base', 1000) or 1000
    daily_events = getattr(config, 'daily_event_count', 50) or 50
    
    distinct_ids = [f"mp_user_{fake.uuid4()[:8]}" for _ in range(n_users)]
    people = []
    for uid in distinct_ids:
        people.append({
            "distinct_id": uid,
            "city": fake.city(),
            "email": fake.ascii_email()
        })

    event_names = ["Screen View", "Sign Up", "Purchase", "Button Click"]
    total_events = int(daily_events * days)
    
    # Build user profiles with stable attributes
    user_profiles = {}
    for uid in distinct_ids:
        user_profiles[uid] = {
            "country": fake.country_code(),
            "device": random.choice(["desktop", "mobile", "tablet"]),
        }

    from datetime import datetime, timedelta
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)

    utm_sources = ["google", "facebook", "email", "direct", None]
    utm_mediums = ["cpc", "organic", "email", "referral", None]
    utm_campaigns = ["summer_sale", "brand_awareness", "retargeting", None]

    events = []
    for _ in range(total_events):
        uid = random.choice(distinct_ids)
        profile = user_profiles[uid]
        # Spread timestamps across the full date range
        random_dt = start_date + timedelta(seconds=random.randint(0, days * 86400))
        
        # MANUAL FLATTENING: Move keys from properties dict to top level
        events.append({
            "event": random.choice(event_names),
            "prop_distinct_id": uid,
            "prop_time": int(random_dt.timestamp()),
            "prop_browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "prop_city": fake.city(),
            "prop_country_code": profile["country"],
            "prop_device_type": profile["device"],
            "prop_utm_source": random.choice(utm_sources),
            "prop_utm_medium": random.choice(utm_mediums),
            "prop_utm_campaign": random.choice(utm_campaigns),
        })

    return {"events": events, "people": people}
