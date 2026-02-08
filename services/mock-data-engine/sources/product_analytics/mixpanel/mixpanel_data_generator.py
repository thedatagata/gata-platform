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
    
    events = []
    for _ in range(total_events):
        # MANUAL FLATTENING: Move keys from properties dict to top level
        events.append({
            "event": random.choice(event_names),
            "prop_distinct_id": random.choice(distinct_ids),
            "prop_time": int(fake.unix_time()),
            "prop_browser": "Chrome",
            "prop_city": fake.city()
        })

    return {"events": events, "people": people}
