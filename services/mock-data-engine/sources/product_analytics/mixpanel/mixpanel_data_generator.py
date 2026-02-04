
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_mixpanel_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Mixpanel data.
    Objects: events, people (users)
    """
    n_users = config.unique_user_base or 1000
    daily_events = config.daily_event_count or 5000
    
    # 1. People
    distinct_ids = [f"mp_user_{fake.uuid4()[:8]}" for _ in range(n_users)]
    people_df = pl.DataFrame({
        "$distinct_id": distinct_ids,
        "$city": [fake.city() for _ in range(n_users)],
        "$email": [fake.ascii_email() for _ in range(n_users)]
    })

    # 2. Events ($export)
    event_names = ["Screen View", "Sign Up", "Purchase", "Button Click"]
    total_events = daily_events * days
    
    # Create simple event log
    events_df = pl.DataFrame({
        "event": np.random.choice(event_names, total_events),
        "properties": [{
            "distinct_id": np.random.choice(distinct_ids),
            "time": int(fake.unix_time()),
            "$browser": "Chrome"
        } for _ in range(total_events)]
    })
    # Warning: Mixpanel schema is messy (nested props). Keeping it simple here.
    # To facilitate dlt/duckdb loading, we might flatten properties or just keep as struct/json?
    # For now, let's just make 'properties' a JSON string or dict. Dlt handles dicts.

    return {
        "events": events_df.to_dicts(),
        "people": people_df.to_dicts()
    }
