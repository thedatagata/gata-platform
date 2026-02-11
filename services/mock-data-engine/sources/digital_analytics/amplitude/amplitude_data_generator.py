import polars as pl
import numpy as np
from faker import Faker
import random
from datetime import date, timedelta, datetime
from typing import Dict, List, Any

fake = Faker()

def generate_amplitude_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Amplitude events and users using Dense Defaults for DNA stability.
    Bypasses structural 'None' issues by filling joined fields with typed placeholders.
    """
    n_users = getattr(config, 'unique_user_base', 1000) or 1000
    daily_events = getattr(config, 'daily_event_count', 100) or 100
    
    # 1. Generate Users
    user_ids = [f"user_{fake.uuid4()[:8]}" for _ in range(n_users)]
    device_types = ["Apple iPhone", "Samsung Galaxy", "Chrome", "Firefox"]
    
    users_df = pl.DataFrame({
        "user_id": user_ids,
        "device_type": np.random.choice(device_types, n_users),
        "country": [fake.country_code() for _ in range(n_users)]
    })

    # 2. Generate Events
    event_types = ["view_item", "add_to_cart", "checkout_start", "purchase", "page_view"]
    total_events = int(daily_events * days)
    
    # Calculate intervals for timestamp generation
    if total_events > 0:
        interval_seconds = max(1, int((days * 24 * 60 * 60) / total_events))
    else:
        interval_seconds = 1
        
    start_dt = datetime.combine(date.today() - timedelta(days=days), datetime.min.time())
    event_times = [start_dt + timedelta(seconds=i * interval_seconds) for i in range(total_events)]
    
    # Construct base event log
    events_df = pl.DataFrame({
        "event_id": [fake.uuid4() for _ in range(total_events)],
        "event_type": np.random.choice(event_types, total_events),
        "user_id": np.random.choice(user_ids, total_events),
        "event_time": pl.Series(event_times).dt.to_string("%Y-%m-%d %H:%M:%S")
    })
    
    # 3. Join User Properties and Apply Dense Defaults
    # We join user metadata and immediately fill any nulls to ensure dlt sees consistent types.
    events_df = (
        events_df.join(users_df, on="user_id", how="left")
        .with_columns([
            pl.col("device_type").fill_null("Unknown"), # DENSE DEFAULT
            pl.col("country").fill_null("N/A")           # DENSE DEFAULT
        ])
    )

    return {
        "events": events_df.to_dicts(),
        "users": users_df.to_dicts()
    }
