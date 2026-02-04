
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta, datetime
from typing import Dict, List, Any

fake = Faker()

def generate_amplitude_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Amplitude data.
    Objects: events, users (optional context)
    """
    n_users = config.unique_user_base or 1000
    daily_events = config.daily_event_count or 5000
    
    # 1. Users
    user_ids = [f"user_{fake.uuid4()[:8]}" for _ in range(n_users)]
    device_types = ["Apple iPhone", "Samsung Galaxy", "Chrome", "Firefox"]
    
    users_df = pl.DataFrame({
        "user_id": user_ids,
        "device_type": np.random.choice(device_types, n_users),
        "country": [fake.country_code() for _ in range(n_users)]
    })

    # 2. Events
    # Generate streams of events
    event_types = ["view_item", "add_to_cart", "checkout_start", "purchase", "page_view"]
    total_events = daily_events * days
    
    interval_val = (days * 24 * 60 * 60) / total_events
    interval_seconds = int(interval_val)
    if interval_seconds < 1:
        interval_seconds = 1
        
    # Convert to datetime for seconds precision
    start_dt = datetime.combine(date.today() - timedelta(days=days), datetime.min.time())
    end_dt = datetime.combine(date.today(), datetime.min.time())
    
    # Randomly distribute dates - Manual Generation to avoid Polars date_range issues
    event_times = [start_dt + timedelta(seconds=i * interval_seconds) for i in range(total_events)]
    
    event_dates = pl.Series("event_time", event_times)
    
    # If explicit count needed, sample
    if len(event_dates) > total_events:
        event_dates = event_dates.sample(total_events)
    
    # Construct huge event log
    n_real_events = len(event_dates)
    events_df = pl.DataFrame({
        "event_id": [fake.uuid4() for _ in range(n_real_events)],
        "event_type": np.random.choice(event_types, n_real_events),
        "user_id": np.random.choice(user_ids, n_real_events),
        "event_time": event_dates.dt.to_string("%Y-%m-%d %H:%M:%S")
    })
    
    # Join user props
    events_df = events_df.join(users_df, on="user_id", how="left")

    return {
        "events": events_df.to_dicts(),
        "users": users_df.to_dicts()
    }
