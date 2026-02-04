import polars as pl
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_stripe_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Stripe Charges based on daily_orders_mean and aov_mean.
    """
    # Total expected orders
    total_orders = int(config.daily_orders_mean * days)
    
    # Generate IDs and Timestamps
    charge_ids = [f"ch_{fake.uuid4().replace('-', '')[:24]}" for _ in range(total_orders)]
    
    # Distribute timestamps across the window
    start_ts = datetime.now() - timedelta(days=days)
    timestamps = [
        fake.date_time_between(start_date=start_ts, end_date="now") 
        for _ in range(total_orders)
    ]
    timestamps.sort() # Sort for realism
    
    # Generate Amounts (LogNormal distribution for realistic cart sizes)
    # AOV is in dollars, Stripe needs cents
    aov_cents = config.aov_mean * 100
    amounts = np.random.lognormal(mean=np.log(aov_cents), sigma=0.4, size=total_orders).astype(int)
    
    charges = pl.DataFrame({
        "id": charge_ids,
        "amount": amounts,
        "amount_captured": amounts,
        "amount_refunded": 0,
        "currency": "usd",
        "created": timestamps,
        "status": "succeeded",
        "paid": True,
        "refunded": False,
        "payment_method_details": [{"type": "card", "card": {"brand": "visa", "last4": "4242"}}] * total_orders
    })

    return {"charges": charges.to_dicts()}