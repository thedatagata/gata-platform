"""
Shared constants for the mock data simulation engine.

This file contains ONLY:
    - Funnel event step names (shared schema contract)
    - Timing parameters (universal physics)
    - Utility helpers

Everything else comes from the source that owns it:
    - Funnel probabilities     → tenants.yaml generation.funnel block
    - Campaigns, UTMs          → paid ad generators
    - Products, categories     → ecommerce generators
    - Device/geo/landing pages → analytics generators
"""
import random
from typing import Dict, List, Any

SEED = 42

# ==========================================
# ECOMMERCE FUNNEL STEP DEFINITIONS
# ==========================================
# This is the shared schema contract. Every analytics source formats
# its events to align with these step names.
FUNNEL_EVENTS = [
    "session_start",
    "view_item",
    "add_to_cart",
    "begin_checkout",
    "add_payment_info",
    "purchase",
]

# ==========================================
# TIMING PARAMETERS
# ==========================================
# Within a session: time between funnel steps (1 min to 30 min)
INTRA_SESSION_DELAY_SECONDS = (60, 1800)

# Between sessions: time before a returning user comes back (31 min to 7 days)
INTER_SESSION_DELAY_SECONDS = (1860, 604800)


# ==========================================
# UTILITY HELPERS
# ==========================================
def pick_weighted(items: List[Dict], weight_key: str = "weight", rng: random.Random = None) -> Dict:
    """Pick a random item from a list of dicts using weighted probability."""
    r = rng or random
    weights = [item[weight_key] for item in items]
    return r.choices(items, weights=weights, k=1)[0]
