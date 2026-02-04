from pydantic import BaseModel
from typing import List, Optional, Any

class GA4Event(BaseModel):
    event_date: str
    event_timestamp: int
    event_name: str
    event_params: List[dict]
    user_pseudo_id: str
    user_id: Optional[str] = None
    geo: Optional[dict] = None
    traffic_source: Optional[dict] = None
    ecommerce: Optional[dict] = None
    device: Optional[dict] = None