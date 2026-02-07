from pydantic import BaseModel
from typing import List, Optional, Any

class GA4Event(BaseModel):
    event_date: Optional[str] = None
    event_timestamp: Optional[int] = None
    event_name: Optional[str] = None
    event_params: Optional[List[dict]] = None
    user_pseudo_id: Optional[str] = None
    user_id: Optional[str] = None
    geo: Optional[dict] = None
    traffic_source: Optional[dict] = None
    ecommerce: Optional[dict] = None
    device: Optional[dict] = None