from pydantic import BaseModel
from datetime import date
from typing import Optional, List

# --- Dimensions ---

class GAdsCustomer(BaseModel):
    resource_name: str # customers/{customer_id}
    id: str
    descriptive_name: str
    currency_code: str
    time_zone: str

class GAdsCampaign(BaseModel):
    resource_name: str # customers/{id}/campaigns/{id}
    id: str
    name: str
    status: str
    advertising_channel_type: str # SEARCH, DISPLAY, PERFORMANCE_MAX

class GAdsAdGroup(BaseModel):
    resource_name: str
    id: str
    campaign_id: str
    name: str
    status: str
    type: str # SEARCH_STANDARD

class GAdsAd(BaseModel):
    resource_name: str
    id: str
    ad_group_id: str
    name: Optional[str] # Often constructed from headlines
    final_urls: List[str]
    status: str

# --- Fact Table (Ad Performance) ---

class GAdsAdPerformance(BaseModel):
    date: date
    customer_id: str
    campaign_id: str
    ad_group_id: str
    ad_id: str
    cost_micros: int
    impressions: int
    clicks: int
    conversions: float