from pydantic import BaseModel
from typing import Optional, Literal
from datetime import date

class FBCampaign(BaseModel):
    id: str
    name: str
    objective: str
    status: Literal["ACTIVE", "PAUSED", "ARCHIVED"]
    
class FBAdSet(BaseModel):
    id: str
    campaign_id: str
    name: str
    status: str
    daily_budget: Optional[float]
    
class FBAd(BaseModel):
    id: str
    adset_id: str
    campaign_id: str
    name: str
    creative_id: str
    status: str

# --- Fact Table ---

class FBAdInsight(BaseModel):
    date_start: date
    campaign_id: str
    adset_id: str
    ad_id: str
    spend: float
    impressions: int
    clicks: int
    conversions: int
    cpc: Optional[float] = None
    cpm: Optional[float] = None
    ctr: Optional[float] = None