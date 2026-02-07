from pydantic import BaseModel
from typing import Optional, Literal
from datetime import date

class MetaCampaign(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    objective: Optional[str] = None
    status: Optional[Literal["ACTIVE", "PAUSED", "ARCHIVED"]] = None
    
class MetaAdSet(BaseModel):
    id: Optional[str] = None
    campaign_id: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    daily_budget: Optional[float] = None
    
class MetaAd(BaseModel):
    id: Optional[str] = None
    adset_id: Optional[str] = None
    campaign_id: Optional[str] = None
    name: Optional[str] = None
    creative_id: Optional[str] = None
    status: Optional[str] = None

class MetaAdInsight(BaseModel):
    date_start: Optional[date] = None
    campaign_id: Optional[str] = None
    adset_id: Optional[str] = None
    ad_id: Optional[str] = None
    spend: Optional[float] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    conversions: Optional[int] = None
    cpc: Optional[float] = None
    cpm: Optional[float] = None
    ctr: Optional[float] = None