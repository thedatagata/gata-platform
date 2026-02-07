from pydantic import BaseModel
from datetime import date
from typing import Optional, List

class GAdsCustomer(BaseModel):
    resource_name: Optional[str] = None
    id: Optional[str] = None
    descriptive_name: Optional[str] = None
    currency_code: Optional[str] = None
    time_zone: Optional[str] = None

class GAdsCampaign(BaseModel):
    resource_name: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    advertising_channel_type: Optional[str] = None

class GAdsAdGroup(BaseModel):
    resource_name: Optional[str] = None
    id: Optional[str] = None
    campaign_id: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    type: Optional[str] = None

class GAdsAd(BaseModel):
    resource_name: Optional[str] = None
    id: Optional[str] = None
    ad_group_id: Optional[str] = None
    name: Optional[str] = None
    final_urls: Optional[List[str]] = None
    status: Optional[str] = None

class GAdsAdPerformance(BaseModel):
    date_start: Optional[date] = None
    customer_id: Optional[str] = None
    campaign_id: Optional[str] = None
    ad_group_id: Optional[str] = None
    ad_id: Optional[str] = None
    cost_micros: Optional[int] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    conversions: Optional[float] = None