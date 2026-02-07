from pydantic import BaseModel
from typing import Optional
from datetime import date

class TikTokCampaign(BaseModel):
    campaign_id: Optional[str] = None
    campaign_name: Optional[str] = None
    objective_type: Optional[str] = None
    status: Optional[str] = None
    create_time: Optional[str] = None

class TikTokAdGroup(BaseModel):
    adgroup_id: Optional[str] = None
    campaign_id: Optional[str] = None
    adgroup_name: Optional[str] = None
    status: Optional[str] = None

class TikTokAd(BaseModel):
    ad_id: Optional[str] = None
    adgroup_id: Optional[str] = None
    campaign_id: Optional[str] = None
    ad_name: Optional[str] = None
    status: Optional[str] = None

class TikTokAdReportDaily(BaseModel):
    stat_time_day: Optional[date] = None
    ad_id: Optional[str] = None
    adgroup_id: Optional[str] = None
    campaign_id: Optional[str] = None
    spend: Optional[float] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    conversions: Optional[int] = None