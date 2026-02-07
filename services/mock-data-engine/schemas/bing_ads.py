from pydantic import BaseModel
from typing import Optional
from datetime import date

class BingCampaign(BaseModel):
    Id: Optional[str] = None
    Name: Optional[str] = None
    Status: Optional[str] = None

class BingAdGroup(BaseModel):
    Id: Optional[str] = None
    CampaignId: Optional[str] = None
    Name: Optional[str] = None

class BingAd(BaseModel):
    Id: Optional[str] = None
    AdGroupId: Optional[str] = None
    Type: Optional[str] = None
    Title: Optional[str] = None

class BingAccountPerformanceReport(BaseModel):
    TimePeriod: Optional[date] = None
    Spend: Optional[float] = None
    Impressions: Optional[int] = None
    Clicks: Optional[int] = None
    AccountName: Optional[str] = None