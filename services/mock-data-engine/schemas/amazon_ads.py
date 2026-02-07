from pydantic import BaseModel
from typing import Optional

class AmzSPCampaign(BaseModel):
    campaignId: Optional[str] = None
    name: Optional[str] = None
    state: Optional[str] = None
    dailyBudget: Optional[float] = None

class AmzSPAdGroup(BaseModel):
    adGroupId: Optional[str] = None
    campaignId: Optional[str] = None
    name: Optional[str] = None
    state: Optional[str] = None

class AmzSPAd(BaseModel):
    adId: Optional[str] = None
    adGroupId: Optional[str] = None
    campaignId: Optional[str] = None
    sku: Optional[str] = None
    state: Optional[str] = None