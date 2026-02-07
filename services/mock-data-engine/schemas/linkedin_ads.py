from pydantic import BaseModel
from datetime import date
from typing import Optional

class LinkedInCampaign(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    status: Optional[str] = None
    type: Optional[str] = None
    objective_type: Optional[str] = None
    account_id: Optional[str] = None
    daily_budget_amount: Optional[str] = None
    currency_code: Optional[str] = "USD"
    created_at: Optional[str] = None

class LinkedInAdAnalytics(BaseModel):
    campaign_id: Optional[str] = None
    date_range_start: Optional[date] = None
    impressions: Optional[int] = None
    clicks: Optional[int] = None
    cost_in_local_currency: Optional[float] = None
    conversions: Optional[int] = None
    leads: Optional[int] = 0
    one_click_leads: Optional[int] = 0
    external_website_conversions: Optional[int] = 0
