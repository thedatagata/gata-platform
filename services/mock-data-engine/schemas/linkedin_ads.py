from pydantic import BaseModel
from datetime import date
from typing import Optional

class LinkedInCampaign(BaseModel):
    id: str
    name: str
    status: str  # ACTIVE, PAUSED
    type: str  # SPONSORED_UPDATES, TEXT_AD, SPONSORED_INMAILS
    objective_type: str  # LEAD_GENERATION, BRAND_AWARENESS, WEBSITE_VISITS
    account_id: str
    daily_budget_amount: str
    currency_code: str = "USD"
    created_at: str

class LinkedInAdAnalytics(BaseModel):
    campaign_id: str
    date_range_start: date
    impressions: int
    clicks: int
    cost_in_local_currency: str  # string amount like "45.23"
    conversions: int
    leads: int = 0
    one_click_leads: int = 0
    external_website_conversions: int = 0
