from faker import Faker
import random
from typing import Dict, List, Any
import polars as pl
from datetime import timedelta, date

fake = Faker()

def generate_linkedin_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    campaign_count = getattr(config, 'campaign_count', 5)
    daily_spend_mean = getattr(config, 'daily_spend_mean', 500.0)
    
    campaigns = []
    for _ in range(campaign_count):
        campaigns.append({
            "id": str(random.randint(1000000, 9999999)),
            "name": f"{fake.catch_phrase()} - {random.choice(['Awareness', 'Leads', 'Conversions'])}",
            "status": "ACTIVE",
            "type": "SPONSORED_UPDATES",
            "objective_type": "LEAD_GENERATION",
            "account_id": str(random.randint(500000, 999999)),
            "daily_budget_amount": str(daily_spend_mean / campaign_count),
            "currency_code": "USD",
            "created_at": fake.iso8601()
        })

    end_date = date.today()
    start_date = end_date - timedelta(days=days)
    analytics = []
    daily_campaign_spend = daily_spend_mean / campaign_count
    
    current_date = start_date
    while current_date <= end_date:
        for cam in campaigns:
            spend_noise = random.uniform(0.7, 1.3)
            spend = daily_campaign_spend * spend_noise
            impressions = int(spend * random.uniform(50, 150))
            clicks = int(impressions * random.uniform(0.005, 0.02))
            conversions = int(clicks * random.uniform(0.05, 0.15))
            
            analytics.append({
                "campaign_id": cam['id'],
                "date_range_start": current_date,
                "impressions": impressions,
                "clicks": clicks,
                "cost_in_local_currency": round(float(spend), 2),
                "conversions": conversions,
                "leads": conversions,
                "one_click_leads": int(conversions * 0.8),
                "external_website_conversions": int(conversions * 0.2)
            })
        current_date += timedelta(days=1)

    return {
        "campaigns": campaigns,
        "ad_analytics": analytics
    }