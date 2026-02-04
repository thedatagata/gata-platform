import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_facebook_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates consistent Facebook Ads data with relational integrity.
    Schema matches dlt verified source: campaigns, ad_sets, ads, facebook_insights.
    """
    
    # --- 1. Campaigns ---
    # Logic: Tyrell Corp (SaaS) runs more "Leads" campaigns; Cyberdyne (Defense) runs "Sales".
    objectives = ["OUTCOME_SALES", "OUTCOME_LEADS", "OUTCOME_AWARENESS"]
    n_campaigns = config.campaign_count
    
    campaign_ids = [f"{tenant_slug}_camp_{i}" for i in range(n_campaigns)]
    campaigns_df = pl.DataFrame({
        "id": campaign_ids,
        "name": [f"{tenant_slug.upper()} | {fake.catch_phrase()} | {fake.year()}" for _ in range(n_campaigns)],
        "objective": np.random.choice(objectives, n_campaigns),
        "status": "ACTIVE"
    })

    # --- 2. Ad Sets ---
    # Ad Sets belong to Campaigns
    n_adsets = 15
    adset_ids = [f"{tenant_slug}_adset_{i}" for i in range(n_adsets)]
    
    adsets_df = pl.DataFrame({
        "id": adset_ids,
        "campaign_id": np.random.choice(campaign_ids, n_adsets), # Link to Campaign
        "name": [f"Targeting: {fake.job()}" for _ in range(n_adsets)],
        "status": "ACTIVE",
        "daily_budget": np.random.uniform(50, 500, n_adsets)
    })

    # --- 3. Ads ---
    # Ads belong to Ad Sets (and inherit Campaign ID)
    n_ads = 50
    ad_ids = [f"{tenant_slug}_ad_{i}" for i in range(n_ads)]
    
    # Create a mapping of AdSet -> Campaign to ensure integrity
    parent_map = adsets_df.select(["id", "campaign_id"]).sample(n_ads, with_replacement=True)
    
    ads_df = pl.DataFrame({
        "id": ad_ids,
        "adset_id": parent_map["id"],
        "campaign_id": parent_map["campaign_id"],
        "name": [f"Creative: {fake.bs()}" for _ in range(n_ads)],
        "creative_id": [f"cr_{fake.uuid4()[:8]}" for _ in range(n_ads)],
        "status": "ACTIVE"
    })

    # --- 4. Insights (Fact Table) ---
    # Cross-join Ads with the Date Range to create daily rows
    dates = pl.date_range(
        start=date.today() - timedelta(days=days),
        end=date.today(),
        interval="1d",
        eager=True
    ).alias("date_start")
    
    # Base: Every active ad gets a row for every day
    insights_df = (
        ads_df.select(["id", "campaign_id", "adset_id"])
        .rename({"id": "ad_id"})
        .join(dates.to_frame(), how="cross")
    )
    
    n_rows = len(insights_df)
    
    # "The Levers": Generate metrics with some correlation
    # Spend is random; Impressions correlates with Spend; Clicks with Impressions
    # Distribute the 'daily_spend_mean' across the rows
    # Per-ad mean = Total Mean / (n_ads/2 approx active)
    # Simplified: Just scale the lognormal to hit the target roughly
    target_mean = config.daily_spend_mean / (len(ads_df) if len(ads_df) > 0 else 1)
    
    spend = np.random.lognormal(mean=np.log(target_mean), sigma=0.5, size=n_rows)
    impressions = (spend * np.random.uniform(10, 50)).astype(int) # ~$20 CPM base
    clicks = (impressions * np.random.uniform(0.005, 0.03)).astype(int) # 0.5% - 3% CTR
    conversions = (clicks * np.random.uniform(0.05, 0.20)).astype(int) # 5% - 20% CVR
    
    insights_df = insights_df.with_columns([
        pl.Series(spend).alias("spend"),
        pl.Series(impressions).alias("impressions"),
        pl.Series(clicks).alias("clicks"),
        pl.Series(conversions).alias("conversions"),
    ])

    # Convert to list of dicts for dlt
    return {
        "campaigns": campaigns_df.to_dicts(),
        "ad_sets": adsets_df.to_dicts(),
        "ads": ads_df.to_dicts(),
        "facebook_insights": insights_df.to_dicts()
    }