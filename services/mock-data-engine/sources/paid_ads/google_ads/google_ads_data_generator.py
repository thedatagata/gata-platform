import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_google_ads(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Google Ads data with full hierarchy:
    Customer -> Campaign -> AdGroup -> Ad -> Daily Metrics
    """
    
    # --- 1. Customer (Account) ---
    customer_id = str(fake.random_number(digits=10, fix_len=True))
    customer = pl.DataFrame({
        "resource_name": [f"customers/{customer_id}"],
        "id": [customer_id],
        "descriptive_name": [f"{tenant_slug.replace('_', ' ').title()} Search Account"],
        "currency_code": ["USD"],
        "time_zone": ["America/New_York"]
    })

    # --- 2. Campaigns ---
    # Fallback to 5 campaigns if configuration is missing
    n_campaigns = config.campaign_count or 5 
    campaign_ids = [str(fake.random_number(digits=10, fix_len=True)) for _ in range(n_campaigns)]
    
    campaigns = pl.DataFrame({
        "resource_name": [f"customers/{customer_id}/campaigns/{cid}" for cid in campaign_ids],
        "id": campaign_ids,
        "name": [f"Search | {fake.word().title()} | {fake.year()}" for _ in range(n_campaigns)],
        "status": "ENABLED",
        "advertising_channel_type": "SEARCH"
    })

    # --- 3. Ad Groups ---
    # Logic: Each Campaign has 3-5 Ad Groups
    ad_groups_list = []
    for cid in campaign_ids:
        n_groups = np.random.randint(3, 6)
        for _ in range(n_groups):
            ag_id = str(fake.random_number(digits=10, fix_len=True))
            ad_groups_list.append({
                "resource_name": f"customers/{customer_id}/adGroups/{ag_id}",
                "id": ag_id,
                "campaign_id": cid,
                "name": f"AG | {fake.bs()}",
                "status": "ENABLED",
                "type": "SEARCH_STANDARD"
            })
    
    ad_groups = pl.DataFrame(ad_groups_list)

    # --- 4. Ads ---
    # Logic: Each Ad Group has 2-4 Ads
    ads_list = []
    ag_ids = ad_groups["id"].to_list()
    
    for ag_id in ag_ids:
        n_ads = np.random.randint(2, 5)
        for _ in range(n_ads):
            ad_id = str(fake.random_number(digits=10, fix_len=True))
            ads_list.append({
                "resource_name": f"customers/{customer_id}/adGroupAds/{ag_id}~{ad_id}",
                "id": ad_id,
                "ad_group_id": ag_id,
                "name": f"Ad: {fake.catch_phrase()}", # Responsive Search Ad Headline
                "final_urls": [f"https://{tenant_slug}.com/landing/{fake.slug()}"],
                "status": "ENABLED"
            })
            
    ads = pl.DataFrame(ads_list)

    # --- 5. Ad Performance (The Fact Table) ---
    
    # Join Ads -> AdGroups -> Campaigns to get full lineage for the stats table
    # We need campaign_id for the stats table
    ad_lineage = (
        ads.lazy()
        .join(ad_groups.lazy().select(["id", "campaign_id"]), left_on="ad_group_id", right_on="id")
        .select(["id", "ad_group_id", "campaign_id"])
        .collect()
    )

    # Cross Join with Date Range
    dates = pl.date_range(
        start=date.today() - timedelta(days=days),
        end=date.today(),
        interval="1d",
        eager=True
    ).alias("date")

    # Base Metrics Table: Every Ad gets a row per day
    stats = (
        ad_lineage
        .rename({"id": "ad_id"})
        .join(dates.to_frame(), how="cross")
    )
    
    n_rows = len(stats)
    
    # Apply Levers
    # Distribute the 'daily_spend_mean' across the rows
    # Logic: Spend is LogNormal (some ads spend way more), correlated with Impressions
    
    # 1. Spend (micros)
    # We want the SUM of daily spend to approx equal config.daily_spend_mean
    # Per-ad mean = Total Mean / Total Ads
    per_ad_mean = config.daily_spend_mean / len(ads)
    spend_micros = np.random.lognormal(mean=np.log(per_ad_mean), sigma=1.0, size=n_rows) * 1_000_000
    
    # 2. Clicks (derived from Cost / CPC)
    # Avoid div by zero
    cpc_actual = np.random.normal(config.cpc_mean, 0.5, n_rows)
    cpc_actual = np.maximum(cpc_actual, 0.10) # Min CPC $0.10
    clicks = (spend_micros / 1_000_000 / cpc_actual).astype(int)
    
    # 3. Impressions (derived from Clicks / CTR)
    ctr_actual = np.random.beta(2, 100, n_rows) # Skewed towards low CTR (e.g. 2-5%)
    # Ensure impressions >= clicks
    impressions = (clicks / ctr_actual).astype(int)
    impressions = np.maximum(impressions, clicks) 

    # 4. Conversions (Rare event)
    conversions = (clicks * np.random.uniform(0.01, 0.10, n_rows)).astype(int)

    stats = stats.with_columns([
        pl.lit(customer_id).alias("customer_id"),
        pl.Series(spend_micros).cast(pl.Int64).alias("cost_micros"),
        pl.Series(impressions).alias("impressions"),
        pl.Series(clicks).alias("clicks"),
        pl.Series(conversions).alias("conversions")
    ])

    return {
        "customers": customer.to_dicts(),
        "campaigns": campaigns.to_dicts(),
        "ad_groups": ad_groups.to_dicts(),
        "ads": ads.to_dicts(),
        "ad_performance": stats.to_dicts()
    }