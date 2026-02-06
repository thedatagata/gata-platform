import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_tiktok_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates TikTok Ads data: Campaigns -> Ad Groups -> Ads -> Daily Reports.
    Aligns with TikTok Marketing API structure.
    """
    
    # --- 1. Campaigns ---
    n_campaigns = getattr(config, 'campaign_count', 3) or 3
    campaign_ids = [f"tt_camp_{fake.random_number(digits=12)}" for _ in range(n_campaigns)]
    
    campaigns_df = pl.DataFrame({
        "campaign_id": campaign_ids,
        "campaign_name": [f"TT | {fake.word().upper()} | {fake.year()}" for _ in range(n_campaigns)],
        "objective_type": np.random.choice(["VIDEO_VIEWS", "CONVERSIONS", "TRAFFIC"], n_campaigns),
        "status": "ENABLE",
        "create_time": [fake.iso8601() for _ in range(n_campaigns)]
    })

    # --- 2. Ad Groups ---
    # Logic: 2-4 Ad Groups per Campaign
    ad_groups_list = []
    for cid in campaign_ids:
        n_groups = np.random.randint(2, 5)
        for _ in range(n_groups):
            ag_id = f"tt_ag_{fake.random_number(digits=12)}"
            ad_groups_list.append({
                "adgroup_id": ag_id,
                "campaign_id": cid,
                "adgroup_name": f"AG | {fake.bs().title()}",
                "status": "ENABLE"
            })
    ad_groups_df = pl.DataFrame(ad_groups_list)

    # --- 3. Ads ---
    # Logic: 3-5 Ads per Ad Group
    ads_list = []
    for ag in ad_groups_list:
        n_ads = np.random.randint(3, 6)
        for _ in range(n_ads):
            ad_id = f"tt_ad_{fake.random_number(digits=12)}"
            ads_list.append({
                "ad_id": ad_id,
                "adgroup_id": ag["adgroup_id"],
                "campaign_id": ag["campaign_id"],
                "ad_name": f"Creative: {fake.catch_phrase()}",
                "status": "ENABLE"
            })
    ads_df = pl.DataFrame(ads_list)

    # --- 4. Ads Reports Daily (Fact Table) ---
    dates = pl.date_range(
        start=date.today() - timedelta(days=days),
        end=date.today(),
        interval="1d",
        eager=True
    ).alias("stat_time_day")

    # Cross join Ads with Dates for daily granularity
    reports_df = (
        ads_df.select(["ad_id", "adgroup_id", "campaign_id"])
        .join(dates.to_frame(), how="cross")
    )

    n_rows = len(reports_df)
    
    # Distribute spend across ads/days
    daily_spend_mean = getattr(config, 'daily_spend_mean', 200.0) or 200.0
    per_ad_daily_mean = daily_spend_mean / (len(ads_df) if len(ads_df) > 0 else 1)
    
    spend = np.random.lognormal(mean=np.log(per_ad_daily_mean), sigma=0.6, size=n_rows)
    impressions = (spend * np.random.uniform(80, 200)).astype(int) # TikTok often has lower CPM
    clicks = (impressions * np.random.uniform(0.01, 0.04)).astype(int) # 1%-4% CTR
    conversions = (clicks * np.random.uniform(0.02, 0.12)).astype(int) # 2%-12% CVR

    reports_df = reports_df.with_columns([
        pl.Series(spend).alias("spend"),
        pl.Series(impressions).alias("impressions"),
        pl.Series(clicks).alias("clicks"),
        pl.Series(conversions).alias("conversions")
    ])

    return {
        "campaigns": campaigns_df.to_dicts(),
        "ad_groups": ad_groups_df.to_dicts(),
        "ads": ads_df.to_dicts(),
        "ads_reports_daily": reports_df.to_dicts()
    }