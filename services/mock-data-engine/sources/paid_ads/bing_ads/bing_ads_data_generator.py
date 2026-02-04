
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_bing_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Bing Ads data.
    Objects: campaigns, ad_groups, ads, account_performance_report
    """
    n_campaigns = config.campaign_count or 5
    n_adgroups = config.ad_group_count or 15

    # 1. Campaigns
    campaign_ids = [f"{tenant_slug}_bing_camp_{i}" for i in range(n_campaigns)]
    campaigns_df = pl.DataFrame({
        "Id": campaign_ids,
        "Name": [f"Bing | {tenant_slug.upper()} | {fake.catch_phrase()}" for _ in range(n_campaigns)],
        "Status": "Active"
    })

    # 2. Ad Groups
    adgroup_ids = [f"{tenant_slug}_bing_ag_{i}" for i in range(n_adgroups)]
    adgroups_df = pl.DataFrame({
        "Id": adgroup_ids,
        "CampaignId": np.random.choice(campaign_ids, n_adgroups),
        "Name": [f"Discovery: {fake.job()}" for _ in range(n_adgroups)]
    })

    # 3. Ads
    n_ads = n_adgroups * 3
    ad_ids = [f"{tenant_slug}_bing_ad_{i}" for i in range(n_ads)]
    parent_map = adgroups_df.select(["Id"]).sample(n_ads, with_replacement=True)
    
    ads_df = pl.DataFrame({
        "Id": ad_ids,
        "AdGroupId": parent_map["Id"],
        "Type": "Text Ad",
        "Title": [fake.sentence(nb_words=4) for _ in range(n_ads)]
    })

    # 4. Account Performance Report
    # High level daily stats
    dates = pl.date_range(
        start=date.today() - timedelta(days=days),
        end=date.today(),
        interval="1d",
        eager=True
    ).alias("TimePeriod")
    
    report_df = dates.to_frame()
    n_rows = len(report_df)
    
    spend = np.random.lognormal(mean=np.log(100), sigma=0.4, size=n_rows)
    impressions = (spend * np.random.uniform(15, 40)).astype(int)
    clicks = (impressions * np.random.uniform(0.02, 0.05)).astype(int)
    
    report_df = report_df.with_columns([
        pl.Series(spend).alias("Spend"),
        pl.Series(impressions).alias("Impressions"),
        pl.Series(clicks).alias("Clicks"),
        pl.lit(tenant_slug).alias("AccountName")
    ])

    return {
        "campaigns": campaigns_df.to_dicts(),
        "ad_groups": adgroups_df.to_dicts(),
        "ads": ads_df.to_dicts(),
        "account_performance_report": report_df.to_dicts()
    }
