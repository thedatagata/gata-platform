import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_google_ads(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    customer_id = str(fake.random_number(digits=10, fix_len=True))
    customer = pl.DataFrame({
        "resource_name": [f"customers/{customer_id}"],
        "id": [customer_id],
        "descriptive_name": [f"{tenant_slug.replace('_', ' ').title()} Search Account"],
        "currency_code": ["USD"],
        "time_zone": ["America/New_York"]
    })

    n_campaigns = getattr(config, 'campaign_count', 5) or 5 
    campaign_ids = [str(fake.random_number(digits=10, fix_len=True)) for _ in range(n_campaigns)]
    
    campaigns = pl.DataFrame({
        "resource_name": [f"customers/{customer_id}/campaigns/{cid}" for cid in campaign_ids],
        "id": campaign_ids,
        "name": [f"Search | {fake.word().title()} | {fake.year()}" for _ in range(n_campaigns)],
        "status": "ENABLED",
        "advertising_channel_type": "SEARCH"
    })

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
                "name": f"Ad: {fake.catch_phrase()}",
                "final_urls": [f"https://{tenant_slug}.com/landing/{fake.slug()}"],
                "status": "ENABLED"
            })
    ads = pl.DataFrame(ads_list)

    ad_lineage = (
        ads.lazy()
        .join(ad_groups.lazy().select(["id", "campaign_id"]), left_on="ad_group_id", right_on="id")
        .select(["id", "ad_group_id", "campaign_id"])
        .collect()
    )

    dates = pl.date_range(
        start=date.today() - timedelta(days=days),
        end=date.today(),
        interval="1d",
        eager=True
    ).alias("date_start")

    stats = (
        ad_lineage
        .rename({"id": "ad_id"})
        .join(dates.to_frame(), how="cross")
    )
    
    n_rows = len(stats)
    daily_spend_mean = getattr(config, 'daily_spend_mean', 500.0) or 500.0
    per_ad_mean = daily_spend_mean / len(ads)
    spend_micros = np.random.lognormal(mean=np.log(per_ad_mean), sigma=1.0, size=n_rows) * 1_000_000
    
    cpc_mean = getattr(config, 'cpc_mean', 1.2) or 1.2
    cpc_actual = np.random.normal(cpc_mean, 0.5, n_rows)
    cpc_actual = np.maximum(cpc_actual, 0.10)
    clicks = (spend_micros / 1_000_000 / cpc_actual).astype(int)
    
    ctr_actual = np.random.beta(2, 100, n_rows)
    impressions = (clicks / ctr_actual).astype(int)
    impressions = np.maximum(impressions, clicks) 

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