
import polars as pl
import numpy as np
from faker import Faker
from datetime import date, timedelta
from typing import Dict, List, Any

fake = Faker()

def generate_amazon_data(tenant_slug: str, config: Any, days: int = 30) -> Dict[str, List[dict]]:
    """
    Generates Amazon Ads data (Sponsored Products).
    Objects: sponsored_products_campaigns, sponsored_products_ad_groups, sponsored_products_product_ads
    """
    n_campaigns = config.campaign_count or 5
    sp_ratio = config.sponsored_product_ratio or 0.8 # Used to weight budget
    
    # 1. Campaigns
    camp_ids = [f"{tenant_slug}_amz_sp_camp_{i}" for i in range(n_campaigns)]
    campaigns_df = pl.DataFrame({
        "campaignId": camp_ids,
        "name": [f"SP | {fake.bs()}" for _ in range(n_campaigns)],
        "state": "enabled",
        "dailyBudget": np.random.uniform(50, 500, n_campaigns)
    })
    
    # 2. Ad Groups
    n_adgroups = 10
    ag_ids = [f"{tenant_slug}_amz_sp_ag_{i}" for i in range(n_adgroups)]
    adgroups_df = pl.DataFrame({
        "adGroupId": ag_ids,
        "campaignId": np.random.choice(camp_ids, n_adgroups),
        "name": [f"AG: {fake.word()}" for _ in range(n_adgroups)],
        "state": "enabled"
    })
    
    # 3. Product Ads
    n_ads = 30
    ad_ids = [f"{tenant_slug}_amz_sp_ad_{i}" for i in range(n_ads)]
    parent_map = adgroups_df.select(["adGroupId", "campaignId"]).sample(n_ads, with_replacement=True)
    
    ads_df = pl.DataFrame({
        "adId": ad_ids,
        "adGroupId": parent_map["adGroupId"],
        "campaignId": parent_map["campaignId"],
        "sku": [f"SKU-{fake.ean8()}" for _ in range(n_ads)],
        "state": "enabled"
    })

    return {
        "sponsored_products_campaigns": campaigns_df.to_dicts(),
        "sponsored_products_ad_groups": adgroups_df.to_dicts(),
        "sponsored_products_product_ads": ads_df.to_dicts()
    }
