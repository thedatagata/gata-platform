# services/mock-data-engine/config.py
import yaml
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from pathlib import Path

class GenConfig(BaseModel):
    # Standard Levers with defaults to prevent NoneType errors in generators
    daily_spend_mean: float = 100.0
    campaign_count: int = 5
    cpc_mean: float = 1.2
    daily_orders_mean: float = 15.0
    aov_mean: float = 75.0
    product_count: int = 50
    organic_traffic_ratio: float = 0.4
    conversion_rate: float = 0.03
    # Connector Specific Levers
    daily_event_count: int = 1000
    unique_user_base: int = 5000
    daily_order_count: int = 20
    avg_order_value: float = 65.0
    product_catalog_size: int = 100
    avg_ctr: float = 0.015
    ad_group_count: int = 8
    sponsored_product_ratio: float = 0.25

class TableConfig(BaseModel):
    name: str
    logic: Dict[str, Any] = {}

class SourceConfig(BaseModel):
    enabled: bool = False
    generation: GenConfig = Field(default_factory=GenConfig)
    tables: List[TableConfig] = []

class SourceRegistry(BaseModel):
    # Paid Ads
    facebook_ads: SourceConfig = SourceConfig()
    google_ads: SourceConfig = SourceConfig()
    linkedin_ads: SourceConfig = SourceConfig()
    bing_ads: SourceConfig = SourceConfig()
    amazon_ads: SourceConfig = SourceConfig()
    tiktok_ads: SourceConfig = SourceConfig()
    instagram_ads: SourceConfig = SourceConfig()
    # Ecommerce
    shopify: SourceConfig = SourceConfig()
    woocommerce: SourceConfig = SourceConfig()
    bigcommerce: SourceConfig = SourceConfig()
    # Analytics
    amplitude: SourceConfig = SourceConfig()
    mixpanel: SourceConfig = SourceConfig()
    google_analytics: SourceConfig = SourceConfig()
    # STRIPE REMOVED PER REQUEST

class TenantConfig(BaseModel):
    slug: str
    business_name: str
    status: str = "active"
    sources: SourceRegistry

class Manifest(BaseModel):
    tenants: List[TenantConfig]

def load_manifest(path: str = 'tenants.yaml') -> Manifest:
    with open(path, 'r') as f:
        return Manifest(**yaml.safe_load(f))
