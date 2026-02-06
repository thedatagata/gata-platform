import yaml
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from pathlib import Path

class GenConfig(BaseModel):
    # Generic container for generation levers
    daily_spend_mean: Optional[float] = None
    campaign_count: Optional[int] = None
    cpc_mean: Optional[float] = None
    daily_orders_mean: Optional[float] = None
    aov_mean: Optional[float] = None
    product_count: Optional[int] = None
    organic_traffic_ratio: Optional[float] = None
    conversion_rate: Optional[float] = None
    # New Connector Levers
    daily_event_count: Optional[int] = None
    unique_user_base: Optional[int] = None
    daily_order_count: Optional[int] = None
    avg_order_value: Optional[float] = None
    product_catalog_size: Optional[int] = None
    avg_ctr: Optional[float] = None
    ad_group_count: Optional[int] = None
    sponsored_product_ratio: Optional[float] = None

class TableConfig(BaseModel):
    name: str
    logic: Dict[str, Any] = {}

class SourceConfig(BaseModel):
    enabled: bool = False
    generation: GenConfig = Field(default_factory=GenConfig)
    tables: List[TableConfig] = []

class SourceRegistry(BaseModel):
    facebook_ads: SourceConfig = SourceConfig()
    google_ads: SourceConfig = SourceConfig()
    stripe: SourceConfig = SourceConfig()
    shopify: SourceConfig = SourceConfig()
    # New Sources
    linkedin_ads: SourceConfig = SourceConfig()
    bing_ads: SourceConfig = SourceConfig()
    amazon_ads: SourceConfig = SourceConfig()
    amplitude: SourceConfig = SourceConfig()
    mixpanel: SourceConfig = SourceConfig()
    woocommerce: SourceConfig = SourceConfig()
    bigcommerce: SourceConfig = SourceConfig()
    google_analytics: SourceConfig = SourceConfig()
    instagram_ads: SourceConfig = SourceConfig()

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
