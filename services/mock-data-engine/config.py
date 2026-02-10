# services/mock-data-engine/config.py
import yaml
from pydantic import BaseModel, Field
from typing import List, Dict, Optional, Any
from pathlib import Path

class FunnelConfig(BaseModel):
    """
    Funnel probability means — set per-tenant in tenants.yaml.
    These are the CENTER of the distribution that the simulation
    draws from. The actual probability for each user/session is
    randomized around these means.
    """
    # Within-session advance probabilities (low at top, high at bottom)
    session_start_to_view_item: float = 0.55
    view_item_to_add_to_cart: float = 0.25
    add_to_cart_to_begin_checkout: float = 0.55
    begin_checkout_to_add_payment_info: float = 0.75
    add_payment_info_to_purchase: float = 0.88

    # Return session probabilities by funnel depth reached
    # Key names match the step index: depth_0 = bounced, depth_5 = purchased
    return_after_bounce: float = 0.06       # depth 0
    return_after_view: float = 0.15         # depth 1
    return_after_cart: float = 0.35         # depth 2
    return_after_checkout: float = 0.50     # depth 3
    return_after_payment: float = 0.60      # depth 4
    return_after_purchase: float = 0.45     # depth 5

    # Returning customer boost multiplier (brand familiarity)
    returning_customer_boost: float = 1.4

    def get_advance_rates(self) -> Dict[str, float]:
        """Return advance rates as the dict the simulation expects."""
        return {
            "session_start_to_view_item": self.session_start_to_view_item,
            "view_item_to_add_to_cart": self.view_item_to_add_to_cart,
            "add_to_cart_to_begin_checkout": self.add_to_cart_to_begin_checkout,
            "begin_checkout_to_add_payment_info": self.begin_checkout_to_add_payment_info,
            "add_payment_info_to_purchase": self.add_payment_info_to_purchase,
        }

    def get_return_rates(self) -> Dict[int, float]:
        """Return rates indexed by funnel depth."""
        return {
            0: self.return_after_bounce,
            1: self.return_after_view,
            2: self.return_after_cart,
            3: self.return_after_checkout,
            4: self.return_after_payment,
            5: self.return_after_purchase,
        }


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
    # Funnel simulation levers (tenant-configurable)
    funnel: FunnelConfig = Field(default_factory=FunnelConfig)

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
