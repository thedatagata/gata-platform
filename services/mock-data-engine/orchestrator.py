import dlt
import polars as pl
from typing import Dict, Any, List, Iterator
import math

# --- Pydantic Schema Imports for Data Contracts ---
from schemas.meta_ads import MetaCampaign, MetaAdSet, MetaAd, MetaAdInsight
from schemas.google_ads import GAdsCustomer, GAdsCampaign, GAdsAdGroup, GAdsAd, GAdsAdPerformance
from schemas.google_analytics import GA4Event
from schemas.linkedin_ads import LinkedInCampaign, LinkedInAdAnalytics
from schemas.shopify import ShopifyProduct, ShopifyOrder
from schemas.woocommerce import WooCommerceProduct, WooCommerceOrder
# NEW SCHEMAS
from schemas.bigcommerce import BigCommerceProduct, BigCommerceOrder
from schemas.bing_ads import BingCampaign, BingAdGroup, BingAd, BingAccountPerformanceReport
from schemas.tiktok_ads import TikTokCampaign, TikTokAdGroup, TikTokAd, TikTokAdReportDaily
from schemas.amazon_ads import AmzSPCampaign, AmzSPAdGroup, AmzSPAd
from schemas.amplitude import AmplitudeEvent, AmplitudeUser
from schemas.mixpanel import MixpanelEvent, MixpanelPerson

# --- Generator Imports ---
from sources.paid_ads.facebook_ads.fb_ads_data_generator import generate_facebook_data
from sources.paid_ads.google_ads.google_ads_data_generator import generate_google_ads
from sources.paid_ads.tiktok_ads.tiktok_ads_data_generator import generate_tiktok_data
from sources.paid_ads.instagram_ads.ig_ads_data_generator import generate_instagram_data
from sources.paid_ads.linkedin_ads.linkedin_ads_data_generator import generate_linkedin_data
from sources.paid_ads.bing_ads.bing_ads_data_generator import generate_bing_data
from sources.paid_ads.amazon_ads.amazon_ads_data_generator import generate_amazon_data
from sources.ecommerce_platforms.shopify.shopify_data_generator import generate_shopify_data
from sources.ecommerce_platforms.woocommerce.woocommerce_data_generator import generate_woocommerce_data
from sources.ecommerce_platforms.bigcommerce.bigcommerce_data_generator import generate_bigcommerce_data
from sources.digital_analytics.google_analytics.ga4_data_generator import generate_ga4_data
from sources.product_analytics.amplitude.amplitude_data_generator import generate_amplitude_data
from sources.product_analytics.mixpanel.mixpanel_data_generator import generate_mixpanel_data

class MockOrchestrator:
    def __init__(self, config: Any, days: int = 30, credentials: str = None):
        self.config = config
        self.days = days
        self.credentials = credentials

    def _validate_source_mix(self):
        if self.config.slug == 'library_sample': return
        sources = self.config.sources
        if not any(getattr(sources, s).enabled for s in ['shopify', 'woocommerce', 'bigcommerce']):
            raise ValueError(f"Tenant '{self.config.slug}' must have an Ecommerce source.")
        if not any(getattr(sources, s).enabled for s in ['google_analytics', 'amplitude', 'mixpanel']):
            raise ValueError(f"Tenant '{self.config.slug}' must have an Analytics source.")
        if not any(getattr(sources, s).enabled for s in ['facebook_ads', 'google_ads', 'tiktok_ads', 'linkedin_ads', 'bing_ads', 'amazon_ads', 'instagram_ads']):
            raise ValueError(f"Tenant '{self.config.slug}' must have a Paid Ads source.")

    def run(self) -> Dict[str, Any]:
        self._validate_source_mix()
        destination = 'duckdb' if self.credentials == 'duckdb' else 'motherduck'
        pipeline = dlt.pipeline(pipeline_name=f'mock_load_{self.config.slug}', destination=destination, dataset_name=self.config.slug)

        SCHEMA_MAP = {
            'instagram_ads': {'campaigns': MetaCampaign, 'ad_sets': MetaAdSet, 'ads': MetaAd, 'facebook_insights': MetaAdInsight},
            'facebook_ads': {'campaigns': MetaCampaign, 'ad_sets': MetaAdSet, 'ads': MetaAd, 'facebook_insights': MetaAdInsight},
            'google_ads': {'customers': GAdsCustomer, 'campaigns': GAdsCampaign, 'ad_groups': GAdsAdGroup, 'ads': GAdsAd, 'ad_performance': GAdsAdPerformance},
            'google_analytics': {'events': GA4Event},
            'linkedin_ads': {'campaigns': LinkedInCampaign, 'ad_analytics': LinkedInAdAnalytics},
            'shopify': {'products': ShopifyProduct, 'orders': ShopifyOrder},
            'woocommerce': {'products': WooCommerceProduct, 'orders': WooCommerceOrder},
            'bigcommerce': {'products': BigCommerceProduct, 'orders': BigCommerceOrder},
            'bing_ads': {'campaigns': BingCampaign, 'ad_groups': BingAdGroup, 'ads': BingAd, 'account_performance_report': BingAccountPerformanceReport},
            'tiktok_ads': {'campaigns': TikTokCampaign, 'ad_groups': TikTokAdGroup, 'ads': TikTokAd, 'ads_reports_daily': TikTokAdReportDaily},
            'amazon_ads': {'sponsored_products_campaigns': AmzSPCampaign, 'sponsored_products_ad_groups': AmzSPAdGroup, 'sponsored_products_product_ads': AmzSPAd},
            'amplitude': {'events': AmplitudeEvent, 'users': AmplitudeUser},
            'mixpanel': {'events': MixpanelEvent, 'people': MixpanelPerson}
        }

        def load_source(source_name: str, data: Dict[str, List[dict]]):
            for table_name, rows in data.items():
                full_table_name = f"raw_{self.config.slug}_{source_name}_{table_name}"
                model = SCHEMA_MAP.get(source_name, {}).get(table_name)
                df = pl.DataFrame(rows)
                
                @dlt.resource(
                    name=full_table_name, write_disposition='replace', columns=model,
                    schema_contract={"tables": "evolve", "columns": "evolve", "data_type": "freeze"}
                )
                def paged_resource(chunk_size: int = 1000):
                    n_chunks = math.ceil(len(df) / chunk_size)
                    for i in range(n_chunks):
                        yield df.slice(i * chunk_size, chunk_size).to_dicts()

                pipeline.run(paged_resource())
            return data

        sources = self.config.sources
        ad_campaigns_context = []
        shopify_orders_context = []

        # Paid Ads
        if sources.facebook_ads.enabled:
            fb_raw = generate_facebook_data(self.config.slug, sources.facebook_ads.generation, self.days)
            load_source('facebook_ads', fb_raw)
            if 'campaigns' in fb_raw: ad_campaigns_context.extend(fb_raw['campaigns'])

        if sources.google_ads.enabled:
            gads_raw = generate_google_ads(self.config.slug, sources.google_ads.generation, self.days)
            load_source('google_ads', gads_raw)
            if 'campaigns' in gads_raw: ad_campaigns_context.extend(gads_raw['campaigns'])

        if sources.linkedin_ads.enabled:
            load_source('linkedin_ads', generate_linkedin_data(self.config.slug, sources.linkedin_ads.generation, self.days))

        if sources.bing_ads.enabled:
            load_source('bing_ads', generate_bing_data(self.config.slug, sources.bing_ads.generation, self.days))

        if sources.tiktok_ads.enabled:
            load_source('tiktok_ads', generate_tiktok_data(self.config.slug, sources.tiktok_ads.generation, self.days))

        if sources.instagram_ads.enabled:
            load_source('instagram_ads', generate_instagram_data(self.config.slug, sources.instagram_ads.generation, self.days))

        if sources.amazon_ads.enabled:
            load_source('amazon_ads', generate_amazon_data(self.config.slug, sources.amazon_ads.generation, self.days))

        # Ecommerce
        if sources.shopify.enabled:
            shopify_raw = generate_shopify_data(self.config.slug, sources.shopify.generation, [])
            load_source('shopify', shopify_raw)
            shopify_orders_context = shopify_raw.get('orders', [])

        elif sources.woocommerce.enabled:
            woo_raw = generate_woocommerce_data(self.config.slug, sources.woocommerce.generation, [], days=self.days)
            load_source('woocommerce', woo_raw)
            shopify_orders_context = woo_raw.get('orders', [])

        elif sources.bigcommerce.enabled:
            bc_raw = generate_bigcommerce_data(self.config.slug, sources.bigcommerce.generation, [], self.days)
            load_source('bigcommerce', bc_raw)
            shopify_orders_context = bc_raw.get('orders', [])

        # Analytics
        if sources.google_analytics.enabled:
            load_source('google_analytics', generate_ga4_data(self.config.slug, sources.google_analytics.generation, shopify_orders_context, ad_campaigns_context))

        elif sources.amplitude.enabled:
            load_source('amplitude', generate_amplitude_data(self.config.slug, sources.amplitude.generation, self.days))

        elif sources.mixpanel.enabled:
            load_source('mixpanel', generate_mixpanel_data(self.config.slug, sources.mixpanel.generation, self.days))

        return pipeline.default_schema.to_dict()