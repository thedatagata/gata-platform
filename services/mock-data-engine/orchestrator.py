from typing import Dict, Any, List
import sys
import dlt

# Generators (Stripe Removed, TikTok Added)
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
        self.registry = {} 

    def _validate_source_mix(self):
        """Ensures at least one source per category is active."""
        sources = self.config.sources
        ecommerce_group = ['shopify', 'woocommerce', 'bigcommerce']
        analytics_group = ['google_analytics', 'amplitude', 'mixpanel']
        # Paid Ads group for audit gap enforcement
        ads_group = ['facebook_ads', 'google_ads', 'tiktok_ads', 'linkedin_ads', 'bing_ads', 'amazon_ads', 'instagram_ads']
        
        enabled_ecommerce = [s for s in ecommerce_group if getattr(sources, s).enabled]
        enabled_analytics = [s for s in analytics_group if getattr(sources, s).enabled]
        enabled_ads = [s for s in ads_group if getattr(sources, s).enabled]
        
        # Audit Gap Fix: Enforce minimum of 1
        if not enabled_ecommerce:
            raise ValueError(f"Tenant '{self.config.slug}' must have at least one Ecommerce source.")
        if not enabled_analytics:
            raise ValueError(f"Tenant '{self.config.slug}' must have at least one Analytics source.")
        if not enabled_ads:
            raise ValueError(f"Tenant '{self.config.slug}' must have at least one Paid Ads source.")

    def run(self) -> Dict[str, Any]:
        self._validate_source_mix()
        destination = 'duckdb' if self.credentials and 'duckdb' in self.credentials else 'motherduck'
        
        pipeline = dlt.pipeline(
            pipeline_name=f'mock_load_{self.config.slug}',
            destination=destination,
            dataset_name=self.config.slug
        )
        
        def load_source(source_name: str, data: Dict[str, List[dict]]):
            for table_name, rows in data.items():
                full_table_name = f"raw_{self.config.slug}_{source_name}_{table_name}"
                wrapped_rows = [{"raw_data_payload": r} for r in rows]
                pipeline.run(
                    dlt.resource(
                        wrapped_rows, name=full_table_name, write_disposition='replace',
                        columns={"raw_data_payload": {"data_type": "complex"}}
                    )
                )
            return data

        sources = self.config.sources
        ad_campaigns_context = [] 
        shopify_orders_context = [] 

        # 1. Paid Ads (Generating and collecting campaign metadata for attribution)
        if sources.facebook_ads.enabled:
            fb_raw = generate_facebook_data(self.config.slug, sources.facebook_ads.generation, self.days)
            load_source('facebook_ads', fb_raw)
            if 'campaigns' in fb_raw: ad_campaigns_context.extend(fb_raw['campaigns'])

        if sources.google_ads.enabled:
            gads_raw = generate_google_ads(self.config.slug, sources.google_ads.generation, self.days)
            load_source('google_ads', gads_raw)
            if 'campaigns' in gads_raw: ad_campaigns_context.extend(gads_raw['campaigns'])

        if sources.tiktok_ads.enabled:
            tt_raw = generate_tiktok_data(self.config.slug, sources.tiktok_ads.generation, self.days)
            load_source('tiktok_ads', tt_raw)
            if 'campaigns' in tt_raw: ad_campaigns_context.extend(tt_raw['campaigns'])

        if sources.instagram_ads.enabled:
            ig_raw = generate_instagram_data(self.config.slug, sources.instagram_ads.generation, self.days)
            load_source('instagram_ads', ig_raw)
            if 'campaigns' in ig_raw: ad_campaigns_context.extend(ig_raw['campaigns'])

        if sources.linkedin_ads.enabled:
            li_raw = generate_linkedin_data(self.config.slug, sources.linkedin_ads.generation, self.days)
            load_source('linkedin_ads', li_raw)
            if 'campaigns' in li_raw: ad_campaigns_context.extend(li_raw['campaigns'])

        if sources.bing_ads.enabled:
            bing_raw = generate_bing_data(self.config.slug, sources.bing_ads.generation, self.days)
            load_source('bing_ads', bing_raw)
            if 'campaigns' in bing_raw: ad_campaigns_context.extend(bing_raw['campaigns'])

        if sources.amazon_ads.enabled:
            amz_raw = generate_amazon_data(self.config.slug, sources.amazon_ads.generation, self.days)
            load_source('amazon_ads', amz_raw)
            if 'sponsored_products_campaigns' in amz_raw: ad_campaigns_context.extend(amz_raw['sponsored_products_campaigns'])

        # 2. Ecommerce (Generating and collecting orders for session conversion simulation)
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

        # 3. Digital Analytics (Simulating traffic based on Ad and Ecommerce context)
        if sources.google_analytics.enabled:
            ga4_raw = generate_ga4_data(self.config.slug, sources.google_analytics.generation, shopify_orders_context, ad_campaigns_context)
            load_source('google_analytics', ga4_raw)

        elif sources.amplitude.enabled:
            amp_raw = generate_amplitude_data(self.config.slug, sources.amplitude.generation, self.days)
            load_source('amplitude', amp_raw)

        elif sources.mixpanel.enabled:
            mix_raw = generate_mixpanel_data(self.config.slug, sources.mixpanel.generation, self.days)
            load_source('mixpanel', mix_raw)

        return pipeline.default_schema.to_dict()