from typing import Dict, Any, List
import sys
import dlt

# Standard Imports
from sources.paid_ads.facebook_ads.fb_ads_data_generator import generate_facebook_data
from sources.paid_ads.google_ads.google_ads_data_generator import generate_google_ads
from sources.transactions.stripe.stripe_data_generator import generate_stripe_data
from sources.ecommerce_platforms.shopify.shopify_data_generator import generate_shopify_data
from sources.digital_analytics.google_analytics.ga4_data_generator import generate_ga4_data
from sources.paid_ads.instagram_ads.ig_ads_data_generator import generate_instagram_data
from sources.paid_ads.linkedin_ads.linkedin_ads_data_generator import generate_linkedin_data
from sources.paid_ads.bing_ads.bing_ads_data_generator import generate_bing_data
from sources.paid_ads.amazon_ads.amazon_ads_data_generator import generate_amazon_data
from sources.product_analytics.amplitude.amplitude_data_generator import generate_amplitude_data
from sources.product_analytics.mixpanel.mixpanel_data_generator import generate_mixpanel_data
from sources.ecommerce_platforms.woocommerce.woocommerce_data_generator import generate_woocommerce_data
from sources.ecommerce_platforms.bigcommerce.bigcommerce_data_generator import generate_bigcommerce_data

class MockOrchestrator:
    def __init__(self, config: Any, days: int = 30, credentials: str = None):
        self.config = config
        self.days = days
        self.credentials = credentials
        self.registry = {} 

    def _validate_source_mix(self):
        """
        Enforces business logic for source combinations:
        - Max 1 Ecommerce Platform
        - Max 1 Digital Analytics Source
        - Unlimited Paid Ads and Stripe
        """
        sources = self.config.sources
        
        # Categorize sources for validation
        ecommerce_group = ['shopify', 'woocommerce', 'bigcommerce']
        analytics_group = ['google_analytics', 'amplitude', 'mixpanel']
        
        enabled_ecommerce = [s for s in ecommerce_group if getattr(sources, s).enabled]
        enabled_analytics = [s for s in analytics_group if getattr(sources, s).enabled]
        
        # Validate Ecommerce
        if len(enabled_ecommerce) > 1:
            print(f"CRITICAL ERROR: Tenant '{self.config.slug}' has multiple ecommerce platforms enabled: {enabled_ecommerce}")
            print("Action: A tenant may only have one primary ecommerce platform.")
            sys.exit(1)
            
        # Validate Analytics
        if len(enabled_analytics) > 1:
            print(f"CRITICAL ERROR: Tenant '{self.config.slug}' has multiple digital analytics sources enabled: {enabled_analytics}")
            print("Action: A tenant may only have one primary digital analytics source.")
            sys.exit(1)

    def run(self) -> Dict[str, Any]:
        self._validate_source_mix()
        
        # Initialize dlt pipeline
        # Determine destination based on credentials presence/content
        destination = 'duckdb' if self.credentials and 'duckdb' in self.credentials and ('/' in self.credentials or '\\' in self.credentials) else 'motherduck'
        
        pipeline = dlt.pipeline(
            pipeline_name=f'mock_load_{self.config.slug}',
            destination=destination,
            dataset_name=self.config.slug
        )
        
        # Helper to wrap and run dlt resources
        def load_source(source_name: str, data: Dict[str, List[dict]]):
            for table_name, rows in data.items():
                # Table name pattern: raw_{tenant}_{source}_{object}
                full_table_name = f"raw_{self.config.slug}_{source_name}_{table_name}"
                # Physcially load data and capture metadata
                pipeline.run(
                    dlt.resource(rows, name=full_table_name, write_disposition='replace')
                )
            return data

        sources = self.config.sources
        
        # --- Context Containers ---
        ad_campaigns_context = [] 
        stripe_charges_context = []
        shopify_orders_context = [] 

        # 1. Paid Ads (Multi-source allowed)
        if sources.facebook_ads.enabled:
            print("  - Processing Facebook Ads...")
            fb_raw = generate_facebook_data(self.config.slug, sources.facebook_ads.generation, self.days)
            load_source('facebook_ads', fb_raw)
            if 'campaigns' in fb_raw:
                ad_campaigns_context.extend(fb_raw['campaigns'])

        if sources.google_ads.enabled:
            print("  - Processing Google Ads...")
            gads_raw = generate_google_ads(self.config.slug, sources.google_ads.generation, self.days)
            load_source('google_ads', gads_raw)
            if 'campaigns' in gads_raw:
                ad_campaigns_context.extend(gads_raw['campaigns'])

        if sources.instagram_ads.enabled:
            print("  - Processing Instagram Ads...")
            ig_raw = generate_instagram_data(self.config.slug, sources.instagram_ads.generation, self.days)
            load_source('instagram_ads', ig_raw)
            if 'campaigns' in ig_raw:
                ad_campaigns_context.extend(ig_raw['campaigns'])

        if sources.linkedin_ads.enabled:
            print("  - Processing LinkedIn Ads...")
            li_raw = generate_linkedin_data(self.config.slug, sources.linkedin_ads.generation, self.days)
            load_source('linkedin_ads', li_raw)
            if 'campaigns' in li_raw:
                ad_campaigns_context.extend(li_raw['campaigns'])

        if sources.bing_ads.enabled:
            print("  - Processing Bing Ads...")
            bing_raw = generate_bing_data(self.config.slug, sources.bing_ads.generation, self.days)
            load_source('bing_ads', bing_raw)
            if 'campaigns' in bing_raw:
                ad_campaigns_context.extend(bing_raw['campaigns'])

        if sources.amazon_ads.enabled:
            print("  - Processing Amazon Ads...")
            amz_raw = generate_amazon_data(self.config.slug, sources.amazon_ads.generation, self.days)
            load_source('amazon_ads', amz_raw)
            if 'sponsored_products_campaigns' in amz_raw:
                ad_campaigns_context.extend(amz_raw['sponsored_products_campaigns'])

        # 2. Transactions (Shared)
        if sources.stripe.enabled:
            print("  - Processing Stripe...")
            stripe_raw = generate_stripe_data(self.config.slug, sources.stripe.generation, self.days)
            load_source('stripe', stripe_raw)
            stripe_charges_context = stripe_raw.get('charges', [])

        # 3. Ecommerce Platforms (Exclusive)
        if sources.shopify.enabled:
            print("  - Processing Shopify...")
            shopify_raw = generate_shopify_data(self.config.slug, sources.shopify.generation, stripe_charges_context)
            load_source('shopify', shopify_raw)
            shopify_orders_context = shopify_raw.get('orders', [])

        elif sources.woocommerce.enabled:
            print("  - Processing WooCommerce...")
            woo_raw = generate_woocommerce_data(self.config.slug, sources.woocommerce.generation, stripe_charges_context, days=self.days)
            load_source('woocommerce', woo_raw)
            if 'orders' in woo_raw:
                shopify_orders_context = woo_raw['orders']

        elif sources.bigcommerce.enabled:
            print("  - Processing BigCommerce...")
            bc_raw = generate_bigcommerce_data(self.config.slug, sources.bigcommerce.generation, self.days)
            load_source('bigcommerce', bc_raw)
            if 'orders' in bc_raw:
                shopify_orders_context = bc_raw['orders']

        # 4. Digital Analytics (Exclusive)
        if sources.google_analytics.enabled:
            print("  - Processing Google Analytics...")
            ga4_raw = generate_ga4_data(
                self.config.slug, 
                sources.google_analytics.generation,
                shopify_orders_context,
                ad_campaigns_context
            )
            load_source('google_analytics', ga4_raw)

        elif sources.amplitude.enabled:
            print("  - Processing Amplitude...")
            amp_raw = generate_amplitude_data(self.config.slug, sources.amplitude.generation, self.days)
            load_source('amplitude', amp_raw)

        elif sources.mixpanel.enabled:
            print("  - Processing Mixpanel...")
            mix_raw = generate_mixpanel_data(self.config.slug, sources.mixpanel.generation, self.days)
            load_source('mixpanel', mix_raw)

        # Return technical schema dictionary for BSL and Rill generation
        return pipeline.default_schema.to_dict()