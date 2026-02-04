from typing import Dict, Any, List
import sys

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
    def __init__(self, config: Any, days: int = 30):
        self.config = config
        self.days = days
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
        # Perform validation before generation
        self._validate_source_mix()
        
        sources = self.config.sources
        
        # --- Context Containers ---
        ad_campaigns_context = [] 
        stripe_charges_context = []
        shopify_orders_context = [] # Used generically for attribution across platforms

        # 1. Paid Ads (Multi-source allowed)
        if sources.facebook_ads.enabled:
            print("  - Generating Facebook Ads...")
            fb_data = generate_facebook_data(self.config.slug, sources.facebook_ads.generation, self.days)
            self.registry['facebook_ads'] = fb_data
            if 'campaigns' in fb_data:
                ad_campaigns_context.extend(fb_data['campaigns'])

        if sources.google_ads.enabled:
            print("  - Generating Google Ads...")
            gads_data = generate_google_ads(self.config.slug, sources.google_ads.generation, self.days)
            self.registry['google_ads'] = gads_data
            if 'campaigns' in gads_data:
                ad_campaigns_context.extend(gads_data['campaigns'])

        if sources.instagram_ads.enabled:
            print("  - Generating Instagram Ads...")
            ig_data = generate_instagram_data(self.config.slug, sources.instagram_ads.generation, self.days)
            self.registry['instagram_ads'] = ig_data
            if 'campaigns' in ig_data:
                ad_campaigns_context.extend(ig_data['campaigns'])

        if sources.linkedin_ads.enabled:
            print("  - Generating LinkedIn Ads...")
            li_data = generate_linkedin_data(self.config.slug, sources.linkedin_ads.generation, self.days)
            self.registry['linkedin_ads'] = li_data
            if 'campaigns' in li_data:
                ad_campaigns_context.extend(li_data['campaigns'])

        if sources.bing_ads.enabled:
            print("  - Generating Bing Ads...")
            bing_data = generate_bing_data(self.config.slug, sources.bing_ads.generation, self.days)
            self.registry['bing_ads'] = bing_data
            if 'campaigns' in bing_data:
                ad_campaigns_context.extend(bing_data['campaigns'])

        if sources.amazon_ads.enabled:
            print("  - Generating Amazon Ads...")
            amz_data = generate_amazon_data(self.config.slug, sources.amazon_ads.generation, self.days)
            self.registry['amazon_ads'] = amz_data
            if 'sponsored_products_campaigns' in amz_data:
                ad_campaigns_context.extend(amz_data['sponsored_products_campaigns'])

        # 2. Transactions (Shared)
        if sources.stripe.enabled:
            print("  - Generating Stripe Charges...")
            stripe_data = generate_stripe_data(self.config.slug, sources.stripe.generation, self.days)
            self.registry['stripe'] = stripe_data
            stripe_charges_context = stripe_data['charges']

        # 3. Ecommerce Platforms (Exclusive)
        if sources.shopify.enabled:
            print("  - Generating Shopify Orders...")
            shopify_data = generate_shopify_data(self.config.slug, sources.shopify.generation, stripe_charges_context)
            self.registry['shopify'] = shopify_data
            shopify_orders_context = shopify_data['orders']

        elif sources.woocommerce.enabled:
            print("  - Generating WooCommerce Orders...")
            woo_data = generate_woocommerce_data(self.config.slug, sources.woocommerce.generation, stripe_charges_context)
            self.registry['woocommerce'] = woo_data
            if 'orders' in woo_data:
                shopify_orders_context = woo_data['orders']

        elif sources.bigcommerce.enabled:
            print("  - Generating BigCommerce Orders...")
            bc_data = generate_bigcommerce_data(self.config.slug, sources.bigcommerce.generation, self.days)
            self.registry['bigcommerce'] = bc_data
            if 'orders' in bc_data:
                shopify_orders_context = bc_data['orders']

        # 4. Digital Analytics (Exclusive)
        if sources.google_analytics.enabled:
            print("  - Generating GA4 Traffic...")
            ga4_data = generate_ga4_data(
                self.config.slug, 
                sources.google_analytics.generation,
                shopify_orders_context,
                ad_campaigns_context
            )
            self.registry['google_analytics'] = ga4_data

        elif sources.amplitude.enabled:
            print("  - Generating Amplitude Events...")
            amp_data = generate_amplitude_data(self.config.slug, sources.amplitude.generation, self.days)
            self.registry['amplitude'] = amp_data

        elif sources.mixpanel.enabled:
            print("  - Generating Mixpanel Events...")
            mix_data = generate_mixpanel_data(self.config.slug, sources.mixpanel.generation, self.days)
            self.registry['mixpanel'] = mix_data

        return self.registry