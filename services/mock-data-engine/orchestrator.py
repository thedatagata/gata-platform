import dlt
import polars as pl
from typing import Dict, Any, List, Iterator
from datetime import datetime

# --- Standardized Generator Imports ---
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
    def __init__(self, config: Any, days: int = 90, credentials: str = None):
        """Initializes with a default 90-day window for categorical density."""
        self.config = config
        self.days = days
        self.credentials = credentials

    def run(self) -> Dict[str, Any]:
        """Orchestrates the ETL pipeline for tenant or library establish."""
        is_local = self.credentials and 'duckdb' in self.credentials
        if is_local:
            from pathlib import Path
            sandbox_path = str(Path(__file__).resolve().parent.parent.parent / "warehouse" / "sandbox.duckdb")
            destination = dlt.destinations.duckdb(credentials=sandbox_path)
        else:
            destination = 'motherduck'
        pipeline = dlt.pipeline(
            pipeline_name=f'mock_load_{self.config.slug}',
            destination=destination,
            dataset_name=self.config.slug,
        )
        
        load_package = []

        def create_table_etl(source_name: str, table_name: str, raw_data: List[dict]):
            """Chains Extract, Transform, and Load stages for physical schema stability."""
            full_table_name = f"raw_{self.config.slug}_{source_name}_{table_name}"

            @dlt.resource(name=f"{full_table_name}_extract", selected=False)
            def extract():
                """Stage 1: Load raw generator output into Polars."""
                yield pl.DataFrame(raw_data)

            @dlt.transformer(data_from=extract, name=f"{full_table_name}_transform", selected=False)
            def transform(df: pl.DataFrame):
                """Stage 2: Bypass Arrow UTC/tzdata errors by casting dates to strings."""
                dt_cols = [c for c, t in df.schema.items() if isinstance(t, (pl.Datetime, pl.Date))]
                if dt_cols:
                    df = df.with_columns([pl.col(c).cast(pl.String) for c in dt_cols])
                return df

            @dlt.transformer(data_from=transform, name=full_table_name, max_table_nesting=0)
            def load(df: pl.DataFrame):
                """Stage 3: Yield Arrow table for strict warehouse typing."""
                yield df.to_arrow()

            return load

        # --- Generation & Collection Logic ---
        sources = self.config.sources
        ad_context, order_context = [], []

        # 1. Paid Ads
        if sources.facebook_ads.enabled:
            raw = generate_facebook_data(self.config.slug, sources.facebook_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('facebook_ads', t, r))
            ad_context.extend(raw.get('campaigns', []))

        if sources.google_ads.enabled:
            raw = generate_google_ads(self.config.slug, sources.google_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('google_ads', t, r))
            ad_context.extend(raw.get('campaigns', []))

        if sources.linkedin_ads.enabled:
            raw = generate_linkedin_data(self.config.slug, sources.linkedin_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('linkedin_ads', t, r))

        if sources.bing_ads.enabled:
            raw = generate_bing_data(self.config.slug, sources.bing_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('bing_ads', t, r))

        if sources.tiktok_ads.enabled:
            raw = generate_tiktok_data(self.config.slug, sources.tiktok_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('tiktok_ads', t, r))

        if sources.instagram_ads.enabled:
            raw = generate_instagram_data(self.config.slug, sources.instagram_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('instagram_ads', t, r))

        if sources.amazon_ads.enabled:
            raw = generate_amazon_data(self.config.slug, sources.amazon_ads.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('amazon_ads', t, r))

        # 2. Ecommerce
        if sources.shopify.enabled:
            raw = generate_shopify_data(self.config.slug, sources.shopify.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('shopify', t, r))
            order_context.extend(raw.get('orders', []))
        elif sources.woocommerce.enabled:
            raw = generate_woocommerce_data(self.config.slug, sources.woocommerce.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('woocommerce', t, r))
            order_context.extend(raw.get('orders', []))
        elif sources.bigcommerce.enabled:
            raw = generate_bigcommerce_data(self.config.slug, sources.bigcommerce.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('bigcommerce', t, r))
            order_context.extend(raw.get('orders', []))

        # 3. Analytics
        if sources.google_analytics.enabled:
            raw = generate_ga4_data(self.config.slug, sources.google_analytics.generation, order_context, ad_context)
            for t, r in raw.items(): load_package.append(create_table_etl('google_analytics', t, r))

        if sources.amplitude.enabled:
            raw = generate_amplitude_data(self.config.slug, sources.amplitude.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('amplitude', t, r))

        if sources.mixpanel.enabled:
            raw = generate_mixpanel_data(self.config.slug, sources.mixpanel.generation, self.days)
            for t, r in raw.items(): load_package.append(create_table_etl('mixpanel', t, r))

        # Atomic Run: Solves internal metadata NOT NULL issues.
        if load_package:
            pipeline.run(load_package)
            
        return pipeline.default_schema.to_dict()