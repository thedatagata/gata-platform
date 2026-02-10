-- Tyrell Corp: Unified Ad Performance
-- Sources: Facebook Ads, Google Ads, Instagram Ads
{{ config(materialized='table') }}

{{ build_fct_ad_performance('tyrell_corp', ['facebook_ads', 'google_ads', 'instagram_ads']) }}
