-- Stark Industries: Unified Ad Performance
-- Sources: Facebook Ads, Instagram Ads
{{ config(materialized='table') }}

{{ build_fct_ad_performance('stark_industries', ['facebook_ads', 'instagram_ads']) }}
