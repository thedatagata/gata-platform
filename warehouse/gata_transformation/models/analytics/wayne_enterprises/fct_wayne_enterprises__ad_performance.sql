-- Wayne Enterprises: Unified Ad Performance
-- Sources: Bing Ads, Google Ads
{{ config(materialized='table') }}

{{ build_fct_ad_performance('wayne_enterprises', ['bing_ads', 'google_ads']) }}
