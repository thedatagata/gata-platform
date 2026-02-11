{{ config(materialized='table') }}
{{ build_fct_ad_performance('stark_industries', ['facebook_ads', 'instagram_ads']) }}
