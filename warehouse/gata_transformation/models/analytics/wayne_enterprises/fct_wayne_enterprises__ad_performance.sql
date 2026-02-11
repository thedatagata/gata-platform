{{ config(materialized='table') }}
{{ build_fct_ad_performance('wayne_enterprises', ['bing_ads', 'google_ads']) }}
