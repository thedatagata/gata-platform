{{ config(materialized='table') }}
{{ build_dim_campaigns('wayne_enterprises', ['bing_ads', 'google_ads']) }}
