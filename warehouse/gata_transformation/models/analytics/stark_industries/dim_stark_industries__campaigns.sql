{{ config(materialized='table') }}
{{ build_dim_campaigns('stark_industries', ['facebook_ads', 'instagram_ads']) }}
