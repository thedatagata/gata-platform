{{ config(materialized='table') }}
{{ build_dim_campaigns('tyrell_corp', ['facebook_ads', 'google_ads', 'instagram_ads']) }}
