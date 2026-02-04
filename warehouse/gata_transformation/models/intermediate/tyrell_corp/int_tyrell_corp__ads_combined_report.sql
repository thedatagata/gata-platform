{{ config(materialized='table') }}

{#
    Shell: Tyrell Corp Ads Combined Report
    Factory: build_ads_blended_fact
    Sources: facebook_ads, google_ads, instagram_ads
    
    Tenant-isolated, config-driven ad performance across all paid platforms.
#}

{{ build_ads_blended_fact('tyrell_corp') }}
