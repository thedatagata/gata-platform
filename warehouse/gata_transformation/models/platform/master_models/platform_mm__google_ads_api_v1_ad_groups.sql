{{ config(materialized='table') }}

{{ build_ads_blended_fact('google_ads_api_v1_ad_groups') }}