{{ config(materialized='table') }}

{{ build_ads_blended_fact('facebook_ads_api_v1_ads') }}