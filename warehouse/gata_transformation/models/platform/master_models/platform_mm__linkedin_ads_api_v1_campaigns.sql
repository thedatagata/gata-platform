{{ config(materialized='table') }}

{{ build_ads_blended_fact('linkedin_ads_api_v1_campaigns') }}