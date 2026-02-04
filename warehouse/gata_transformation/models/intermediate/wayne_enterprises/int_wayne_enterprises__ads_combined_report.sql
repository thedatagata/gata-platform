{{ config(materialized='table') }}

{#
    Shell: Wayne Enterprises Ads Combined Report
    Factory: build_ads_blended_fact
    Sources: google_ads only (no FB/IG)
#}

{{ build_ads_blended_fact('wayne_enterprises') }}
