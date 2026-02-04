{{ config(materialized='table') }}

{#
    Shell: Tyrell Corp Ecommerce Report
    Factory: build_ecommerce_fact
    Sources: shopify, stripe
    
    Unified order + payment view with Shopify-Stripe linkage.
#}

{{ build_ecommerce_fact('tyrell_corp') }}
