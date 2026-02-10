-- Tyrell Corp: Orders
-- Sources: Shopify
{{ config(materialized='table') }}

{{ build_fct_orders('tyrell_corp', ['shopify']) }}
