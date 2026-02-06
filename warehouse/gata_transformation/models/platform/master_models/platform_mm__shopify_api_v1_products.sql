{{ config(materialized='table') }}

{{ build_ecommerce_fact('shopify_api_v1_products') }}