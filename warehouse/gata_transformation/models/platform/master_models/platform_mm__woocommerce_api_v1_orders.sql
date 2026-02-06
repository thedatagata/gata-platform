{{ config(materialized='table') }}

{{ build_ecommerce_fact('woocommerce_api_v1_orders') }}