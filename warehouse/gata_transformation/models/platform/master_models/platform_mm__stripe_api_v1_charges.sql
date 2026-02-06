{{ config(materialized='table') }}

{{ build_ecommerce_fact('stripe_api_v1_charges') }}