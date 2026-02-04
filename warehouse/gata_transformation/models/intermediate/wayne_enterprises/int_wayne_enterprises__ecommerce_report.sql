{{ config(materialized='table') }}

{#
    Shell: Wayne Enterprises Ecommerce Report
    Factory: build_ecommerce_fact
    Sources: shopify only (no stripe)
#}

{{ build_ecommerce_fact('wayne_enterprises') }}
