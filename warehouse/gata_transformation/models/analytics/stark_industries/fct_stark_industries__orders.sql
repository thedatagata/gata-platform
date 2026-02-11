{{ config(materialized='table') }}
{{ build_fct_orders('stark_industries', ['woocommerce']) }}
