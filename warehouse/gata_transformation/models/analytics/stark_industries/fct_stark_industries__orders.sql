-- Stark Industries: Orders
-- Sources: WooCommerce
{{ config(materialized='table') }}

{{ build_fct_orders('stark_industries', ['woocommerce']) }}
