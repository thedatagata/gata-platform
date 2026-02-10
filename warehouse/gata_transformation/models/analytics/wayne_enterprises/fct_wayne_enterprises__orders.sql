-- Wayne Enterprises: Orders
-- Sources: BigCommerce
{{ config(materialized='table') }}

{{ build_fct_orders('wayne_enterprises', ['bigcommerce']) }}
