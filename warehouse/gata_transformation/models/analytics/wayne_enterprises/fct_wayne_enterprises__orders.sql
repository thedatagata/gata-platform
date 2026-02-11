{{ config(materialized='table') }}
{{ build_fct_orders('wayne_enterprises', ['bigcommerce']) }}
