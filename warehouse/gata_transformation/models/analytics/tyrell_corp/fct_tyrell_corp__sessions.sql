-- Tyrell Corp: Sessions
-- Sources: Google Analytics
-- Conversion events: purchase (from tenants.yaml)
{{ config(materialized='table') }}

{{ build_fct_sessions('tyrell_corp', 'google_analytics', ['purchase']) }}
