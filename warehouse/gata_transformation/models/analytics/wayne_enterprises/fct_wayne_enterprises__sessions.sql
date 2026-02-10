-- Wayne Enterprises: Sessions
-- Sources: Google Analytics
-- No conversion events defined in tenants.yaml
{{ config(materialized='table') }}

{{ build_fct_sessions('wayne_enterprises', 'google_analytics', []) }}
