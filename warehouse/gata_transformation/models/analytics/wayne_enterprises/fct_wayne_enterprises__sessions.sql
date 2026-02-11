{{ config(materialized='table') }}
{{ build_fct_sessions('wayne_enterprises', 'google_analytics', []) }}
