{{ config(materialized='table') }}
{{ build_fct_sessions('tyrell_corp', 'google_analytics', ['purchase']) }}
