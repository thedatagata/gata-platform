{{ config(materialized='table') }}

{{ build_analytics_fact('google_analytics_api_v1_events') }}