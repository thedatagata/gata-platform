{{ config(materialized='table') }}

{#
    Shell: Tyrell Corp Analytics Session Report
    Factory: build_analytics_fact
    Sources: google_analytics
    
    Session-level attribution with conversion detection.
    Logic config from tenants.yaml controls conversion events.
#}

{{ build_analytics_fact('tyrell_corp') }}
