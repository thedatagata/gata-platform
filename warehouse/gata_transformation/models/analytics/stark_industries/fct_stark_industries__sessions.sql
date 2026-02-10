-- Stark Industries: Sessions
-- Sources: Mixpanel
-- Conversion events: Purchase (matches generator event name casing)
{{ config(materialized='table') }}

{{ build_fct_sessions('stark_industries', 'mixpanel', ['Purchase']) }}
