WITH base_events AS (
    {{ generate_master_union('events') }}
),

hydrated AS (
    SELECT 
        tenant_slug,
        source_platform,
        tenant_skey,
        loaded_at,
        
        -- Standard Event Fields
        {{ extract_field('event_name') }},
        {{ extract_field('event_timestamp', 'bigint') }} as event_timestamp_raw,
        
        -- User Identifiers
        {{ extract_field('user_id') }},
        {{ extract_field('user_pseudo_id') }},
        
        -- Platform Specific Extraction
        CASE 
            WHEN source_platform LIKE '%google_analytics%' THEN 
                {{ extract_ga4_param('ga_session_id', should_alias=False) }}
            ELSE 
                {{ extract_field('session_id', should_alias=False) }}
        END as session_id,
        
        CASE
            WHEN source_platform LIKE '%google_analytics%' THEN
                {{ extract_ga4_param('page_location', should_alias=False) }}
            ELSE
                {{ extract_field('url', should_alias=False) }}
        END as page_location,

        raw_data_payload
    FROM base_events
)

SELECT 
    *,
    -- Timestamp Standardization
    CASE 
        WHEN event_timestamp_raw > 1000000000000000 THEN to_timestamp(event_timestamp_raw / 1000000) -- micros
        ELSE to_timestamp(event_timestamp_raw / 1000) -- millis
    END as event_timestamp,
    
    {{ gen_tenant_key(['source_platform', 'session_id', 'event_timestamp_raw']) }} as event_key,
    
    -- Attribution Extraction (Enrichment)
    {{ extract_utm('page_location', 'utm_source') }} as utm_source,
    {{ extract_utm('page_location', 'utm_medium') }} as utm_medium,
    {{ extract_utm('page_location', 'utm_campaign') }} as utm_campaign
    
FROM hydrated
