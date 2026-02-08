WITH base AS (
    {{ generate_master_union('ad_performance') }}
),

hydrated AS (
    SELECT 
        tenant_slug,
        source_platform,
        tenant_skey,
        loaded_at,
        {{ extract_field('date_start', 'date') }},
        {{ extract_field('spend', 'double') }},
        {{ extract_field('impressions', 'bigint') }},
        {{ extract_field('clicks', 'bigint') }},
        {{ extract_field('campaign_id') }}
    FROM base
)

SELECT 
    *,
    {{ gen_tenant_key(['source_platform', 'campaign_id']) }} as campaign_key
FROM hydrated
WHERE 1=1
{{ apply_tenant_logic(tenant_slug, source_name, 'ad_performance', 'filter') }}
