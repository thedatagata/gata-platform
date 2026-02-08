WITH base AS (
    {{ generate_master_union('orders') }}
)
SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,
    {{ extract_field('id') }},
    {{ extract_field('order_number') }},
    {{ extract_field('total_price', 'double') }},
    {{ extract_field('currency') }},
    {{ extract_field('created_at', 'timestamp') }},
    {{ extract_field('financial_status') }} as status,
    -- Identifiers for Identity/Session Linking
    {{ extract_field('email') }},
    {{ extract_field('user_id') }} as customer_id,
    {{ extract_field('browser_ip') }},
    {{ extract_field('landing_site') }}
FROM base
