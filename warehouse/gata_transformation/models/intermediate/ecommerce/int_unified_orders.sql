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
    {{ extract_field('customer_id') }}, -- Added per refinement plan (might be user_id in some raw data, keeping consistent with request)
    {{ extract_field('user_id') }}, -- Some sources use user_id
    {{ extract_field('browser_ip') }},
    {{ extract_field('landing_site') }}
FROM base
