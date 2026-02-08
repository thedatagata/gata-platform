WITH base AS (
    {{ generate_master_union('campaigns') }}
)
SELECT
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,
    {{ extract_field('id') }},
    {{ extract_field('name') }},
    {{ extract_field('status') }},
    {{ extract_field('objective') }},
    -- Generate Surrogate Key for the Entity
    {{ gen_tenant_key(['source_platform', 'id']) }} as entity_key
FROM base
QUALIFY ROW_NUMBER() OVER (PARTITION BY entity_key ORDER BY loaded_at DESC) = 1
