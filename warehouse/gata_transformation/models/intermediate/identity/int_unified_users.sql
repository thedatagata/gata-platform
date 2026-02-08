WITH base_users AS (
    {{ generate_master_union('users') }}
)
SELECT 
    tenant_slug,
    source_platform,
    tenant_skey,
    loaded_at,
    {{ extract_field('id') }} as source_user_id,
    {{ extract_field('email') }},
    {{ extract_field('created_at', 'timestamp') }},
    
    -- Generate Surrogate Key
    {{ gen_tenant_key(['source_platform', 'id']) }} as user_key
FROM base_users
