
{{ config(
    materialized='incremental',
    enabled=true
) }}

WITH staging AS (
    SELECT
        tenant_slug,
        status,
        sources_config,
        config_hash,
        loaded_at as updated_at
    FROM {{ ref('stg_platform_ops__tenant_manifest') }}
),

{% if is_incremental() %}
latest_active_config AS (
    -- Get the last recorded config for each tenant to detect changes
    SELECT 
        tenant_slug, 
        config_hash,
        first_seen_at,
        onboarded_at
    FROM {{ this }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
{% endif %}

final AS (
    SELECT
        staging.tenant_slug,
        {{ generate_tenant_key('staging.tenant_slug', updated_at='staging.updated_at', config_hash='staging.config_hash') }} as tenant_skey,
        staging.status,

        staging.sources_config,
        staging.config_hash,
        staging.updated_at,
        
        -- logic for first_seen_at
        {% if is_incremental() %}
        COALESCE(latest.first_seen_at, staging.updated_at) as first_seen_at,
        {% else %}
        staging.updated_at as first_seen_at,
        {% endif %}

        -- logic for onboarded_at
        CASE 
            WHEN staging.status = 'enabled' THEN 
                {% if is_incremental() %}
                COALESCE(latest.onboarded_at, staging.updated_at)
                {% else %}
                staging.updated_at
                {% endif %}
            ELSE 
                {% if is_incremental() %}
                latest.onboarded_at
                {% else %}
                NULL
                {% endif %}
        END as onboarded_at

    FROM staging
    {% if is_incremental() %}
    LEFT JOIN latest_active_config latest ON staging.tenant_slug = latest.tenant_slug
    WHERE staging.config_hash != latest.config_hash OR latest.config_hash IS NULL
    {% endif %}
)

SELECT * FROM final