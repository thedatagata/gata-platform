{#
  Factory: Campaign Dimension
  Unions campaign engines for a tenant's enabled sources.
  Discovers engines by convention: engine_{source}_campaigns

  Usage: {{ build_dim_campaigns('tyrell_corp') }}
#}
{% macro build_dim_campaigns(tenant_slug) %}

{%- set tenant_config = get_tenant_config(tenant_slug) -%}
{%- set ns = namespace(first=true) -%}

{%- if tenant_config and tenant_config.get('sources') -%}
    {%- for source, config in tenant_config['sources'].items() -%}
        {%- if config.get('enabled') -%}
            {%- set engine_fn = context.get('engine_' ~ source ~ '_campaigns') -%}
            {%- if engine_fn -%}
                {%- if not ns.first %} UNION ALL {% endif -%}
                {{ engine_fn(tenant_slug) }}
                {%- set ns.first = false -%}
            {%- endif -%}
        {%- endif -%}
    {%- endfor -%}
{%- endif -%}

{%- if ns.first -%}
    SELECT
        CAST(NULL AS VARCHAR) AS tenant_slug,
        CAST(NULL AS VARCHAR) AS source_platform,
        CAST(NULL AS VARCHAR) AS campaign_id,
        CAST(NULL AS VARCHAR) AS campaign_name,
        CAST(NULL AS VARCHAR) AS campaign_status
    WHERE 1 = 0
{%- endif -%}

{% endmacro %}
