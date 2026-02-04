{%- macro get_clients_to_process() -%}
{%- set manifest = get_manifest() -%}
{%- set slugs = [] -%}
{%- for tenant in manifest.tenants -%}
{%- do slugs.append(tenant.slug) -%}
{%- endfor -%}
{{ return(slugs) }}
{%- endmacro -%}
