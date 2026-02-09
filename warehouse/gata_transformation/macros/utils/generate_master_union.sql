{% macro generate_master_union(master_type, apply_logic=True) %}
    {# 
       Mapping concept names to physical master tables.
    #}
    {%- set mappings = {
        'campaigns': [
            'platform_mm__facebook_ads_api_v1_campaigns',
            'platform_mm__google_ads_api_v1_campaigns',
            'platform_mm__bing_ads_api_v1_campaigns',
            'platform_mm__linkedin_ads_api_v1_campaigns',
            'platform_mm__amazon_ads_api_v1_sponsored_products_campaigns'
        ],
        'products': [
             'platform_mm__shopify_api_v1_products',
             'platform_mm__bigcommerce_api_v1_products',
             'platform_mm__woocommerce_api_v1_products'
        ],
        'orders': [
             'platform_mm__shopify_api_v1_orders',
             'platform_mm__bigcommerce_api_v1_orders',
             'platform_mm__woocommerce_api_v1_orders'
        ],
        'ad_performance': [
             'platform_mm__facebook_ads_api_v1_facebook_insights',
             'platform_mm__google_ads_api_v1_ad_performance',
             'platform_mm__bing_ads_api_v1_account_performance_report', 
             'platform_mm__linkedin_ads_api_v1_ad_analytics_by_campaign'
        ],
        'events': [
             'platform_mm__google_analytics_api_v1_events',
             'platform_mm__amplitude_api_v1_events',
             'platform_mm__mixpanel_api_v1_events'
        ],
        'users': [
             'platform_mm__amplitude_api_v1_users',
             'platform_mm__mixpanel_api_v1_people',
             'platform_mm__google_ads_api_v1_customers',
             'platform_mm__shopify_api_v1_customers'
        ]
    } -%}

    {%- set tables = mappings.get(master_type, []) -%}

    {%- if not tables -%}
        SELECT 'No tables found for type: {{ master_type }}' as error
    {%- else -%}
        {%- for table in tables -%}
             {# Parse source_name from physical table string (e.g. 'platform_mm__google_ads_api...') #}
             {%- set parts = table.split('__') -%}
             {%- set source_part = parts[1] if parts|length > 1 else 'unknown' -%}
             {%- set source_name = source_part.split('_api')[0] -%}
             
             SELECT 
                 *, 
                 '{{ table }}' as _table_source
                 
                 {%- if apply_logic -%}
                     {# Injects calculated columns into the SELECT list #}
                     {{ apply_tenant_logic(none, source_name, master_type, 'calculation') }}
                 {%- endif -%}
                 
             FROM {{ ref(table) }}
             WHERE 1=1
             {%- if apply_logic -%}
                 {# Injects filters into the WHERE clause #}
                 {{ apply_tenant_logic(none, source_name, master_type, 'filter') }}
             {%- endif -%}
             
             {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
