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
             {# Parse source platform from table name, e.g., 'platform_mm__google_ads_api...' -> 'google_ads' #}
             {%- set platform = table.split('__')[1].split('_api')[0] -%}
             SELECT 
                *,
                '{{ platform }}' as source_platform
                {%- if apply_logic %}
                {# 1. Inject calculations into the SELECT list #}
                {{ apply_tenant_logic(none, platform, master_type, 'calculation') }}
                {%- endif %}
             FROM {{ ref(table) }}
             WHERE 1=1
             {%- if apply_logic %}
             {# 2. Inject filters into the WHERE clause #}
             {{ apply_tenant_logic(none, platform, master_type, 'filter') }}
             {%- endif %}
             {%- if not loop.last %} UNION ALL {% endif -%}
        {%- endfor -%}
    {%- endif -%}
{% endmacro %}
