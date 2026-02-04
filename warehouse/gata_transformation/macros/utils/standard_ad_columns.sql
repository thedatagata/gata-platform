{%- macro standard_ad_columns(source_label, date_col, campaign_name_col, ad_group_name_col, campaign_id_col, ad_group_id_col, utm_source_col=none, utm_medium_col=none, utm_campaign_col=none, spend_col='spend', impressions_col='impressions', clicks_col='clicks', conversions_col='conversions') -%}
  '{{ source_label }}' as source,
    CAST({{ date_col }} AS DATE) as date,
    {{ clean_string(campaign_name_col) }} as campaign_name,
    {{ clean_string(ad_group_name_col) }} as ad_group_name,
    {{ clean_string(campaign_id_col) }} as campaign_id,
    {{ clean_string(ad_group_id_col) }} as ad_group_id,
    LOWER({{ clean_string(utm_source_col if utm_source_col else 'NULL') }}) as utm_source,
    LOWER({{ clean_string(utm_medium_col if utm_medium_col else 'NULL') }}) as utm_medium,
    LOWER({{ clean_string(utm_campaign_col if utm_campaign_col else 'NULL') }}) as utm_campaign,
    SUM(CAST({{ spend_col }} AS DOUBLE)) as spend,
    SUM(CAST({{ impressions_col }} AS BIGINT)) as impressions,
    SUM(CAST({{ clicks_col }} AS BIGINT)) as clicks,
    SUM(CAST({{ conversions_col }} AS DOUBLE)) as conversions
{%- endmacro -%}
