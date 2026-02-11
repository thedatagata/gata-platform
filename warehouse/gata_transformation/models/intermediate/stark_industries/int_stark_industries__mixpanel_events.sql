{{ generate_intermediate_unpacker(
    tenant_slug='stark_industries',
    source_platform='mixpanel',
    master_model_id='mixpanel_api_v1_events',
    columns=[
        {'json_key': 'event', 'alias': 'event_name', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_distinct_id', 'alias': 'user_pseudo_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_distinct_id', 'alias': 'user_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_time', 'alias': 'event_timestamp', 'cast_to': 'BIGINT', 'expression': "CAST(raw_data_payload->>'$.prop_time' AS BIGINT) * 1000"},
        {'json_key': 'prop_city', 'alias': 'geo_city', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_country_code', 'alias': 'geo_country', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_device_type', 'alias': 'device_category', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_utm_source', 'alias': 'traffic_source', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_utm_medium', 'alias': 'traffic_medium', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_utm_campaign', 'alias': 'traffic_campaign', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_order_id', 'alias': 'prop_order_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'prop_revenue', 'alias': 'prop_revenue', 'cast_to': 'DOUBLE'}
    ]
) }}