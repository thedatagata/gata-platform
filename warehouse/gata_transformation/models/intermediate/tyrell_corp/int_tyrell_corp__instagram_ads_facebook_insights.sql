{{ generate_intermediate_unpacker(
    'tyrell_corp', 'instagram_ads', 'facebook_ads_api_v1_facebook_insights',
    [
        {'json_key': 'date_start', 'alias': 'report_date', 'cast_to': 'DATE'},
        {'json_key': 'campaign_id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'adset_id', 'alias': 'adset_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'ad_id', 'alias': 'ad_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
        {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
        {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
        {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'}
    ]
) }}
