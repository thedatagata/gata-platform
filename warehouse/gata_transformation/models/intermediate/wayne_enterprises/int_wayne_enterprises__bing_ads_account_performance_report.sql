{{ generate_intermediate_unpacker(
    'wayne_enterprises', 'bing_ads', 'bing_ads_api_v1_account_performance_report',
    [
        {'json_key': 'TimePeriod', 'alias': 'report_date', 'cast_to': 'DATE'},
        {'json_key': 'Spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
        {'json_key': 'Impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
        {'json_key': 'Clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'}
    ]
) }}
