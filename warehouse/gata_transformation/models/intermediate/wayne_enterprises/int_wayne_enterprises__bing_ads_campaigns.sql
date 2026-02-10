{{ generate_intermediate_unpacker(
    'wayne_enterprises', 'bing_ads', 'bing_ads_api_v1_campaigns',
    [
        {'json_key': 'Id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'Name', 'alias': 'campaign_name', 'cast_to': 'VARCHAR'},
        {'json_key': 'Status', 'alias': 'status', 'cast_to': 'VARCHAR'}
    ]
) }}
