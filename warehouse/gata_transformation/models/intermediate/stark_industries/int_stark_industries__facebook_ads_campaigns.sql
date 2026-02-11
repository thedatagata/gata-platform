{{ generate_intermediate_unpacker(
    'stark_industries', 'facebook_ads', 'facebook_ads_api_v1_campaigns',
    [
        {'json_key': 'id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
        {'json_key': 'name', 'alias': 'campaign_name', 'cast_to': 'VARCHAR'},
        {'json_key': 'status', 'alias': 'status', 'cast_to': 'VARCHAR'}
    ]
) }}
