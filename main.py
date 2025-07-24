# Write python code to join the three datasets on user_id, producing a unified DataFrame that contains:
# event_id, event_type, event_timestamp
# email, signup_date
# campaign_id, channel

import json

with open('clickstream_v1.json', 'r') as f:
    events_data = json.load(f)

with open('user_profile_v1.json', 'r') as f:
    users_data = json.load(f)

with open('marketing_data_v1.json', 'r') as f:
    campaigns_data = json.load(f)


users_dict = {user['user_id']: user for user in users_data}
campaigns_dict = {campaign['user_id']: campaign for campaign in campaigns_data}


unified_data = []
for event in events_data:
    user_id = event['user_id']
    
    user_info = users_dict.get(user_id, {})
    campaign_info = campaigns_dict.get(user_id, {})

    unified_record = {
        'event_id': event.get('event_id'),
        'event_type': event.get('event_type'),
        'event_timestamp': event.get('event_timestamp'),
        'email': user_info.get('email', None),
        'signup_date': user_info.get('signup_date', None),
        'campaign_id': campaign_info.get('campaign_id', None),
        'channel': campaign_info.get('channel', None)
    }
    unified_data.append(unified_record)

print(f"Total unified records: {len(unified_data)}")

with open('unified_data.json', 'w') as jsonfile:
    json.dump(unified_data, jsonfile, indent=4)
    if unified_data:
        print(f"\nData saved to unified_data.json with {len(unified_data)} records")
    else:
        print("No data to save")