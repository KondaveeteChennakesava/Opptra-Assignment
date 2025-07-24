# Converting json files to JSON Lines format for BigQuery ingestion.

import json
import os

def convert_json(file_name, output_dir='bigQuery_dataset'):
    os.makedirs(output_dir, exist_ok=True)
    input_path = file_name + '.json'
    output_path = os.path.join(output_dir, file_name + '.ndjson')
    with open(input_path) as infile, open(output_path, 'w') as outfile:
        data = json.load(infile)
        if isinstance(data, list):
            for obj in data:
                outfile.write(json.dumps(obj) + '\n')
        else:
            outfile.write(json.dumps(data) + '\n')

files = ['clickstream_v1', 'user_profile_v1', 'marketing_data_v1']
for file in files:
    convert_json(file)
