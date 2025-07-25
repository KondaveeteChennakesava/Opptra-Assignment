import json
import os
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import schedule
import time

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('etl_pipeline.log'),
            logging.StreamHandler()
        ]
    )

class FileConverter:
    def json_to_ndjson(self, input_path, output_path):
        with open(input_path) as infile, open(output_path, 'w') as outfile:
            data = json.load(infile)
            for record in (data if isinstance(data, list) else [data]):
                outfile.write(json.dumps(record) + '\n')
        return output_path

class DataUnifier:
    def create_unified(self, clickstream_path, user_path, marketing_path, out_path):

        with open(clickstream_path) as f: events = json.load(f)
        with open(user_path) as f: users = json.load(f)
        with open(marketing_path) as f: marketing = json.load(f)

        users_dict = {user['user_id']: user for user in users}
        campaigns_dict = {camp['user_id']: camp for camp in marketing}
        
        with open(out_path, 'w') as out:
            for e in events:
                u = users_dict.get(e['user_id'], {})
                c = campaigns_dict.get(e['user_id'], {})
                record = {
                    'event_id': e.get('event_id'),
                    'event_type': e.get('event_type'),
                    'event_timestamp': e.get('event_timestamp'),
                    'email': u.get('email'), 'signup_date': u.get('signup_date'),
                    'campaign_id': c.get('campaign_id'), 'channel': c.get('channel')
                }
                out.write(json.dumps(record) + '\n')
        return out_path

class BigQueryManager:
    def __init__(self, project_id, dataset_id):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id

    def create_dataset_if_not_exists(self):
        dataset_ref = self.client.dataset(self.dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            logging.info(f"Dataset {self.dataset_id} exists")
        except NotFound:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"
            self.client.create_dataset(dataset)
            logging.info(f"Created dataset {self.dataset_id}")

    def create_table(self, table_name, schema):
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        try:
            self.client.get_table(table_ref)
            logging.info(f"Table {table_name} exists")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            self.client.create_table(table)
            logging.info(f"Created table {table_name}")

    def load_ndjson_to_table(self, ndjson_path, table_name):
        table_ref = self.client.dataset(self.dataset_id).table(table_name)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False
        )
        with open(ndjson_path, "rb") as source:
            job = self.client.load_table_from_file(source, table_ref, job_config=job_config)
        job.result()
        table = self.client.get_table(table_ref)
        logging.info(f"Loaded {table.num_rows} rows to {table_name}")

class ETLPipeline:
    def __init__(self, config, bq_manager, unifier, converter):
        self.config = config
        self.bq = bq_manager
        self.unifier = unifier
        self.converter = converter
    
    def run(self):
        logging.info("Starting ETL pipeline")
        self.bq.create_dataset_if_not_exists()
        for source in self.config['tables']:
            ndjson = self.converter.json_to_ndjson(
                source['json'], source['ndjson']
            )
            self.bq.create_table(source['table'], source['schema'])
            self.bq.load_ndjson_to_table(ndjson, source['table'])
        unified_path = self.unifier.create_unified(
            self.config['clickstream_json'],
            self.config['users_json'],
            self.config['marketing_json'],
            self.config['unified_ndjson']
        )
        self.bq.create_table('unified_data', self.config['unified_schema'])
        self.bq.load_ndjson_to_table(unified_path, 'unified_data')
        logging.info("Pipeline finished successfully")

def get_config():
    os.makedirs('bigQuery_dataset', exist_ok=True)
    return {
        'tables': [
            {
                'json': 'clickstream_v1.json',
                'ndjson': 'bigQuery_dataset/clickstream_v1.ndjson',
                'table': 'clickstream',
                'schema': [
                    bigquery.SchemaField("event_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("event_timestamp", "TIMESTAMP", mode="REQUIRED"),
                ]
            },
            {
                'json': 'user_profile_v1.json',
                'ndjson': 'bigQuery_dataset/user_profile_v1.ndjson',
                'table': 'user_profile',
                'schema': [
                    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("email", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("signup_date", "DATE", mode="REQUIRED"),
                ]
            },
            {
                'json': 'marketing_data_v1.json',
                'ndjson': 'bigQuery_dataset/marketing_data_v1.ndjson',
                'table': 'marketing_data',
                'schema': [
                    bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("campaign_id", "STRING", mode="REQUIRED"),
                    bigquery.SchemaField("channel", "STRING", mode="REQUIRED"),
                ]
            }
        ],
        'clickstream_json': 'clickstream_v1.json',
        'users_json': 'user_profile_v1.json',
        'marketing_json': 'marketing_data_v1.json',
        'unified_ndjson': 'bigQuery_dataset/unified_data.ndjson',
        'unified_schema': [
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("event_timestamp", "TIMESTAMP"),
            bigquery.SchemaField("email", "STRING"),
            bigquery.SchemaField("signup_date", "DATE"),
            bigquery.SchemaField("campaign_id", "STRING"),
            bigquery.SchemaField("channel", "STRING"),
        ]
    }

def run_scheduled_job():
    setup_logging()
    config = get_config()
    bq_manager = BigQueryManager("opptra-assignment", "clickstream")
    converter = FileConverter()
    unifier = DataUnifier()
    pipeline = ETLPipeline(config, bq_manager, unifier, converter)
    pipeline.run()

if __name__ == "__main__":
    run_scheduled_job()
    schedule.every().day.at("02:00").do(run_scheduled_job)
    logging.info("Scheduler started. Pipeline will run daily at 2:00 AM")
    while True:
        schedule.run_pending()
        time.sleep(60)
