# Daily ETL Pipeline for BigQuery

## Overview
This project implements an automated daily ETL (Extract, Transform, Load) pipeline that processes JSON datasets and loads them into Google BigQuery. The pipeline handles clickstream events, user profiles, and marketing campaign data, creating both individual tables and a unified dataset for analytics.

## Project Structure
```
automation/
├── daily_etl_pipeline.py      # Main ETL pipeline script
├── bigQuery_dataset/           # Output directory for processed files
│   ├── clickstream_v1.ndjson
│   ├── user_profile_v1.ndjson
│   ├── marketing_data_v1.ndjson
│   └── unified_data.ndjson
├── etl_pipeline.log           # Pipeline execution logs
└── README.md                  # This file
```

## Features

### 🔄 Automated Data Processing
- **File Conversion**: Converts JSON files to NDJSON (Newline Delimited JSON) format for BigQuery compatibility
- **Data Unification**: Joins three datasets on `user_id` to create a comprehensive unified dataset
- **Scheduled Execution**: Runs automatically every day at 2:00 AM
- **Error Handling**: Comprehensive logging and error management

### 📊 Data Sources
1. **Clickstream Data** (`clickstream_v1.json`): User interaction events
2. **User Profile Data** (`user_profile_v1.json`): User account information  
3. **Marketing Data** (`marketing_data_v1.json`): Campaign and channel data

### 🎯 Output Tables
- `clickstream`: Individual user events
- `user_profile`: User account details
- `marketing_data`: Campaign information
- `unified_data`: Joined dataset combining all three sources

## Architecture

### Class Structure

#### `FileConverter`
Handles JSON to NDJSON conversion for BigQuery ingestion.
```python
json_to_ndjson(input_path, output_path)
```

#### `DataUnifier`
Creates unified dataset by joining multiple data sources.
```python
create_unified(clickstream_path, user_path, marketing_path, out_path)
```

#### `BigQueryManager`
Manages BigQuery operations including dataset/table creation and data loading.
```python
create_dataset_if_not_exists()
create_table(table_name, schema)
load_ndjson_to_table(ndjson_path, table_name)
```

#### `ETLPipeline`
Orchestrates the complete ETL workflow.
```python
run()  # Executes full pipeline
```

## Prerequisites

### 1. Python Dependencies
```bash
pip install google-cloud-bigquery schedule
```

### 2. Google Cloud Setup
- Google Cloud Project with BigQuery API enabled
- Service Account with BigQuery permissions
- Authentication configured (see Authentication section)

### 3. Data Files
Ensure these files exist in the parent directory:
- `clickstream_v1.json`
- `user_profile_v1.json` 
- `marketing_data_v1.json`

## Authentication

### Application Default Credentials (Recommended)
```bash
# Install Google Cloud CLI
curl https://sdk.cloud.google.com | bash
exec -l $SHELL

# Authenticate
gcloud auth application-default login
```

## Configuration

### BigQuery Settings
Update these values in `run_scheduled_job()`:
```python
PROJECT_ID = "your-project-id"       # Your Google Cloud Project ID
DATASET_ID = "your-dataset-name"     # BigQuery dataset name
```

### Table Schemas

#### Clickstream Table
- `event_id` (STRING, REQUIRED)
- `user_id` (STRING, REQUIRED)
- `event_type` (STRING, REQUIRED)
- `event_timestamp` (TIMESTAMP, REQUIRED)

#### User Profile Table
- `user_id` (STRING, REQUIRED)
- `email` (STRING, REQUIRED)
- `signup_date` (DATE, REQUIRED)

#### Marketing Data Table
- `user_id` (STRING, REQUIRED)
- `campaign_id` (STRING, REQUIRED)
- `channel` (STRING, REQUIRED)

#### Unified Data Table
- `event_id` (STRING)
- `event_type` (STRING)
- `event_timestamp` (TIMESTAMP)
- `email` (STRING)
- `signup_date` (DATE)
- `campaign_id` (STRING)
- `channel` (STRING)

## Usage

### One-time Execution
```bash
cd /path/to/automation/
python daily_etl_pipeline.py
```

### Scheduled Execution
The script automatically schedules daily runs at 2:00 AM:
```bash
python daily_etl_pipeline.py
# Script will run continuously, executing pipeline daily
```

### Custom Schedule
Modify the schedule in the main block:
```python
# Run every day at 6:00 AM
schedule.every().day.at("06:00").do(run_scheduled_job)

# Run every Monday at 9:00 AM  
schedule.every().monday.at("09:00").do(run_scheduled_job)

# Run every hour
schedule.every().hour.do(run_scheduled_job)
```

## Monitoring and Logging

### Log File
The pipeline creates `etl_pipeline.log` with detailed execution information:
- Timestamp of each operation
- Success/failure status
- Row counts for loaded tables
- Error messages and stack traces

### Log Levels
- **INFO**: Normal operation messages
- **ERROR**: Error conditions and failures
- **WARNING**: Non-critical issues

### Sample Log Output
```
2025-07-25 02:00:01 - INFO - Starting ETL pipeline
2025-07-25 02:00:02 - INFO - Dataset clickstream exists
2025-07-25 02:00:03 - INFO - Table clickstream exists
2025-07-25 02:00:04 - INFO - Loaded 10 rows to clickstream
2025-07-25 02:00:05 - INFO - Pipeline finished successfully
```

## Data Flow

```
JSON Files → NDJSON Conversion → BigQuery Tables
    ↓
Unified Dataset Creation → BigQuery Unified Table
```

1. **Extract**: Read JSON source files
2. **Transform**: Convert to NDJSON and join datasets
3. **Load**: Upload to BigQuery tables
4. **Schedule**: Repeat daily

## Troubleshooting

### Common Issues

#### Authentication Errors
```
DefaultCredentialsError: Your default credentials were not found
```
**Solution**: Set up Google Cloud authentication (see Authentication section)

#### File Not Found
```
FileNotFoundError: [Errno 2] No such file or directory: 'clickstream_v1.json'
```
**Solution**: Ensure JSON files exist in the parent directory

#### BigQuery Permission Errors
```
google.api_core.exceptions.Forbidden: 403 Access Denied
```
**Solution**: Verify service account has BigQuery Editor permissions

#### Schema Validation Errors
```
google.cloud.exceptions.BadRequest: 400 Invalid schema
```
**Solution**: Check timestamp format in source data (should be ISO 8601)

### Debugging Steps
1. Check log file for detailed error messages
2. Verify file paths and permissions
3. Test BigQuery authentication: `gcloud auth list`
4. Validate JSON file format and structure
5. Ensure BigQuery quotas are not exceeded
