## Scenario
You are working as a data engineer at a company that processes clickstream and user profile data. You are given the following three datasets stored in json format:

clickstream: Contains event-level data of user interactions on the website.

Schema v1:

    event_id (String)
    user_id (String)
    event_type (String)
    event_timestamp (Timestamp)


user_profile: Contains information about users.

Schema v1:

    user_id (String)
    email (String)
    signup_date (Date)


marketing_data: Contains optional marketing campaign metadata.

Schema v1:

    user_id (String)
    campaign_id (String)
    channel (String)

## Task

Write python code to join the three datasets on user_id, producing a unified DataFrame that contains:

    event_id, event_type, event_timestamp
    email, signup_date
    campaign_id, channel

## Folder Structure
```
opptra_assignment/
├── main.py                    # Main script for data integration
├── clickstream_v1.json        # Event tracking data
├── user_profile_v1.json       # User profile information
├── marketing_data_v1.json     # Marketing campaign data
├── unified_data.json          # Output file (generated)
└── README.md                  # Project documentation
```

## Data Sources

### 1. Clickstream Data (`clickstream_v1.json`)
Contains user interaction events with the following fields:
- `event_id`: Unique identifier for each event
- `user_id`: User identifier
- `event_type`: Type of event (page_view, click, scroll, form_submit, download, logout)
- `event_timestamp`: ISO 8601 formatted timestamp

### 2. User Profile Data (`user_profile_v1.json`)
Contains user account information:
- `user_id`: User identifier
- `email`: User's email address
- `signup_date`: Account creation date (YYYY-MM-DD format)

### 3. Marketing Data (`marketing_data_v1.json`)
Contains marketing campaign information:
- `user_id`: User identifier
- `campaign_id`: Unique campaign identifier
- `channel`: Marketing channel (email, social, affiliate, search, display, direct)

## Features
- **Data Integration**: Joins three datasets on `user_id`
- **Left Join Logic**: Preserves all events, even if user profile or marketing data is missing
- **JSON Output**: Saves unified data in JSON format with proper formatting
- **Error Handling**: Gracefully handles missing data with null values
- **Pure Python**: No external dependencies required (uses only built-in libraries)

## Usage

### Prerequisites
- Python 3.6+
- JSON data files in the same directory as the script

### Running the Script
```bash
python main.py
```

### Expected Output
```
Total unified records: 10
Data saved to unified_data.json with 10 records
```

## Output Schema
The unified dataset contains the following fields:
- `event_id`: From clickstream data
- `event_type`: From clickstream data  
- `event_timestamp`: From clickstream data
- `email`: From user profile data
- `signup_date`: From user profile data
- `campaign_id`: From marketing data
- `channel`: From marketing data

## Sample Output Record
```json
{
    "event_id": "evt001",
    "event_type": "page_view",
    "event_timestamp": "2025-07-01T10:00:00Z",
    "email": "user1@example.com",
    "signup_date": "2025-06-15",
    "campaign_id": "cmp001",
    "channel": "email"
}
```

## Data Processing Logic
1. **Load Data**: Reads three JSON files into Python dictionaries
2. **Create Lookup Tables**: Converts user and marketing data into dictionaries for efficient lookups
3. **Join Processing**: For each event, looks up corresponding user and marketing data
4. **Handle Missing Data**: Uses null values when user_id doesn't exist in profile or marketing data
5. **Save Results**: Outputs unified data to JSON file with proper formatting

## Technical Details
- **Join Type**: Left join (preserves all events)
- **Key Field**: `user_id`
- **Memory Efficient**: Uses dictionary lookups for O(1) access time
- **Format**: JSON with 4-space indentation for readability
