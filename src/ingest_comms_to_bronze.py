import os
import requests
import json
import time
from datetime import datetime, timezone

BRONZE_PATH = "data/bronze/legislators_comms"
os.makedirs(BRONZE_PATH, exist_ok=True)


def fetch_legislator_data():
    print('Starting the fetch process')
    # Make a GET request to the API endpoint
    url = "https://rickandmortyapi.com/api/character"
    response = requests.get(url, timeout=10)

    # Check if the request was successful
    response.raise_for_status()

    # Save the data to a file in JSON format
    if response.status_code == 200:
        data = response.json()
        
        # Get the current date and time in UTC timezone
        now_utc = datetime.now(timezone.utc)
        partition_date = now_utc.strftime("%Y-%m-%d")
        unix_ts = int(time.time())

        # Create the directory if it doesn't exist already
        full_dir_path = os.path.join(BRONZE_PATH, f"ingested_at={partition_date}")
        os.makedirs(full_dir_path, exist_ok=True)
        
        # Save the data to a file
        file_name = f"raw_comm_{unix_ts}.jason"
        full_file_path = os.path.join(full_dir_path, file_name)

        # Write the data to the file in JSON format with indentation and UTF-8 encoding
        with open(full_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Data saved successfully to:", full_file_path)
        
raw = fetch_legislator_data()
