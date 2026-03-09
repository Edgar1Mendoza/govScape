import os
import requests
import json
import time
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

BRONZE_PATH = "data/bronze/legislators_comms"
os.makedirs(BRONZE_PATH, exist_ok=True)
BASE_URL = "https://api.congress.gov/v3/member"
API_KEY = os.getenv("CONGRESS_API_KEY")

if not API_KEY:
    raise ValueError("CONGRESS_API_KEY environment variable is not set")


def fetch_legislator_data():

    query_params = {
        "api_key": API_KEY,
        "format": "json",
        "currentMember": "true"
    }

    print('Starting the fetch process')

    # Save the data to a file in JSON format
    try:
        # Make a GET request to the API endpoint
        response = requests.get(BASE_URL, timeout=10, params=query_params)

        # Check if the request was successful
        response.raise_for_status()
        
        data = response.json()

        # Get the current date and time in UTC timezone
        now_utc = datetime.now(timezone.utc)
        partition_date = now_utc.strftime("%Y-%m-%d")
        unix_ts = int(time.time())

        # Create the directory if it doesn't exist already
        full_dir_path = os.path.join(BRONZE_PATH, f"\
        ingested_at={partition_date}")
        os.makedirs(full_dir_path, exist_ok=True)

        # Save the data to a file
        file_name = f"raw_comm_{unix_ts}.jason"
        full_file_path = os.path.join(full_dir_path, file_name)

        # Write the data to the file in JSON format with indentation and UTF-8
        with open(full_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        print("Data saved successfully to:", full_file_path)
        return full_file_path
    except Exception as e:
        print("An error occurred:", str(e))
        return None


if __name__ == "__main__":

    raw = fetch_legislator_data()
