import os
import requests
import json
import time
import logging
from datetime import datetime, timezone
from config import config

# Configure logging
logger = logging.getLogger(__name__)

os.makedirs(config.bronze_path, exist_ok=True)
BASE_URL = "https://api.congress.gov/v3/member"


def fetch_legislator_data():

    logger.info("Starting the data ingestion from Congress API")

    query_params = {
        "api_key": config.congress_api_key.get_secret_value(),
        "format": "json",
        "currentMember": "true"
    }

    # Save the data to a file in JSON format
    try:
        logger.info("Requesting data from Base URL: %s", BASE_URL)

        # Make a GET request to the API endpoint
        response = requests.get(BASE_URL, timeout=10, params=query_params)

        # Check if the request was successful
        response.raise_for_status()

        # Parse the JSON response
        data = response.json()

        member_count = len(data.get("members", []))
        logger.info("Successfully retrieved %s members from API", member_count)

        # Get the current date and time in UTC timezone
        now_utc = datetime.now(timezone.utc)
        partition_date = now_utc.strftime("%Y-%m-%d")
        unix_ts = int(time.time())

        # Create the directory if it doesn't exist already
        full_dir_path = os.path.join(
            config.bronze_path, f"ingested_at={partition_date}"
        )
        os.makedirs(full_dir_path, exist_ok=True)

        # Save the data to a file
        file_name = f"raw_comm_{unix_ts}.json"
        full_file_path = os.path.join(full_dir_path, file_name)

        # Write the data to the file in JSON format with indentation and UTF-8
        with open(full_file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info(
            "Data ingestion from Congress API completed successfully"
        )
        return full_file_path

    except Exception as e:
        logger.error("Critical error during data ingestion: %s", str(e))
        raise
