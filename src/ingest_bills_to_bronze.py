import json
import logging
import time
from datetime import datetime, timezone
import requests
from config import config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.congress.gov/v3/bill"


def fetch_bills_data():
    logger.info("Starting the data ingestion from Congress API")

    query_params = {
        "api_key": config.congress_api_key.get_secret_value(),
        "format": "json",
        "limit": 250,
    }

    try:
        logger.info("Requesting data from Base URL: %s with limit=250", BASE_URL)

        response = requests.get(BASE_URL, timeout=15, params=query_params)

        response.raise_for_status()

        data = response.json()
        bill_count = len(data.get("bills", []))
        logger.info("Successfully retrieved %s bills from API", bill_count)

        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")
        partition_date = f"ingested_at={current_date}"
        unix_ts = int(time.time())

        full_dir_path = config.bronze_path / "bills" / partition_date
        full_dir_path.mkdir(parents=True, exist_ok=True)

        full_name = f"raw_bills_{unix_ts}.json"
        full_file_path = full_dir_path / full_name
        with open(full_file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

        logger.info("Data successfully saved to %s", full_file_path)

        return full_file_path

    except Exception as e:
        logger.error(f"Failed to request data from Base URL: {e}")
