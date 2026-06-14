import json
import logging
import re
import time
from datetime import datetime, timezone
import requests
from config import config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.congress.gov/v3/bill"


def fetch_bills_data():
    logger.info("Starting the data ingestion from Congress API")

    limit = 250
    # offset = 0
    # all_bills = []
    has_more_data = True
    unix_ts = int(time.time())

    try:
        now_utc = datetime.now(timezone.utc)
        current_date = now_utc.strftime("%Y-%m-%d")
        partition_date = f"ingested_at={current_date}"

        full_dir_path = config.bronze_path / "bills" / partition_date
        full_dir_path.mkdir(parents=True, exist_ok=True)

        existing_offsets = []

        for file in full_dir_path.glob("raw_bills_offset_*.json"):
            match = re.search(r"raw_bills_offset_(\d+)_", file.name)
            if match:
                existing_offsets.append(int(match.group(1)))

        if existing_offsets:
            last_successful_offset = max(existing_offsets)
            offset = last_successful_offset + limit
        else:
            offset = 0

        while has_more_data:
            logger.info("Starting pagination with limit=%s and offset=%s", limit, offset)

            query_params = {
                "api_key": config.congress_api_key.get_secret_value(),
                "format": "json",
                "limit": limit,
                "offset": offset,
            }

            response = requests.get(BASE_URL, timeout=25, params=query_params)
            response.raise_for_status()
            data = response.json()

            current_batch = data.get("bills", [])
            # all_bills.extend(current_batch)

            if current_batch:
                full_name = f"raw_bills_offset_{offset}_{unix_ts}.json"
                full_file_path = full_dir_path / full_name
                with open(full_file_path, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

            logger.info(f"Success retrieving {len(current_batch)} bills from API")

            pagination_info = data.get("pagination", {})

            if "next" in pagination_info and len(current_batch) == limit:
                offset += limit
            else:
                has_more_data = False

        # ARREGLA EL PUTO ALL_BILLS ARRAY (EN EL FUTURO)
        # logger.info(f"Pagination completed with {len(all_bills)} bills")

        logger.info("Data successfully saved to %s", full_file_path)

        return full_file_path

    except Exception as e:
        logger.error(f"Failed to request data from Base URL: {e}")
        raise
