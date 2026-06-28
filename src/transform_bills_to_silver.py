import json
import logging
from datetime import datetime, timezone
import pandas as pd
from config import config

logger = logging.getLogger(__name__)


def transform_bills_to_silver(execution_date: str):
    logger.info(f"Starting transformation for date: {execution_date}")

    partition_date = f"ingested_at={execution_date}"

    bronze_dir = config.bronze_path / "bills" / partition_date
    silver_dir = config.silver_path / "bills" / partition_date

    if not bronze_dir:
        logger.warning("Input directory %s does not exist.", bronze_dir)
        return None

    all_bills_list = []

    try:
        json_files = list(bronze_dir.glob("raw_bills_offset_*.json"))

        if not json_files:
            logger.warning("No JSON files found in %s. Skipping transformation", bronze_dir)
            return None

        logger.info(f"Found {len(json_files)} JSON files in Bronze directory")

        for file_path in json_files:
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                batch_bills = data.get("bills", [])
                all_bills_list.extend(batch_bills)

        df_raw = pd.DataFrame(all_bills_list)
        logger.info(f"Created DataFrame with {df_raw.shape[0]} rows")

        columns_to_keep = {
            "number": "bill_number",
            "type": "bill_type",
            "title": "title",
            "updateDate": "update_date",
            "url": "congress_url",
        }

        available_columns = [col for col in columns_to_keep.keys() if col in df_raw.columns]

        df_refined = df_raw[available_columns].rename(columns=columns_to_keep)

        if "update_date" in df_refined.columns:
            df_refined["update_date"] = pd.to_datetime(df_refined["update_date"], errors="coerce")

        df_refined["proccessed_at"] = pd.to_datetime(datetime.now(timezone.utc))

        silver_dir.mkdir(parents=True, exist_ok=True)
        output_file_path = silver_dir / "bills_refined.parquet"

        df_refined.to_parquet(output_file_path, index=False, compression="snappy")

        logger.info(f"Silver layer successfully created. Saved {len(df_refined)} rows")

        return output_file_path

    except Exception as e:
        logger.error(f"An error occurred during transformation: {e}")
        raise
