import logging
from datetime import datetime, timezone
import pandas as pd
from config import config

logger = logging.getLogger(__name__)


def transform_bills_to_gold(execution_date: str):
    logger.info(f"Starting gold analytics transformation for date: {execution_date}")

    partition_date = f"ingested_at={execution_date}"

    silver_file = config.silver_path / "bills" / partition_date / "bills_refined.parquet"
    gold_dir = config.gold_path / "bills" / partition_date

    if not silver_file:
        logger.error(f"Missing Silver source file: {silver_file}")
        return None

    try:
        df_silver = pd.read_parquet(silver_file)
        logger.info(f"Loaded {len(df_silver)} records from Silver")

        df_gold = df_silver.copy()
        df_gold = df_gold.groupby("bill_type").size().reset_index(name="total_bills")
        df_gold.sort_values("total_bills", ascending=False)
        df_gold = df_gold.rename(columns={"bill_type": "legislative_type"})

        df_gold["report_date"] = pd.to_datetime(execution_date)
        df_gold["calculated_at"] = pd.to_datetime(datetime.now(timezone.utc))

        gold_dir.mkdir(parents=True, exist_ok=True)
        output_file_path = gold_dir / "bills_activity_sumary.parquet"

        df_gold.to_parquet(output_file_path, index=False, compression="snappy")

        logger.info(f"Saved {len(df_gold)} records to Gold")

        print(df_gold.head(10))

        return output_file_path

    except Exception as e:
        logger.error(f"Failed to transform data in Gold analytics: {e}")
        raise
