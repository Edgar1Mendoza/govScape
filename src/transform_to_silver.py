import pandas as pd
import json
import os
import logging

logger = logging.getLogger(__name__)

BRONZE_PATH = "data/bronze/legislators_comms"
SILVER_PATH = "data/silver/legislators_comms"


def transform_to_silver(processing_date):
    # Refines raw JSON data from Bronze to a structured Parquet in Silver.
    partition_date = f"ingested_at={processing_date}"
    input_dir = os.path.join(BRONZE_PATH, partition_date)

    try:
        logger.info(
            'Starting data transformation for date: %s', partition_date
        )

        if not os.path.exists(input_dir):
            logger.warning('Input directory %s does not exist.', input_dir)
            return None

        # Identify available JSON files in the partition
        files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
        if not files:
            logger.warning(
                'No JSON files found in %s. Skipping transformation', input_dir
            )
            return None

        # Sort and select the most recent file for processing
        files.sort()
        target_file = files[-1]
        input_path = os.path.join(input_dir, target_file)

        logger.info('Processing latest JSON file: %s', target_file)

        # Raw data ingestion
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        # Data flattening and DataFrame initialization
        records = raw_data.get("members", [])
        df = pd.DataFrame(records)

        if df.empty:
            logger.warning("Dataframe is empty")
            return None
        logger.info("Extracted %s records from %s", len(df), input_path)

        # Schema enforcement: Select required features
        target_columns = ['bioguideId', 'name', 'partyName', 'state']
        silver_df = df[target_columns].copy()

        # Business Logic: Filter for Democratic party members
        initual_count = len(silver_df)
        silver_df = silver_df[silver_df["partyName"] == "Democratic"]
        final_count = len(silver_df)
        filtered_count = initual_count - final_count
        logger.info(
            "Filtered %s legislators out of %s", filtered_count, initual_count
        )

        # Data normalization: Standardize state names to lowercase
        silver_df['state'] = silver_df['state'].str.lower()

        # Type Casting: Optional bioguideId conversion
        # silver_df['bioguideId'] = silver_df['bioguideId'].astype(int)

        logger.info("Data transformation completed successfully")

        # Prepare output directory for the Silver layer
        full_dir_path = os.path.join(SILVER_PATH, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        # Persist refined data as Parquet for optimized downstream analytics
        file_name = "legislators_refined.parquet"
        full_file_path = os.path.join(full_dir_path, file_name)

        silver_df.to_parquet(full_file_path, index=False)
        logger.info(
            "Silver layer data successfully saved to %s", full_file_path
        )
        return silver_df

    except Exception as e:
        logger.error(f"Data transformation failed with error: {e}")
        raise


if __name__ == "__main__":
    execution_date = "2026-03-09"
    transform_to_silver(execution_date)
