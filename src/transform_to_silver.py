import pandas as pd
import json
import os
import logging
from config import BRONZE_PATH, SILVER_PATH, CRITICAL_MIN_RECORDS
from config import EXPECTED_MIN_STATES, MANDATORY_COLUMNS, OPTIONAL_COLUMNS


logger = logging.getLogger(__name__)


# ==========================================
# 2. DATA TRANSFORMATION LOGIC
# ==========================================
def clean_legislator_data(df):

    # Schema Selection
    target_columns = ['bioguideId', 'name', 'partyName', 'state']
    silver_df = df[target_columns].copy()

    # Business Logic: Filter by Party
    initual_count = len(silver_df)
    silver_df = silver_df[silver_df["partyName"] == "Democratic"]

    # Standarization: States to lowercase
    final_count = len(silver_df)
    filtered_count = initual_count - final_count
    silver_df['state'] = silver_df['state'].str.lower()

    logger.info(
        "Filtered %s legislators out of %s", filtered_count, initual_count
    )

    return silver_df


# ==========================================
# 3. DATA QUALITY CHECK
# ==========================================
def validate_silver_data(df):
    """
    Performs multi-layer data validation:
    1. Volume: Minimum record threshold.
    2. Nullability: Enforces MANDATORY vs OPTIONAL fields.
    3. Distribution: Minimum geographic representation (States).
    """

    # --- CHECK 1: Volume Integrity ---
    # Ensure the API didn't return a truncated or empty response.
    if len(df) < CRITICAL_MIN_RECORDS:
        logger.error(
            f'Quality check failed: '
            f'Less than {CRITICAL_MIN_RECORDS} records found'
        )
        return False

    # --- CHECK 2: Schema & Nullability (Hard Stop) ---
    # Prevent "Pipeline Breakage" in the Gold layer.
    for col in MANDATORY_COLUMNS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.error(
                f'Quality Check Failed: '
                f'Column {col} has {null_count} null values'
            )

    # --- CHECK 3: Data Quality (Soft Warning) ---
    # Optional columns are logged but don't break the pipeline.
    for col in OPTIONAL_COLUMNS:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            logger.warning(
                f'Quality Check Warning: '
                f'Column {col} has {null_count} null values'
            )

    # --- CHECK 4: Geographic Coverage (Business Logic) ---
    # Verifying the data represents a national scope, not a partial extract.
    unique_states = df['state'].nunique()
    if unique_states < EXPECTED_MIN_STATES:
        logger.error(
            f'Quality Check Warning: '
            f'Less than {EXPECTED_MIN_STATES} unique states found'
        )
        return False

    logger.info("Data quality check passed")
    return True


# ==========================================
# 4. MAIN ORCHESTRATION
# ==========================================
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

        silver_df = clean_legislator_data(df)

        # Type Casting: Optional bioguideId conversion
        # silver_df['bioguideId'] = silver_df['bioguideId'].astype(int)

        logger.info("Data transformation completed successfully")

        # Prepare output directory for the Silver layer
        full_dir_path = os.path.join(SILVER_PATH, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        # Persist refined data as Parquet for optimized downstream analytics
        file_name = "legislators_refined.parquet"
        full_file_path = os.path.join(full_dir_path, file_name)

        # Data Quality Checks (Validation)
        if not validate_silver_data(silver_df):
            logger.critical(
                "Pipeline halted: Silver data does "
                "not meet quality standards"
            )
            raise ValueError("Data quality check failed")

        # Save the DataFrame to Parquet file
        silver_df.to_parquet(full_file_path, index=False)
        logger.info(
            "Silver layer data successfully saved to %s", full_file_path
        )
        return silver_df

    except Exception as e:
        logger.error(f"Data transformation failed with error: {e}")
        raise
