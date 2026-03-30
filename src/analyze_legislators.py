import pandas as pd
import os
import logging
from config import config

logger = logging.getLogger(__name__)


def generate_gold_metrics(processing_date):

    # Get the current date and time in UTC timezone
    partition_date = f"ingested_at={processing_date}"
    input_file = "legislators_refined.parquet"
    input_dir = os.path.join(config.silver_path, partition_date, input_file)

    if not os.path.exists(input_dir):
        logger.warning('Input directory %s does not exist.', input_dir)
        return

    df = pd.read_parquet(input_dir)

    # Check if the DataFrame is empty
    if df.empty:
        logger.warning('Parquet file %s is empty.', input_dir)
        return None
    logger.info('Generating gold metrics for date: %s', partition_date)

    # Top 5 states with most legislators
    top_state = df['state'].value_counts().head(5)

    # Create a gold directory if it doesn't exist and rename the report
    os.makedirs(config.gold_path, exist_ok=True)
    report_name = f'summary_{processing_date}.csv'

    # Save the top 5 states with most legislators
    top_state.to_csv(os.path.join(config.gold_path, report_name), index=True)

    logger.info(
        'Gold metrics generated successfully for date: %s', partition_date
        )
