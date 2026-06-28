from datetime import datetime, timezone
import time
import logging

from ingest_comms_to_bronze import fetch_legislator_data
from transform_to_silver import transform_to_silver
from analyze_legislators import generate_gold_metrics

from ingest_bills_to_bronze import fetch_bills_data
from transform_bills_to_silver import transform_bills_to_silver
from transform_bills_to_gold import transform_bills_to_gold

logger = logging.getLogger(__name__)


def run_pipeline_legislators():

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    start_time = time.time()

    logger.info("Pipeline started at %s", today + ".")

    today = "2026-06-27"

    try:
        fetch_legislator_data()

        transform_to_silver(today)

        generate_gold_metrics(today)

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Pipeline finished successfully in {duration:2f} seconds.")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e} : {time.time()} - {start_time}")
        return


def run_pipeline_bills():

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    start_time = time.time()

    logger.info("Pipeline started at %s", today + ".")

    try:
        fetch_bills_data()
        today = "2026-06-14"

        transform_bills_to_silver(today)

        transform_bills_to_gold(today)

        end_time = time.time()
        duration = end_time - start_time
        logger.info(f"Pipeline finished successfully in {duration:2f} seconds.")

    except Exception as e:
        logger.error(f"Pipeline failed with error: {e} : {time.time()} - {start_time}")
        return


if __name__ == "__main__":
    run_pipeline_legislators()
