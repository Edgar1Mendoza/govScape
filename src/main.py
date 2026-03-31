import logging
import time
from ingest_comms_to_bronze import fetch_legislator_data
from transform_to_silver import transform_to_silver
from analyze_legislators import generate_gold_metrics
from datetime import datetime, timezone

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("govscape_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def run_pipeline():

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    start_time = time.time()

    logger.info("Pipeline started at %s", today + ".")

    try:
        # Ingestion of legislator data to bronze layer
        fetch_legislator_data()

        # Transformation of data to silver layer
        transform_to_silver(today)

        # Analysis of legislator data
        generate_gold_metrics(today)

        end_time = time.time()
        duration = end_time - start_time
        logger.info(
            f"Pipeline finished successfully in {duration:2f} seconds."
            )

    except Exception as e:
        logger.error(
            f"Pipeline failed with error: {e} : {time.time()} - {start_time}"
            )
        return


if __name__ == "__main__":
    run_pipeline()
