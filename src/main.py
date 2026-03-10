from ingest_comms_to_bronze import fetch_legislator_data
from transform_to_silver import transform_to_silver
from analyze_legislators import generate_gold_metrics
from datetime import datetime


def run_pipeline():

    today = datetime.now().strftime("%Y-%m-%d")
    print(f'Starting the pipeline for {today}...')

    # Ingestion of legislator data to bronze layer
    fetch_legislator_data()

    # Transformation of data to silver layer
    transform_to_silver(today)

    # Analysis of legislator data
    generate_gold_metrics(today)

    print('Pipeline completed successfully!')


if __name__ == "__main__":
    run_pipeline()
