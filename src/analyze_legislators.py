import pandas as pd
import os

SILVER_PATH = "data/silver/legislators_comms"
GOLD_PATH = "data/gold/legislators_reports"


def generate_gold_metrics(processing_date):

    partition_date = f"ingested_at={processing_date}"
    input_file = "legislators_refined.parquet"
    input_dir = os.path.join(SILVER_PATH, partition_date, input_file)

    if not os.path.exists(input_dir):
        print(f"File file not found: {input_dir}")
        return

    df = pd.read_parquet(input_dir)

    print(f'Analyzing data for {partition_date}...')

    top_state = df['state'].value_counts().head(5)

    print(f'Top 5 states with most legislators: {top_state}')

    os.makedirs(GOLD_PATH, exist_ok=True)
    report_name = f'summary_{processing_date}.csv'
    top_state.to_csv(os.path.join(GOLD_PATH, report_name), index=True)

    print('Report generated successfully!')


if __name__ == "__main__":

    generate_gold_metrics('2026-03-09')
