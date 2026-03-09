import pandas as pd
import json
import os

# Set the paths for the bronze and silver data
BRONZE_PATH = "data/bronze/legislators_comms"
SILVER_PATH = "data/silver/legislators_comms"


def transform_to_silver(date_folder):

    input_dir = os.path.join(BRONZE_PATH, f"ingested_at={date_folder}")

    files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
    if not files:
        print(f"No JSON files found in {input_dir}")
        return None
    input_path = os.path.join(input_dir, files[0])

    with open(input_path, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    items = raw_data.get('results', [])

    df = pd.DataFrame(items)
    return df


if __name__ == "__main__":
    date_folder = "2023-08-01"
    input_path = transform_to_silver(date_folder)
    print(input_path)
