import pandas as pd
import json
import os

# Set the paths for the bronze and silver directories
BRONZE_PATH = "data/bronze/legislators_comms"
SILVER_PATH = "data/silver/legislators_comms"


def transform_to_silver(processing_date):

    # Get the list of JSON files in the input directory
    partition_date = f"ingested_at={processing_date}"
    input_dir = os.path.join(BRONZE_PATH, partition_date)
    try:
        # Get the list of JSON files in the input directory
        files = [f for f in os.listdir(input_dir) if f.endswith('.json')]
        if not files:
            return None
        print(f"Found {len(files)} JSON files in {input_dir}")
        # Sort list of JSON files in reverse so the most recent file is first
        files.sort()
        target_file = files[-1]

        # Construct the full path to the JSON file
        input_path = os.path.join(input_dir, target_file)
        print(input_path)

        # Data Ingestion
        # Load the data from the JSON file
        with open(input_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        # Data Flattening
        # Extract the records from the JSON data
        records = raw_data.get("members", [])
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(records)

        # Schema Enforcement (Data Selection)
        # Select the target columns from the DataFrame
        target_columns = ['bioguideId', 'name', 'partyName', 'state']
        # Copy the selected columns to a new DataFrame
        silver_df = df[target_columns].copy()

        # print(silver_df.head(7))

        # Business Logic Filtering
        # Filter the DataFrame to include only Republican legislators
        silver_df = silver_df[silver_df["partyName"] == "Democratic"]

        # Convert the "state" column to lowercase
        silver_df['state'] = silver_df['state'].str.lower()

        # Type Casting
        # Convert the "id" column to an integer
        # silver_df['bioguideId'] = silver_df['bioguideId'].astype(int)

        full_dir_path = os.path.join(SILVER_PATH, partition_date)
        os.makedirs(full_dir_path, exist_ok=True)

        file_name = "legislators_refined.parquet"
        full_file_path = os.path.join(full_dir_path, file_name)

        silver_df.to_parquet(full_file_path, index=False)
        return silver_df

    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    execution_date = "2026-03-09"
    transform_to_silver(execution_date)
