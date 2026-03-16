import os

# Paths
BASE_DATA_PATH = "data"
BRONZE_PATH = os.path.join(BASE_DATA_PATH, "bronze/legislators_comms")
SILVER_PATH = os.path.join(BASE_DATA_PATH, "silver/legislators_comms")
GOLD_PATH = os.path.join(BASE_DATA_PATH, "gold/metrics")

# Logging
LOG_FILE = "govscape_pipeline.log"
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
