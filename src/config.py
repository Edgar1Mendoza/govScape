import logging
from pathlib import Path
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    congress_api_key: SecretStr = Field(alias="CONGRESS_API_KEY")

    BASE_DIR: Path = Path(__file__).resolve().parent.parent

    base_data_path: Path = BASE_DIR / "data"

    @property
    def bronze_path(self) -> Path:
        return self.base_data_path / "bronze" / "legislators_comms"

    @property
    def silver_path(self) -> Path:
        return self.base_data_path / "silver" / "legislators_comms"

    @property
    def gold_path(self) -> Path:
        return self.base_data_path / "gold" / "metrics"

    critical_min_records: int = Field(alias="CRITICAL_MIN_RECORDS", default=5)
    expected_min_states: int = Field(alias="EXPECTED_MIN_STATES", default=5)
    mandatory_columns: list[str] = ['bioguideId', 'state']
    optional_columns: list[str] = ['name', 'partyName']

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
LOG_FILE = "govscape_pipeline.log"

logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

config = Settings()
