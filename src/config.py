import os
from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    congress_api_key: SecretStr = Field(alias="CONGRESS_API_KEY")

    base_data_path: str = "data"

    @property
    def bronze_path(self):
        return os.path.join(self.base_data_path, "bronze/legislators_comms")

    @property
    def silver_path(self):
        return os.path.join(self.base_data_path, "silver/legislators_comms")

    @property
    def gold_path(self):
        return os.path.join(self.base_data_path, "gold/metrics")

    critical_min_records: int = Field(alias="CRITICAL_MIN_RECORDS", default=5)
    expected_min_states: int = Field(alias="EXPECTED_MIN_STATES", default=5)
    mandatory_columns: list[str] = ['bioguideId', 'state']
    optional_columns: list[str] = ['name', 'partyName']

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore"
    )


config = Settings()
