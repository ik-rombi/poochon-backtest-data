from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "poochon-backtest-data"
    aws_region: str = "eu-west-1"
    log_level: str = "INFO"
    port: int = 8080

    data_bucket: str | None = None
    coverage_table_name: str | None = None
    replay_table_name: str | None = None
    replay_state_machine_arn: str | None = None

    request_payer: str = Field(default="requester")

    model_config = SettingsConfigDict(
        env_prefix="POOCHON_",
        case_sensitive=False,
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()
