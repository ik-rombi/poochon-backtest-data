from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "poochon-backtest-data"
    aws_region: str = "us-east-1"
    log_level: str = "INFO"

    data_bucket: str | None = None
    coverage_table_name: str | None = None
    shard_table_name: str | None = None

    pmxt_base_url: str = "https://r2v2.pmxt.dev"
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    vatic_base_url: str = "https://api.vatic.trading"
    binance_base_url: str = "https://api.binance.com"
    binance_us_base_url: str = "https://api.binance.us"

    pm_mirror_state_machine_arn: str | None = None
    pm_slice_state_machine_arn: str | None = None
    hl_mirror_state_machine_arn: str | None = None
    hl_slice_state_machine_arn: str | None = None

    request_payer: str = "requester"

    model_config = SettingsConfigDict(
        env_prefix="POOCHON_",
        case_sensitive=False,
        extra="ignore",
    )


def get_settings() -> Settings:
    return Settings()
