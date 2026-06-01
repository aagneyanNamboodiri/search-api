from functools import lru_cache
from typing import Literal

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application settings loaded from environment / .env file."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Application
    app_name: str = "Search API"
    environment: Literal["development", "staging", "production"] = "development"
    debug: bool = True

    # Elasticsearch connection
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_cloud_id: str | None = None
    elasticsearch_api_key: str | None = None
    elasticsearch_username: str | None = None
    elasticsearch_password: str | None = None
    elasticsearch_index: str = "documents"

    # Client behavior
    elasticsearch_request_timeout: int = 10
    elasticsearch_verify_certs: bool = True

    @model_validator(mode="after")
    def _validate_auth(self) -> "Settings":
        has_basic_auth = bool(self.elasticsearch_username) and bool(
            self.elasticsearch_password
        )
        has_partial_basic = bool(self.elasticsearch_username) != bool(
            self.elasticsearch_password
        )
        if has_partial_basic:
            raise ValueError(
                "ELASTICSEARCH_USERNAME and ELASTICSEARCH_PASSWORD must be set together."
            )
        # API key and basic auth are mutually exclusive.
        if self.elasticsearch_api_key and has_basic_auth:
            raise ValueError(
                "Use either ELASTICSEARCH_API_KEY or username/password, not both."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
