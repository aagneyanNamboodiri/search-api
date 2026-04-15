from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Elasticsearch (Docker)
    es_url: str = "http://localhost:9200"
    es_username: str | None = None
    es_password: str | None = None
    es_verify_certs: bool = True

    # PostgreSQL (local, tenant-based)
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "infiviz"
    pg_user: str = "postgres"
    pg_password: str = "postgres"

    class Config:
        env_prefix = "SEARCH_API_"
        env_file = ".env"


settings = Settings()
