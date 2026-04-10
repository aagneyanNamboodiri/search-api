from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Elasticsearch (running in Docker)
    es_host: str = "localhost"
    es_port: int = 9200

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
