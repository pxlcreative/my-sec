from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    database_url: str
    elasticsearch_url: str = "http://elasticsearch:9200"
    redis_url: str = "redis://redis:6379/0"
    secret_key: str = "change-me"
    data_dir: str = "/data"
    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
