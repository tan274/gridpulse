from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env")

    database_url: str
    test_database_url: str = "postgresql://postgres:postgres@localhost:5432/gridpulse_test"
    eia_api_key: str
    aws_region: str = "us-east-1"
    s3_bucket_name: str
    aws_access_key_id: str = ""
    aws_secret_access_key: str = ""


settings = Settings()
