from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8')

    s3_endpoint_url: str
    s3_access_key_id: str
    s3_secret_access_key: str
    s3_bucket_raw: str = 'raw'
    s3_bucket_staging: str = 'staging'
    s3_bucket_rejected: str = 'rejected'
    mlflow_tracking_uri: str


settings = Settings()
