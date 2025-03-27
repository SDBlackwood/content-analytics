from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """Application settings for the Content Analytics Pipeline"""

    # Kafka settings
    kafka_bootstrap_servers: str = Field(
        "localhost:29092", description="Kafka bootstrap servers"
    )
    kafka_topic: str = Field("media-events", description="Kafka topic for media events")
    kafka_group_id: str = Field(
        "content-analytics-consumer", description="Consumer group ID"
    )
    kafka_auto_offset_reset: str = Field(
        "latest", description="Offset policy i.e read from latest or earliest"
    )

    # S3 settings
    s3_endpoint_url: Optional[str] = Field(
        "http://localhost:9000",
        description="Endpoint URL",
    )
    s3_access_key_id: str = Field("minioadmin", description="S3 access key ID")
    s3_secret_access_key: str = Field("minioadmin", description="S3 secret access key")
    s3_bucket: str = Field("content-analytics", description="S3 bucket name")
    s3_region: str = Field("us-east-1", description="S3 region")

    # Batch processing settings
    batch_size: int = Field(
        10000, description="Number of events to process in each batch"
    )
    batch_poll_timeout_seconds: int = Field(
        30, description="Timeout for polling Kafka in seconds"
    )
    batch_poll_interval_ms: int = Field(
        10000, description="Poll interval in milliseconds"
    )

    # Storage settings
    storage_base_path: str = Field("events", description="Base path for storage")
    storage_compression: str = Field(
        "snappy", description="Compression type (snappy, gzip, brotli)"
    )
    storage_file_prefix: str = Field(
        "media-events", description="File prefix for stored data"
    )

    # Logging settings
    log_level: str = Field(
        "INFO", description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    )
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s", description="Log format"
    )
    log_file: str = Field("logs/content-analytics.log", description="Log file path")

    # Security settings
    encryption_enabled: bool = Field(False, description="Enable encryption")
    encryption_key: Optional[str] = Field(None, description="Encryption key")

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")


# Create a singleton instance that can be imported
settings = Settings()
