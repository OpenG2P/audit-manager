"""
Configuration management.

Loads settings from YAML config file with environment variable overrides.
Mirrors the id-generator pattern — env vars use `__` as nested delimiter,
e.g. AUDIT_MANAGER__KAFKA__BOOTSTRAP_SERVERS.
"""

import os
from functools import lru_cache
from pathlib import Path

import yaml
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class IngestConfig(BaseModel):
    queue_max_size: int = Field(default=10000, ge=1)
    max_batch_size: int = Field(default=500, ge=1)


class KafkaProducerConfig(BaseModel):
    acks: str = "1"
    linger_ms: int = 50
    compression_type: str = "zstd"
    max_batch_size: int = 524288


class KafkaConsumerConfig(BaseModel):
    batch_max_records: int = 500
    flush_interval_ms: int = 1000
    auto_offset_reset: str = "earliest"
    enable_auto_commit: bool = False


class KafkaConfig(BaseModel):
    bootstrap_servers: str = "kafka:9092"
    topic: str = "openg2p.audit.events"
    dlq_topic: str = "openg2p.audit.dlq"
    client_id: str = "openg2p-audit-manager"
    consumer_group: str = "openg2p-audit-consumer"
    producer: KafkaProducerConfig = KafkaProducerConfig()
    consumer: KafkaConsumerConfig = KafkaConsumerConfig()


class DatabaseConfig(BaseModel):
    partition_pre_create_months: int = Field(default=3, ge=1)
    partition_retention_months: int = Field(default=84, ge=0)
    partition_check_interval_seconds: int = Field(default=3600, ge=60)


class AuditManagerConfig(BaseModel):
    service_id: str = "openg2p.auditmanager"
    api_version: str = "1.0"
    ingest: IngestConfig = IngestConfig()
    kafka: KafkaConfig = KafkaConfig()
    database: DatabaseConfig = DatabaseConfig()


class Settings(BaseSettings):
    audit_manager: AuditManagerConfig = AuditManagerConfig()

    model_config = {"env_nested_delimiter": "__"}


def _find_config_path() -> Path:
    config_path = os.environ.get("CONFIG_PATH", None)
    if config_path:
        return Path(config_path)

    cwd_config = Path.cwd() / "config" / "default.yaml"
    if cwd_config.exists():
        return cwd_config

    src_config = Path(__file__).parent.parent.parent / "config" / "default.yaml"
    if src_config.exists():
        return src_config

    raise FileNotFoundError(
        "Config file not found. Set CONFIG_PATH env var or place "
        "config/default.yaml in the working directory."
    )


@lru_cache
def get_settings() -> Settings:
    config_path = _find_config_path()
    with open(config_path) as f:
        yaml_data = yaml.safe_load(f) or {}
    return Settings(**yaml_data)
