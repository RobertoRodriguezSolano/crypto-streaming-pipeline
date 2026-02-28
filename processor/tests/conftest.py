import pytest
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_kafka_message():
    """Sample message from Kafka."""
    return {
        "crypto_id": "bitcoin",
        "timestamp": "2026-02-26T10:00:00+00:00",
        "price_usd": 68433.0,
        "price_eur": 63000.0,
        "market_cap_usd": 1350000000000,
        "volume_24h_usd": 25000000000,
        "change_24h_percent": 4.56,
        "last_updated": 1708945200
    }


@pytest.fixture
def sample_batch():
    """Sample batch of messages."""
    return [
        {
            "crypto_id": "bitcoin",
            "timestamp": "2026-02-26T10:00:00+00:00",
            "price_usd": 68433.0,
            "price_eur": 63000.0,
            "market_cap_usd": 1350000000000,
            "volume_24h_usd": 25000000000,
            "change_24h_percent": 4.56,
            "last_updated": 1708945200
        },
        {
            "crypto_id": "ethereum",
            "timestamp": "2026-02-26T10:00:00+00:00",
            "price_usd": 2073.54,
            "price_eur": 1900.0,
            "market_cap_usd": 250000000000,
            "volume_24h_usd": 12000000000,
            "change_24h_percent": 8.59,
            "last_updated": 1708945200
        }
    ]


@pytest.fixture
def mock_env_vars(monkeypatch):
    """Sets up environment variables for testing."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "test_crypto_prices")
    monkeypatch.setenv("KAFKA_GROUP_ID", "test_group")
    monkeypatch.setenv("PG_HOST", "localhost")
    monkeypatch.setenv("PG_PORT", "5432")
    monkeypatch.setenv("PG_DATABASE", "test_db")
    monkeypatch.setenv("PG_USER", "test_user")
    monkeypatch.setenv("PG_PASSWORD", "test_pass")
    monkeypatch.setenv("BATCH_SIZE", "10")
    monkeypatch.setenv("BATCH_TIMEOUT_SECONDS", "5")