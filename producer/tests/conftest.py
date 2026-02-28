import pytest
import os
import sys

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_api_response():
    """Sample response from CoinGecko API."""
    return {
        "bitcoin": {
            "usd": 68433.0,
            "eur": 63000.0,
            "usd_market_cap": 1350000000000,
            "usd_24h_vol": 25000000000,
            "usd_24h_change": 4.56,
            "last_updated_at": 1708945200
        },
        "ethereum": {
            "usd": 2073.54,
            "eur": 1900.0,
            "usd_market_cap": 250000000000,
            "usd_24h_vol": 12000000000,
            "usd_24h_change": 8.59,
            "last_updated_at": 1708945200
        }
    }


@pytest.fixture
def sample_transformed_message():
    """Sample transformed message."""
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
def mock_env_vars(monkeypatch):
    """Sets up environment variables for testing."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "test_crypto_prices")
    monkeypatch.setenv("API_INTERVAL_SECONDS", "10")