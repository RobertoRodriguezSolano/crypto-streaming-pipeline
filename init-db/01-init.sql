-- Create table for cryptocurrency prices
CREATE TABLE IF NOT EXISTS crypto_prices (
    id SERIAL PRIMARY KEY,
    crypto_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    price_usd DECIMAL(20, 8),
    price_eur DECIMAL(20, 8),
    market_cap_usd DECIMAL(30, 2),
    volume_24h_usd DECIMAL(30, 2),
    change_24h_percent DECIMAL(10, 4),
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for better Grafana query performance
CREATE INDEX idx_crypto_prices_crypto_id ON crypto_prices(crypto_id);
CREATE INDEX idx_crypto_prices_timestamp ON crypto_prices(timestamp DESC);
CREATE INDEX idx_crypto_prices_crypto_timestamp ON crypto_prices(crypto_id, timestamp DESC);

-- View for latest price per crypto
CREATE OR REPLACE VIEW latest_prices AS
SELECT DISTINCT ON (crypto_id)
    crypto_id,
    timestamp,
    price_usd,
    price_eur,
    market_cap_usd,
    volume_24h_usd,
    change_24h_percent
FROM crypto_prices
ORDER BY crypto_id, timestamp DESC;