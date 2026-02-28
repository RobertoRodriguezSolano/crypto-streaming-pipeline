import json
import time
import os
import sys
import logging
from datetime import datetime, timezone
from kafka import KafkaProducer
from kafka.errors import KafkaError
import requests

# ===========================================
# LOGGING CONFIGURATION
# ===========================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# ===========================================
# CONFIGURATION FROM ENVIRONMENT VARIABLES
# ===========================================
class Config:
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC')
    API_INTERVAL_SECONDS = int(os.getenv('API_INTERVAL_SECONDS'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES'))
    RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL'))
    
    # Cryptocurrencies to monitor
    CRYPTO_IDS = [
    'bitcoin', 'ethereum', 'cardano', 'solana', 'ripple',
    'dogecoin', 'polkadot', 'avalanche-2', 'chainlink', 'polygon-ecosystem-token'
    ]
    
    # CoinGecko API
    COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
    
    @classmethod
    def validate(cls):
        """Validates required configuration."""
        required = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC']
        for field in required:
            if not getattr(cls, field):
                raise ValueError(f"Missing required config: {field}")
        logger.info("Configuration validated successfully")


# ===========================================
# KAFKA PRODUCER
# ===========================================
class CryptoKafkaProducer:
    def __init__(self):
        self.producer = None
        self._connect()
    
    def _connect(self):
        """Creates Kafka producer with retry logic."""
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks='all',
                    retries=3,
                    max_block_ms=30000
                )
                logger.info(f"✅ Connected to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}")
                return
            except KafkaError as e:
                logger.warning(f"⏳ Attempt {attempt + 1}/{Config.MAX_RETRIES} - Waiting for Kafka: {e}")
                time.sleep(Config.RETRY_INTERVAL)
        
        raise ConnectionError("❌ Could not connect to Kafka after multiple attempts")
    
    def send(self, key: str, value: dict) -> bool:
        """Sends a message to Kafka."""
        try:
            future = self.producer.send(Config.KAFKA_TOPIC, key=key, value=value)
            future.get(timeout=10)
            return True
        except KafkaError as e:
            logger.error(f"❌ Error sending message: {e}")
            return False
    
    def flush(self):
        """Flushes pending messages."""
        if self.producer:
            self.producer.flush()
    
    def close(self):
        """Closes the producer."""
        if self.producer:
            self.producer.close()
            logger.info("Kafka producer closed")


# ===========================================
# COINGECKO API CLIENT
# ===========================================
class CoinGeckoClient:
    @staticmethod
    def fetch_prices() -> dict | None:
        """Fetches cryptocurrency prices from CoinGecko API."""
        params = {
            'ids': ','.join(Config.CRYPTO_IDS),
            'vs_currencies': 'usd,eur',
            'include_market_cap': 'true',
            'include_24hr_vol': 'true',
            'include_24hr_change': 'true',
            'include_last_updated_at': 'true'
        }
        
        try:
            response = requests.get(
                Config.COINGECKO_URL,
                params=params,
                timeout=10
            )
            response.raise_for_status()
            logger.debug(f"API response: {response.status_code}")
            return response.json()
        except requests.exceptions.Timeout:
            logger.error("❌ API request timed out")
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ API HTTP error: {e}")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ API request error: {e}")
        return None


# ===========================================
# DATA TRANSFORMER
# ===========================================
class DataTransformer:
    @staticmethod
    def transform(raw_data: dict) -> list[dict]:
        """Transforms raw API data into structured messages."""
        messages = []
        timestamp = datetime.now(timezone.utc).isoformat()
        
        for crypto_id, data in raw_data.items():
            message = {
                'crypto_id': crypto_id,
                'timestamp': timestamp,
                'price_usd': data.get('usd', 0),
                'price_eur': data.get('eur', 0),
                'market_cap_usd': data.get('usd_market_cap', 0),
                'volume_24h_usd': data.get('usd_24h_vol', 0),
                'change_24h_percent': data.get('usd_24h_change', 0),
                'last_updated': data.get('last_updated_at', 0)
            }
            messages.append(message)
        
        return messages


# ===========================================
# MAIN APPLICATION
# ===========================================
def print_banner():
    """Prints application banner."""
    banner = """
╔════════════════════════════════════════════════════════════╗
║              🚀 CRYPTO PRICE PRODUCER                      ║
╠════════════════════════════════════════════════════════════╣
║  Kafka Server : {kafka:<40} ║
║  Topic        : {topic:<40} ║
║  Interval     : {interval:<40} ║
║  Cryptos      : {count:<40} ║
╚════════════════════════════════════════════════════════════╝
    """.format(
        kafka=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        interval=f"{Config.API_INTERVAL_SECONDS} seconds",
        count=f"{len(Config.CRYPTO_IDS)} cryptocurrencies"
    )
    print(banner)


def main():
    """Main entry point."""
    print_banner()
    
    # Validate configuration
    Config.validate()
    
    # Initialize producer
    producer = CryptoKafkaProducer()
    
    # Main loop
    iteration = 0
    try:
        while True:
            iteration += 1
            logger.info(f"🔄 Iteration #{iteration}")
            
            # Fetch data from API
            raw_data = CoinGeckoClient.fetch_prices()
            
            if raw_data:
                # Transform data
                messages = DataTransformer.transform(raw_data)
                
                # Send to Kafka
                success_count = 0
                for message in messages:
                    if producer.send(message['crypto_id'], message):
                        success_count += 1
                        logger.info(
                            f"📤 {message['crypto_id']}: "
                            f"${message['price_usd']:.2f} "
                            f"({message['change_24h_percent']:+.2f}%)"
                        )
                
                producer.flush()
                logger.info(f"✅ Sent {success_count}/{len(messages)} messages")
            else:
                logger.warning("⚠️ No data retrieved from API")
            
            # Wait for next iteration
            logger.info(f"😴 Sleeping {Config.API_INTERVAL_SECONDS}s...")
            time.sleep(Config.API_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down gracefully...")
    finally:
        producer.close()


if __name__ == "__main__":
    main()