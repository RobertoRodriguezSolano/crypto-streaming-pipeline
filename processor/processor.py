import os
import json
import time
import sys
import logging
from datetime import datetime
from typing import List, Dict, Any
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
import psycopg2
from psycopg2.extras import execute_values
from psycopg2 import OperationalError

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
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'crypto_prices')
    KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'crypto_processor_group')
    
    # PostgreSQL
    PG_HOST = os.getenv('PG_HOST', 'postgres')
    PG_PORT = os.getenv('PG_PORT', '5432')
    PG_DATABASE = os.getenv('PG_DATABASE', 'crypto_db')
    PG_USER = os.getenv('PG_USER', 'crypto_user')
    PG_PASSWORD = os.getenv('PG_PASSWORD', 'crypto_pass')
    
    # Processing
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '10'))
    BATCH_TIMEOUT_SECONDS = int(os.getenv('BATCH_TIMEOUT_SECONDS', '5'))
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', '30'))
    RETRY_INTERVAL = int(os.getenv('RETRY_INTERVAL', '2'))
    
    @classmethod
    def get_pg_connection_string(cls) -> str:
        return f"postgresql://{cls.PG_USER}:{cls.PG_PASSWORD}@{cls.PG_HOST}:{cls.PG_PORT}/{cls.PG_DATABASE}"
    
    @classmethod
    def validate(cls):
        """Validates required configuration."""
        required = ['KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_TOPIC', 'PG_HOST', 'PG_DATABASE']
        for field in required:
            if not getattr(cls, field):
                raise ValueError(f"Missing required config: {field}")
        logger.info("Configuration validated successfully")


# ===========================================
# DATABASE CLIENT
# ===========================================
class PostgresClient:
    def __init__(self):
        self.connection = None
        self._connect()
    
    def _connect(self):
        """Creates PostgreSQL connection with retry logic."""
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.connection = psycopg2.connect(
                    host=Config.PG_HOST,
                    port=Config.PG_PORT,
                    database=Config.PG_DATABASE,
                    user=Config.PG_USER,
                    password=Config.PG_PASSWORD
                )
                self.connection.autocommit = False
                logger.info(f"✅ Connected to PostgreSQL at {Config.PG_HOST}:{Config.PG_PORT}")
                return
            except OperationalError as e:
                logger.warning(f"⏳ Attempt {attempt + 1}/{Config.MAX_RETRIES} - Waiting for PostgreSQL: {e}")
                time.sleep(Config.RETRY_INTERVAL)
        
        raise ConnectionError("❌ Could not connect to PostgreSQL after multiple attempts")
    
    def insert_batch(self, records: List[Dict[str, Any]]) -> int:
        """Inserts a batch of records into the database."""
        if not records:
            return 0
        
        insert_query = """
            INSERT INTO crypto_prices 
            (crypto_id, timestamp, price_usd, price_eur, market_cap_usd, volume_24h_usd, change_24h_percent)
            VALUES %s
        """
        
        data = [
            (
                r['crypto_id'],
                r['timestamp'],
                r.get('price_usd', 0),
                r.get('price_eur', 0),
                r.get('market_cap_usd', 0),
                r.get('volume_24h_usd', 0),
                r.get('change_24h_percent', 0)
            )
            for r in records
        ]
        
        try:
            with self.connection.cursor() as cursor:
                execute_values(cursor, insert_query, data)
            self.connection.commit()
            return len(data)
        except Exception as e:
            self.connection.rollback()
            logger.error(f"❌ Database insert error: {e}")
            raise
    
    def is_connected(self) -> bool:
        """Checks if connection is alive."""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute("SELECT 1")
            return True
        except Exception:
            return False
    
    def reconnect(self):
        """Reconnects to the database."""
        logger.info("🔄 Reconnecting to PostgreSQL...")
        self.close()
        self._connect()
    
    def close(self):
        """Closes the database connection."""
        if self.connection:
            self.connection.close()
            logger.info("PostgreSQL connection closed")


# ===========================================
# KAFKA CONSUMER
# ===========================================
class CryptoKafkaConsumer:
    def __init__(self):
        self.consumer = None
        self._connect()
    
    def _connect(self):
        """Creates Kafka consumer with retry logic."""
        for attempt in range(Config.MAX_RETRIES):
            try:
                self.consumer = KafkaConsumer(
                    Config.KAFKA_TOPIC,
                    bootstrap_servers=Config.KAFKA_BOOTSTRAP_SERVERS,
                    group_id=Config.KAFKA_GROUP_ID,
                    auto_offset_reset='earliest',
                    enable_auto_commit=False,          # FIX: commit manual tras INSERT exitoso
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    consumer_timeout_ms=Config.BATCH_TIMEOUT_SECONDS * 1000,
                    max_poll_records=Config.BATCH_SIZE
                )
                logger.info(f"✅ Connected to Kafka at {Config.KAFKA_BOOTSTRAP_SERVERS}")
                return
            except NoBrokersAvailable as e:
                logger.warning(f"⏳ Attempt {attempt + 1}/{Config.MAX_RETRIES} - Waiting for Kafka: {e}")
                time.sleep(Config.RETRY_INTERVAL)
        
        raise ConnectionError("❌ Could not connect to Kafka after multiple attempts")
    
    def commit(self):
        """Commits current offset to Kafka manually."""
        self.consumer.commit()

    def __iter__(self):
        return self.consumer.__iter__()
    
    def close(self):
        """Closes the consumer."""
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")


# ===========================================
# STREAM PROCESSOR
# ===========================================
class StreamProcessor:
    def __init__(self):
        self.db_client = PostgresClient()
        self.kafka_consumer = CryptoKafkaConsumer()
        self.batch: List[Dict[str, Any]] = []
        self.batch_count = 0
        self.total_records = 0
    
    def process(self):
        """Main processing loop."""
        logger.info("📖 Starting to consume messages...")
        logger.info("=" * 60)
        
        while True:
            try:
                for message in self.kafka_consumer:
                    record = message.value
                    self.batch.append(record)
                    
                    logger.info(
                        f"📨 Received: {record['crypto_id']} = "
                        f"${record['price_usd']:.2f} "
                        f"({record['change_24h_percent']:+.2f}%)"
                    )
                    
                    # Process batch when full
                    if len(self.batch) >= Config.BATCH_SIZE:
                        self._flush_batch()
                
                # Flush remaining records after timeout
                if self.batch:
                    self._flush_batch()
                
                time.sleep(0.1)
                
            except Exception as e:
                logger.error(f"❌ Processing error: {e}")
                self._handle_error()
    
    def _flush_batch(self):
        """Flushes current batch to database.
        
        Order of operations is critical:
        1. INSERT into PostgreSQL and commit DB transaction
        2. Only if INSERT succeeded, commit offset to Kafka
        
        This guarantees that if the process dies between INSERT and Kafka commit,
        on restart Kafka will re-deliver the same messages and they will be
        inserted again (at-least-once delivery). Without this, a crash after
        the Kafka auto-commit but before the INSERT would silently lose data.
        """
        if not self.batch:
            return
        
        try:
            # Step 1: insert into PostgreSQL
            inserted = self.db_client.insert_batch(self.batch)

            # Step 2: only commit Kafka offset after successful INSERT
            self.kafka_consumer.commit()

            self.batch_count += 1
            self.total_records += inserted
            logger.info(
                f"📥 Batch {self.batch_count}: Inserted {inserted} records "
                f"(Total: {self.total_records})"
            )
        except Exception as e:
            logger.error(f"❌ Batch insert failed: {e}")
            if not self.db_client.is_connected():
                self.db_client.reconnect()
        finally:
            self.batch = []
    
    def _handle_error(self):
        """Handles errors by attempting to reconnect."""
        try:
            if not self.db_client.is_connected():
                self.db_client.reconnect()
        except Exception as e:
            logger.error(f"❌ Reconnection failed: {e}")
            time.sleep(5)
    
    def close(self):
        """Closes all connections."""
        self.kafka_consumer.close()
        self.db_client.close()


# ===========================================
# MAIN APPLICATION
# ===========================================
def print_banner():
    """Prints application banner."""
    banner = """
╔════════════════════════════════════════════════════════════╗
║              🚀 CRYPTO STREAM PROCESSOR                    ║
╠════════════════════════════════════════════════════════════╣
║  Kafka Server  : {kafka:<39} ║
║  Topic         : {topic:<39} ║
║  Consumer Group: {group:<39} ║
║  PostgreSQL    : {pg:<39} ║
║  Batch Size    : {batch:<39} ║
╚════════════════════════════════════════════════════════════╝
    """.format(
        kafka=Config.KAFKA_BOOTSTRAP_SERVERS,
        topic=Config.KAFKA_TOPIC,
        group=Config.KAFKA_GROUP_ID,
        pg=f"{Config.PG_HOST}:{Config.PG_PORT}/{Config.PG_DATABASE}",
        batch=str(Config.BATCH_SIZE)
    )
    print(banner)


def main():
    """Main entry point."""
    print_banner()
    
    # Validate configuration
    Config.validate()
    
    # Initialize and run processor
    processor = StreamProcessor()
    
    try:
        processor.process()
    except KeyboardInterrupt:
        logger.info("🛑 Shutting down gracefully...")
    finally:
        processor.close()


if __name__ == "__main__":
    main()