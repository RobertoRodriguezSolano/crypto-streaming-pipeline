import pytest
from unittest.mock import Mock, patch, MagicMock
from psycopg2 import OperationalError
from kafka.errors import NoBrokersAvailable

# Import after conftest sets up path
from processor import Config, PostgresClient, CryptoKafkaConsumer, StreamProcessor


class TestConfig:
    """Tests for Config class."""

    def test_config_has_kafka_topic(self):
        """Test KAFKA_TOPIC is configured."""
        assert Config.KAFKA_TOPIC is not None
        assert len(Config.KAFKA_TOPIC) > 0

    def test_config_has_valid_batch_size(self):
        """Test BATCH_SIZE is positive."""
        assert Config.BATCH_SIZE > 0

    def test_config_has_valid_timeout(self):
        """Test BATCH_TIMEOUT_SECONDS is positive."""
        assert Config.BATCH_TIMEOUT_SECONDS > 0

    def test_config_pg_connection_string(self, mock_env_vars):
        """Test PostgreSQL connection string generation."""
        conn_string = Config.get_pg_connection_string()

        assert "postgresql://" in conn_string
        assert Config.PG_USER in conn_string
        assert Config.PG_HOST in conn_string

    def test_config_has_kafka_group_id(self):
        """Test KAFKA_GROUP_ID is configured."""
        assert Config.KAFKA_GROUP_ID is not None
        assert len(Config.KAFKA_GROUP_ID) > 0


class TestPostgresClient:
    """Tests for PostgreSQL client."""

    @patch('processor.psycopg2.connect')
    def test_connection_success(self, mock_connect):
        """Test successful database connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        client = PostgresClient()

        assert client.connection is not None
        mock_connect.assert_called_once()

    @patch('processor.time.sleep')
    @patch('processor.psycopg2.connect')
    def test_connection_retries_on_operational_error(self, mock_connect, mock_sleep):
        """Test that _connect retries when Postgres is not ready yet.

        In Docker, the processor can start before Postgres finishes initializing.
        Without retries, it would fail immediately and never recover.
        """
        mock_success = MagicMock()
        mock_connect.side_effect = [
            OperationalError("connection refused"),
            OperationalError("connection refused"),
            mock_success
        ]

        client = PostgresClient()

        assert client.connection is mock_success
        assert mock_connect.call_count == 3
        assert mock_sleep.call_count == 2

    @patch('processor.time.sleep')
    @patch('processor.psycopg2.connect')
    def test_connection_raises_after_max_retries(self, mock_connect, mock_sleep):
        """Test that ConnectionError is raised when all retries are exhausted.

        Without this, the process would silently loop forever instead of
        failing with a clear error message.
        """
        mock_connect.side_effect = OperationalError("always failing")

        with pytest.raises(ConnectionError):
            PostgresClient()

        assert mock_connect.call_count == Config.MAX_RETRIES

    @patch('processor.psycopg2.connect')
    @patch('processor.execute_values')
    def test_insert_batch_success(self, mock_execute_values, mock_connect, sample_batch):
        """Test successful batch insert returns row count and commits."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        result = client.insert_batch(sample_batch)

        assert result == 2
        mock_conn.commit.assert_called_once()
        mock_execute_values.assert_called_once()

    @patch('processor.psycopg2.connect')
    def test_insert_batch_empty(self, mock_connect):
        """Test insert with empty batch returns 0 without touching the DB."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        result = client.insert_batch([])

        assert result == 0
        mock_conn.commit.assert_not_called()

    @patch('processor.psycopg2.connect')
    @patch('processor.execute_values')
    def test_insert_batch_error_rollback(self, mock_execute_values, mock_connect, sample_batch):
        """Test that a failed insert triggers rollback and not commit."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn
        mock_execute_values.side_effect = Exception("Insert failed")

        client = PostgresClient()

        with pytest.raises(Exception):
            client.insert_batch(sample_batch)

        mock_conn.rollback.assert_called()
        mock_conn.commit.assert_not_called()

    @patch('processor.psycopg2.connect')
    def test_is_connected_true(self, mock_connect):
        """Test is_connected executes a real query to verify liveness."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        result = client.is_connected()

        assert result is True
        mock_cursor.execute.assert_called_with("SELECT 1")

    @patch('processor.psycopg2.connect')
    def test_is_connected_false(self, mock_connect):
        """Test is_connected returns False when connection is broken."""
        mock_conn = MagicMock()
        mock_conn.cursor.side_effect = Exception("Connection lost")
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        result = client.is_connected()

        assert result is False

    @patch('processor.psycopg2.connect')
    def test_reconnect_closes_then_connects(self, mock_connect):
        """Test reconnect calls close() first and then _connect() again.

        Order matters: closing the broken connection before opening
        a new one avoids leaking connections.
        """
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        client.reconnect()

        # connect called twice: once on __init__, once on reconnect
        assert mock_connect.call_count == 2
        mock_conn.close.assert_called_once()

    @patch('processor.psycopg2.connect')
    @patch('processor.execute_values')
    def test_insert_batch_data_transformation(self, mock_execute_values, mock_connect, sample_batch):
        """Test that records are correctly mapped to DB columns in the right order."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__ = Mock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = Mock(return_value=False)
        mock_connect.return_value = mock_conn

        client = PostgresClient()
        client.insert_batch(sample_batch)

        call_args = mock_execute_values.call_args
        data = call_args[0][2]

        assert len(data) == 2
        assert data[0][0] == "bitcoin"
        assert data[1][0] == "ethereum"


class TestCryptoKafkaConsumer:
    """Tests for Kafka consumer."""

    @patch('processor.KafkaConsumer')
    def test_consumer_connection_success(self, mock_consumer):
        """Test successful Kafka consumer connection."""
        mock_instance = MagicMock()
        mock_consumer.return_value = mock_instance

        consumer = CryptoKafkaConsumer()

        assert consumer.consumer is not None
        mock_consumer.assert_called_once()

    @patch('processor.time.sleep')
    @patch('processor.KafkaConsumer')
    def test_consumer_retries_on_no_brokers_available(self, mock_consumer, mock_sleep):
        """Test that _connect retries when Kafka is not ready yet.

        Same as Postgres: in Docker the consumer can start before Kafka.
        """
        mock_success = MagicMock()
        mock_consumer.side_effect = [
            NoBrokersAvailable(),
            NoBrokersAvailable(),
            mock_success
        ]

        consumer = CryptoKafkaConsumer()

        assert consumer.consumer is mock_success
        assert mock_consumer.call_count == 3
        assert mock_sleep.call_count == 2

    @patch('processor.time.sleep')
    @patch('processor.KafkaConsumer')
    def test_consumer_raises_after_max_retries(self, mock_consumer, mock_sleep):
        """Test that ConnectionError is raised when all retries are exhausted."""
        mock_consumer.side_effect = NoBrokersAvailable()

        with pytest.raises(ConnectionError):
            CryptoKafkaConsumer()

        assert mock_consumer.call_count == Config.MAX_RETRIES

    @patch('processor.KafkaConsumer')
    def test_consumer_auto_commit_is_disabled(self, mock_consumer):
        """Test that auto commit is disabled — required for manual commit after INSERT.

        With enable_auto_commit=True, Kafka could commit offsets before the DB
        INSERT succeeds, causing silent data loss on crashes.
        """
        mock_instance = MagicMock()
        mock_consumer.return_value = mock_instance

        CryptoKafkaConsumer()

        call_kwargs = mock_consumer.call_args[1]
        assert call_kwargs['enable_auto_commit'] is False

    @patch('processor.KafkaConsumer')
    def test_consumer_configured_with_topic(self, mock_consumer):
        """Test consumer subscribes to the configured topic."""
        mock_instance = MagicMock()
        mock_consumer.return_value = mock_instance

        CryptoKafkaConsumer()

        call_args = mock_consumer.call_args
        assert Config.KAFKA_TOPIC in call_args[0]


class TestStreamProcessor:
    """Tests for Stream Processor."""

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_processor_initialization(self, mock_pg, mock_kafka):
        """Test processor initializes with empty batch and zero counters."""
        processor = StreamProcessor()

        assert processor.batch == []
        assert processor.batch_count == 0
        assert processor.total_records == 0

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_flush_batch_success(self, mock_pg, mock_kafka, sample_batch):
        """Test successful batch flush updates counters and clears batch."""
        mock_pg_instance = MagicMock()
        mock_pg_instance.insert_batch.return_value = 2
        mock_pg.return_value = mock_pg_instance

        processor = StreamProcessor()
        processor.batch = sample_batch.copy()
        processor._flush_batch()

        assert processor.batch == []
        assert processor.batch_count == 1
        assert processor.total_records == 2

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_flush_batch_commits_kafka_after_successful_insert(self, mock_pg, mock_kafka, sample_batch):
        """Test that Kafka offset is committed only after a successful PostgreSQL INSERT.

        This is the core guarantee of at-least-once delivery:
        - INSERT succeeds → commit Kafka offset → message is done
        - INSERT fails   → do NOT commit → Kafka re-delivers on restart
        """
        mock_pg_instance = MagicMock()
        mock_pg_instance.insert_batch.return_value = 2
        mock_pg.return_value = mock_pg_instance

        mock_kafka_instance = MagicMock()
        mock_kafka.return_value = mock_kafka_instance

        processor = StreamProcessor()
        processor.batch = sample_batch.copy()
        processor._flush_batch()

        assert mock_pg_instance.insert_batch.call_count == 1
        assert mock_kafka_instance.commit.call_count == 1

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_flush_batch_does_not_commit_kafka_on_db_failure(self, mock_pg, mock_kafka, sample_batch):
        """Test that Kafka offset is NOT committed when the INSERT fails.

        If we committed the offset before confirming the INSERT, a crash
        would cause those messages to be permanently lost.
        """
        mock_pg_instance = MagicMock()
        mock_pg_instance.insert_batch.side_effect = Exception("DB is down")
        mock_pg_instance.is_connected.return_value = True
        mock_pg.return_value = mock_pg_instance

        mock_kafka_instance = MagicMock()
        mock_kafka.return_value = mock_kafka_instance

        processor = StreamProcessor()
        processor.batch = sample_batch.copy()
        processor._flush_batch()

        mock_kafka_instance.commit.assert_not_called()

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_flush_batch_always_clears_batch(self, mock_pg, mock_kafka, sample_batch):
        """Test batch is always cleared even when the INSERT fails.

        Without this the processor would accumulate records indefinitely
        and cause memory issues.
        """
        mock_pg_instance = MagicMock()
        mock_pg_instance.insert_batch.side_effect = Exception("DB Error")
        mock_pg_instance.is_connected.return_value = True
        mock_pg.return_value = mock_pg_instance

        processor = StreamProcessor()
        processor.batch = sample_batch.copy()
        processor._flush_batch()

        assert processor.batch == []

    @patch('processor.CryptoKafkaConsumer')
    @patch('processor.PostgresClient')
    def test_reconnects_to_postgres_after_failed_insert(self, mock_pg, mock_kafka, sample_batch):
        """Test processor reconnects to Postgres when connection is lost after a failed insert."""
        mock_pg_instance = MagicMock()
        mock_pg_instance.insert_batch.side_effect = Exception("Connection lost")
        mock_pg_instance.is_connected.return_value = False
        mock_pg.return_value = mock_pg_instance

        processor = StreamProcessor()
        processor.batch = sample_batch.copy()
        processor._flush_batch()

        mock_pg_instance.reconnect.assert_called_once()