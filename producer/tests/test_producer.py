import pytest
import responses
import requests
from unittest.mock import Mock, patch, MagicMock, call
import json
from kafka.errors import KafkaError

# Import after conftest sets up path
from producer import Config, CoinGeckoClient, DataTransformer, CryptoKafkaProducer


class TestConfig:
    """Tests for Config class."""

    def test_config_has_kafka_topic(self):
        """Test KAFKA_TOPIC is configured."""
        assert Config.KAFKA_TOPIC is not None
        assert len(Config.KAFKA_TOPIC) > 0

    def test_config_has_valid_interval(self):
        """Test API_INTERVAL_SECONDS is positive."""
        assert Config.API_INTERVAL_SECONDS > 0

    def test_config_crypto_ids_contains_major_coins(self):
        """Test that major cryptocurrencies are included."""
        assert "bitcoin" in Config.CRYPTO_IDS
        assert "ethereum" in Config.CRYPTO_IDS
        assert "cardano" in Config.CRYPTO_IDS

    def test_config_validate_success(self, mock_env_vars):
        """Test configuration validation passes with valid config.
        
        NOTA: mock_env_vars hace monkeypatch de las variables de entorno,
        pero Config las lee al importar el módulo (nivel de clase), no al
        llamar a validate(). Por eso este test pasa gracias a los valores
        por defecto hardcodeados en Config, no por el monkeypatch.
        validate() solo comprueba que los atributos no estén vacíos.
        """
        Config.validate()

    def test_coingecko_url_is_valid(self):
        """Test CoinGecko URL is properly formatted."""
        assert Config.COINGECKO_URL.startswith("https://")
        assert "coingecko.com" in Config.COINGECKO_URL

    def test_config_max_retries_is_positive(self):
        """Test MAX_RETRIES is a positive integer."""
        assert Config.MAX_RETRIES > 0
        assert isinstance(Config.MAX_RETRIES, int)

    def test_config_retry_interval_is_positive(self):
        """Test RETRY_INTERVAL is a positive integer."""
        assert Config.RETRY_INTERVAL > 0
        assert isinstance(Config.RETRY_INTERVAL, int)


class TestCoinGeckoClient:
    """Tests for CoinGecko API client."""

    @responses.activate
    def test_fetch_prices_success(self, sample_api_response):
        """Test successful API fetch."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json=sample_api_response,
            status=200
        )

        result = CoinGeckoClient.fetch_prices()

        assert result is not None
        assert "bitcoin" in result
        assert "ethereum" in result
        assert result["bitcoin"]["usd"] == 68433.0

    @responses.activate
    def test_fetch_prices_api_error(self):
        """Test rate limit (429) returns None."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json={"error": "rate limit"},
            status=429
        )

        result = CoinGeckoClient.fetch_prices()

        assert result is None

    @responses.activate
    def test_fetch_prices_server_error(self):
        """Test 500 error handling."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json={"error": "internal server error"},
            status=500
        )

        result = CoinGeckoClient.fetch_prices()

        assert result is None

    @patch('producer.requests.get')
    def test_fetch_prices_timeout(self, mock_get):
        """Test timeout handling."""
        mock_get.side_effect = requests.exceptions.Timeout("Connection timed out")

        result = CoinGeckoClient.fetch_prices()

        assert result is None

    @patch('producer.requests.get')
    def test_fetch_prices_connection_error(self, mock_get):
        """Test connection error handling."""
        mock_get.side_effect = requests.exceptions.ConnectionError("Network unreachable")

        result = CoinGeckoClient.fetch_prices()

        assert result is None

    @responses.activate
    def test_fetch_prices_sends_correct_crypto_ids(self):
        """Test that the request includes all configured crypto IDs."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json={},
            status=200
        )

        CoinGeckoClient.fetch_prices()

        # Verify the request contained all crypto IDs
        request_params = responses.calls[0].request.url
        for crypto_id in Config.CRYPTO_IDS:
            assert crypto_id in request_params, f"Missing crypto_id in request: {crypto_id}"

    @responses.activate
    def test_fetch_prices_requests_usd_and_eur(self):
        """Test that the request asks for both USD and EUR."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json={},
            status=200
        )

        CoinGeckoClient.fetch_prices()

        request_url = responses.calls[0].request.url
        assert "usd" in request_url
        assert "eur" in request_url


class TestDataTransformer:
    """Tests for DataTransformer class."""

    def test_transform_single_crypto(self, sample_api_response):
        """Test transformation of single cryptocurrency."""
        single_crypto = {"bitcoin": sample_api_response["bitcoin"]}

        result = DataTransformer.transform(single_crypto)

        assert len(result) == 1
        assert result[0]["crypto_id"] == "bitcoin"
        assert result[0]["price_usd"] == 68433.0
        assert result[0]["price_eur"] == 63000.0
        assert result[0]["change_24h_percent"] == 4.56

    def test_transform_multiple_cryptos(self, sample_api_response):
        """Test transformation of multiple cryptocurrencies."""
        result = DataTransformer.transform(sample_api_response)

        assert len(result) == 2
        crypto_ids = [r["crypto_id"] for r in result]
        assert "bitcoin" in crypto_ids
        assert "ethereum" in crypto_ids

    def test_transform_timestamp_is_iso_format(self, sample_api_response):
        """Test that timestamp is in ISO 8601 UTC format."""
        result = DataTransformer.transform(sample_api_response)

        timestamp = result[0]["timestamp"]
        assert "T" in timestamp, "Timestamp must be ISO format with T separator"
        assert "+" in timestamp or "Z" in timestamp, "Timestamp must include UTC offset"

    def test_transform_handles_missing_fields(self):
        """Test handling of missing fields in API response — returns 0 as default."""
        incomplete_data = {
            "bitcoin": {
                "usd": 68433.0
                # Missing: eur, usd_market_cap, usd_24h_vol, usd_24h_change, last_updated_at
            }
        }

        result = DataTransformer.transform(incomplete_data)

        assert len(result) == 1
        assert result[0]["price_usd"] == 68433.0
        assert result[0]["price_eur"] == 0
        assert result[0]["market_cap_usd"] == 0
        assert result[0]["volume_24h_usd"] == 0
        assert result[0]["change_24h_percent"] == 0

    def test_transform_empty_data(self):
        """Test transformation of empty data returns empty list."""
        result = DataTransformer.transform({})

        assert len(result) == 0
        assert isinstance(result, list)

    def test_transform_output_structure(self, sample_api_response):
        """Test that output has all required fields."""
        result = DataTransformer.transform(sample_api_response)

        required_fields = [
            "crypto_id", "timestamp", "price_usd", "price_eur",
            "market_cap_usd", "volume_24h_usd", "change_24h_percent", "last_updated"
        ]

        for field in required_fields:
            assert field in result[0], f"Missing field: {field}"

    def test_transform_preserves_exact_values(self, sample_api_response):
        """Test that values from the API are not altered during transformation."""
        result = DataTransformer.transform(sample_api_response)

        bitcoin = next(r for r in result if r["crypto_id"] == "bitcoin")
        assert bitcoin["price_usd"] == 68433.0
        assert bitcoin["price_eur"] == 63000.0
        assert bitcoin["market_cap_usd"] == 1350000000000
        assert bitcoin["volume_24h_usd"] == 25000000000
        assert bitcoin["change_24h_percent"] == 4.56
        assert bitcoin["last_updated"] == 1708945200

class TestCryptoKafkaProducer:
    """Tests for Kafka Producer class."""

    @patch('producer.KafkaProducer')
    def test_producer_connection_success(self, mock_kafka_producer):
        """Test successful Kafka connection."""
        mock_kafka_producer.return_value = MagicMock()

        producer = CryptoKafkaProducer()

        assert producer.producer is not None
        mock_kafka_producer.assert_called_once()

    @patch('producer.KafkaProducer')
    def test_producer_send_success(self, mock_kafka_producer):
        """Test successful message send returns True."""
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        producer = CryptoKafkaProducer()
        result = producer.send("bitcoin", {"price": 68433.0})

        assert result is True
        mock_instance.send.assert_called_once()

    @patch('producer.KafkaProducer')
    def test_producer_send_failure(self, mock_kafka_producer):
        """Test message send failure returns False instead of raising."""
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("Send failed")
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        producer = CryptoKafkaProducer()
        result = producer.send("bitcoin", {"price": 68433.0})

        assert result is False

    @patch('producer.KafkaProducer')
    def test_producer_send_with_correct_topic(self, mock_kafka_producer):
        """Test that messages are sent to the configured topic."""
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        producer = CryptoKafkaProducer()
        producer.send("bitcoin", {"price": 68433.0})

        call_args = mock_instance.send.call_args
        assert call_args[0][0] == Config.KAFKA_TOPIC

    # -----------------------------------------------------------------
    # NUEVOS TESTS: los que faltaban
    # -----------------------------------------------------------------

    @patch('producer.KafkaProducer')
    def test_producer_send_uses_crypto_id_as_key(self, mock_kafka_producer):
        """Test que la KEY del mensaje Kafka es el crypto_id.
        
        La key determina la partición en Kafka. Si se rompe, mensajes
        del mismo crypto irían a particiones distintas y perderíamos orden.
        """
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        producer = CryptoKafkaProducer()
        producer.send("bitcoin", {"price": 68433.0})

        call_args = mock_instance.send.call_args
        assert call_args[1]["key"] == "bitcoin"

    @patch('producer.time.sleep')
    @patch('producer.KafkaProducer')
    def test_producer_retries_on_kafka_error(self, mock_kafka_producer, mock_sleep):
        """Test que el producer reintenta si Kafka no está disponible al inicio.
        
        Simula que Kafka falla 2 veces y conecta al 3er intento.
        Esto es crítico en Docker donde el producer puede arrancar antes que Kafka.
        """
        mock_success = MagicMock()
        mock_kafka_producer.side_effect = [
            KafkaError("not ready"),
            KafkaError("not ready"),
            mock_success
        ]

        producer = CryptoKafkaProducer()

        assert producer.producer is mock_success
        assert mock_kafka_producer.call_count == 3
        # Verificar que durmió entre intentos
        assert mock_sleep.call_count == 2

    @patch('producer.time.sleep')
    @patch('producer.KafkaProducer')
    def test_producer_raises_connection_error_after_max_retries(self, mock_kafka_producer, mock_sleep):
        """Test que lanza ConnectionError si agota todos los reintentos.
        
        Sin este comportamiento el proceso quedaría en un bucle infinito silencioso.
        """
        mock_kafka_producer.side_effect = KafkaError("always fails")

        with pytest.raises(ConnectionError):
            CryptoKafkaProducer()

        assert mock_kafka_producer.call_count == Config.MAX_RETRIES

    @patch('producer.KafkaProducer')
    def test_producer_send_does_not_raise_on_kafka_error(self, mock_kafka_producer):
        """Test que send() devuelve False en lugar de propagar la excepción.
        
        El main loop no debe romperse por un fallo puntual de envío.
        """
        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.side_effect = KafkaError("broker unavailable")
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        producer = CryptoKafkaProducer()

        # No debe lanzar excepción
        try:
            result = producer.send("bitcoin", {"price": 68433.0})
            assert result is False
        except KafkaError:
            pytest.fail("send() propagó KafkaError en lugar de devolver False")


class TestIntegration:
    """Integration tests (with mocked external services)."""

    @responses.activate
    @patch('producer.KafkaProducer')
    def test_full_pipeline_message_content(self, mock_kafka_producer, sample_api_response):
        """Test que el contenido de los mensajes enviados a Kafka es correcto.
        
        El test anterior solo comprobaba el conteo. Este verifica que los
        valores que llegan a Kafka son exactamente los que devolvió la API.
        """
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json=sample_api_response,
            status=200
        )

        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        raw_data = CoinGeckoClient.fetch_prices()
        messages = DataTransformer.transform(raw_data)
        producer = CryptoKafkaProducer()
        for msg in messages:
            producer.send(msg["crypto_id"], msg)

        # Extraer todas las llamadas y buscar el mensaje de bitcoin
        all_calls = mock_instance.send.call_args_list
        bitcoin_call = next(
            (c for c in all_calls if c[1]["key"] == "bitcoin"),
            None
        )

        assert bitcoin_call is not None, "No se envió ningún mensaje con key='bitcoin'"
        sent_message = bitcoin_call[1]["value"]
        assert sent_message["price_usd"] == 68433.0
        assert sent_message["price_eur"] == 63000.0
        assert sent_message["change_24h_percent"] == 4.56
        assert sent_message["market_cap_usd"] == 1350000000000
        assert "timestamp" in sent_message

    @responses.activate
    @patch('producer.KafkaProducer')
    def test_full_pipeline_each_crypto_has_correct_key(self, mock_kafka_producer, sample_api_response):
        """Test que cada mensaje en Kafka tiene como key su propio crypto_id.
        
        Esto garantiza que Kafka enruta cada crypto a la partición correcta.
        """
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json=sample_api_response,
            status=200
        )

        mock_instance = MagicMock()
        mock_future = MagicMock()
        mock_future.get.return_value = None
        mock_instance.send.return_value = mock_future
        mock_kafka_producer.return_value = mock_instance

        raw_data = CoinGeckoClient.fetch_prices()
        messages = DataTransformer.transform(raw_data)
        producer = CryptoKafkaProducer()
        for msg in messages:
            producer.send(msg["crypto_id"], msg)

        all_calls = mock_instance.send.call_args_list
        for c in all_calls:
            key = c[1]["key"]
            value = c[1]["value"]
            assert key == value["crypto_id"], (
                f"Key '{key}' no coincide con crypto_id '{value['crypto_id']}'"
            )

    @responses.activate
    @patch('producer.KafkaProducer')
    def test_pipeline_handles_api_failure(self, mock_kafka_producer):
        """Test pipeline handles API failure gracefully."""
        responses.add(
            responses.GET,
            Config.COINGECKO_URL,
            json={"error": "service unavailable"},
            status=503
        )

        raw_data = CoinGeckoClient.fetch_prices()

        assert raw_data is None
        mock_kafka_producer.assert_not_called()