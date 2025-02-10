import pytest
from unittest.mock import Mock, patch, call
import paho.mqtt.client as mqtt
from pymongo import MongoClient
from datetime import datetime
import json
import logging
from mqtt_mongo_sub import MQTTMongoSubscriber

@pytest.fixture
def mock_mongo():
    with patch('mqtt_mongo_sub.MongoClient') as mock:
        # Create mock collection and db
        mock_collection = Mock()
        mock_db = Mock()
        mock_db.session3 = mock_collection
        mock_instance = mock.return_value
        mock_instance.ble_scanner = mock_db
        yield mock

@pytest.fixture
def mock_mqtt():
    with patch('mqtt_mongo_sub.mqtt.Client') as mock:
        mock_instance = Mock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def subscriber(mock_mongo, mock_mqtt):
    with patch('mqtt_mongo_sub.logging'):
        sub = MQTTMongoSubscriber(
            mqtt_broker="test.broker",
            mqtt_port=1883,
            mqtt_topic="test/topic",
            mqtt_username="test_user",
            mqtt_password="test_pass",
            mongo_uri="mongodb://test:27017/"
        )
        yield sub

def test_init(mock_mongo, mock_mqtt):
    """Test initialization with various parameters"""
    with patch('mqtt_mongo_sub.logging'):
        sub = MQTTMongoSubscriber(
            mqtt_broker="test.broker",
            mqtt_port=1883,
            mqtt_topic="test/topic",
            mqtt_username="test_user",
            mqtt_password="test_pass",
            mongo_uri="mongodb://test:27017/"
        )
        
        assert sub.mqtt_topic == "test/topic"
        assert sub.messages_received == 0
        assert sub.devices_processed == 0
        assert sub.running == True

        # Verify MQTT setup
        mock_mqtt.username_pw_set.assert_called_once_with("test_user", "test_pass")
        mock_mqtt.connect.assert_called_once_with("test.broker", 1883, 60)

def test_on_connect(subscriber):
    """Test successful and failed MQTT connections"""
    # Test successful connection
    subscriber.on_connect(None, None, None, 0, None)
    assert "Connected to MQTT Broker successfully" in [
        record.getMessage() 
        for record in subscriber.logger.records 
        if "Connected to MQTT Broker successfully" in record.getMessage()
    ]

    # Test failed connection
    subscriber.on_connect(None, None, None, 1, None)
    assert any(
        "Failed to connect" in record.getMessage() 
        for record in subscriber.logger.records
    )

def test_on_message(subscriber):
    """Test message processing"""
    # Create a sample MQTT message
    test_payload = {
        "timestamp": datetime.now().isoformat(),
        "sequence": 1,
        "devices": [
            {
                "mac": "00:11:22:33:44:55",
                "rssi": -70
            }
        ],
        "n_adv_raw": 1
    }
    
    mock_msg = Mock()
    mock_msg.topic = "test/topic"
    mock_msg.payload = json.dumps(test_payload).encode()

    # Process the message
    subscriber.on_message(None, None, mock_msg)

    # Verify message processing
    assert subscriber.messages_received == 1
    assert subscriber.devices_processed == 1
    subscriber.collection.insert_one.assert_called_once()

def test_on_message_invalid_json(subscriber):
    """Test handling of invalid JSON messages"""
    mock_msg = Mock()
    mock_msg.topic = "test/topic"
    mock_msg.payload = b"invalid json"

    subscriber.on_message(None, None, mock_msg)
    assert "Error decoding JSON message" in [
        record.getMessage() 
        for record in subscriber.logger.records 
        if "Error decoding JSON message" in record.getMessage()
    ]

def test_signal_handler(subscriber):
    """Test signal handling"""
    subscriber.signal_handler(None, None)
    assert subscriber.running == False

@pytest.mark.asyncio
async def test_start_and_close(subscriber):
    """Test start and close methods"""
    # Mock time.sleep to avoid actual delays
    with patch('time.sleep'):
        # Simulate running for a brief period
        subscriber.running = False  # This will make the start() method exit immediately
        subscriber.start()
        
        # Verify cleanup
        subscriber.close()
        subscriber.mqtt_client.loop_stop.assert_called_once()
        subscriber.mqtt_client.disconnect.assert_called_once()
        subscriber.mongo_client.close.assert_called_once()

def test_mongo_connection_error(mock_mongo, mock_mqtt):
    """Test MongoDB connection error handling"""
    mock_mongo.side_effect = Exception("MongoDB connection failed")
    
    with pytest.raises(Exception) as exc_info:
        with patch('mqtt_mongo_sub.logging'):
            MQTTMongoSubscriber()
    
    assert "MongoDB connection failed" in str(exc_info.value)

def test_mqtt_connection_error(mock_mongo, mock_mqtt):
    """Test MQTT connection error handling"""
    mock_mqtt.connect.side_effect = Exception("MQTT connection failed")
    
    with pytest.raises(Exception) as exc_info:
        with patch('mqtt_mongo_sub.logging'):
            MQTTMongoSubscriber()
    
    assert "MQTT connection failed" in str(exc_info.value)

@pytest.mark.parametrize("log_level,expected_level", [
    ("debug", logging.DEBUG),
    ("info", logging.INFO),
])
def test_logging_levels(mock_mongo, mock_mqtt, log_level, expected_level):
    """Test different logging levels"""
    with patch('mqtt_mongo_sub.logging') as mock_logging:
        sub = MQTTMongoSubscriber(log_level=log_level)
        mock_logging.getLogger.return_value.setLevel.assert_called_with(expected_level) 