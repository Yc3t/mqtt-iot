import pytest
from unittest.mock import Mock, patch, call
import serial
import paho.mqtt.client as mqtt
from datetime import datetime
import json
import logging
from mqtt_mongo_pub import UARTMQTTPublisher

@pytest.fixture
def mock_serial():
    with patch('serial.Serial') as mock:
        mock_instance = Mock()
        mock.return_value = mock_instance
        # Setup basic serial port behavior
        mock_instance.is_open = True
        mock_instance.read.return_value = b''  # Default empty read
        yield mock_instance

@pytest.fixture
def mock_mqtt():
    with patch('paho.mqtt.client.Client') as mock:
        mock_instance = Mock()
        mock.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def publisher(mock_serial, mock_mqtt):
    with patch('mqtt_mongo_pub.logging'):
        pub = UARTMQTTPublisher(
            port='/dev/ttyUSB0',
            baudrate=115200,
            mqtt_broker="test.broker",
            mqtt_port=1883,
            mqtt_topic="test/topic",
            mqtt_username="test_user",
            mqtt_password="test_pass"
        )
        yield pub

def create_mock_uart_data(sequence=1, n_adv_raw=1, n_mac=1):
    """Helper function to create mock UART data"""
    # Create header
    header = (
        b'\x55\x55\x55\x55'  # Magic bytes
        + sequence.to_bytes(2, 'little')  # Sequence number
        + n_adv_raw.to_bytes(1, 'little')  # Number of raw advertisements
        + n_mac.to_bytes(1, 'little')  # Number of unique MAC addresses
    )
    
    # Create device data (32 bytes per device)
    device = (
        b'\x11\x22\x33\x44\x55\x66'  # MAC address
        + b'\x00'  # Address type
        + b'\x00'  # Advertisement type
        + b'\xC8'  # RSSI (-56 in signed byte)
        + b'\x10'  # Data length
        + b'0123456789ABCDEF'  # 16 bytes of data
        + b'\x01\x00'  # Number of advertisements
        + b'\x00' * 8  # Padding
    )
    
    return header + device * n_mac

def test_init(mock_serial, mock_mqtt):
    """Test initialization with various parameters"""
    with patch('mqtt_mongo_pub.logging'):
        pub = UARTMQTTPublisher(
            port='/dev/ttyUSB0',
            baudrate=115200,
            mqtt_broker="test.broker",
            mqtt_topic="test/topic"
        )
        
        assert pub.port == '/dev/ttyUSB0'
        assert pub.baudrate == 115200
        assert pub.mqtt_topic == "test/topic"
        assert pub.running == True

        # Verify UART setup
        mock_serial.assert_called_once_with(
            port='/dev/ttyUSB0',
            baudrate=115200,
            timeout=1.0
        )

def test_uart_header_parsing(publisher):
    """Test UART header parsing"""
    test_header = create_mock_uart_data(sequence=42, n_adv_raw=5, n_mac=2)[:8]
    result = publisher._parse_header(test_header)
    
    assert result is not None
    assert result['sequence'] == 42
    assert result['n_adv_raw'] == 5
    assert result['n_mac'] == 2

def test_uart_device_parsing(publisher):
    """Test UART device data parsing"""
    test_device = (
        b'\x11\x22\x33\x44\x55\x66'  # MAC address
        + b'\x00'  # Address type
        + b'\x00'  # Advertisement type
        + b'\xC8'  # RSSI
        + b'\x10'  # Data length
        + b'0123456789ABCDEF'  # Data
        + b'\x01\x00'  # Number of advertisements
        + b'\x00' * 8  # Padding
    )
    
    result = publisher._parse_device(test_device)
    
    assert result is not None
    assert result['mac'] == '11:22:33:44:55:66'
    assert result['addr_type'] == 0
    assert result['adv_type'] == 0
    assert result['rssi'] == -56  # 0xC8 as signed byte
    assert result['data_len'] == 16
    assert result['n_adv'] == 1

def test_mqtt_publishing(publisher):
    """Test MQTT message publishing"""
    header = {'sequence': 1, 'n_adv_raw': 2, 'n_mac': 1}
    devices = [{
        'mac': '11:22:33:44:55:66',
        'addr_type': 0,
        'adv_type': 0,
        'rssi': -70,
        'data_len': 16,
        'data': b'0123456789ABCDEF',
        'n_adv': 1
    }]
    
    publisher._publish_buffer(header, devices)
    
    # Verify MQTT publish was called
    publisher.mqtt_client.publish.assert_called_once()
    call_args = publisher.mqtt_client.publish.call_args[0]
    assert call_args[0] == publisher.mqtt_topic  # Topic
    
    # Verify published message format
    published_msg = json.loads(call_args[1])
    assert 'timestamp' in published_msg
    assert published_msg['sequence'] == 1
    assert published_msg['n_adv_raw'] == 2
    assert len(published_msg['devices']) == 1
    assert published_msg['devices'][0]['mac'] == '11:22:33:44:55:66'

def test_receive_messages(publisher, mock_serial):
    """Test message reception loop"""
    # Setup mock serial data
    mock_data = create_mock_uart_data(sequence=1, n_adv_raw=1, n_mac=1)
    mock_serial.read.side_effect = [
        mock_data[i:i+1] for i in range(len(mock_data))
    ] + [b'']  # Empty read to end the loop
    
    # Run receive_messages with a short duration
    publisher.receive_messages(duration=0.1)
    
    # Verify MQTT publish was called
    publisher.mqtt_client.publish.assert_called()

def test_serial_error_handling(publisher, mock_serial):
    """Test serial port error handling"""
    mock_serial.read.side_effect = serial.SerialException("Test error")
    
    # Run with short duration to test error handling
    publisher.receive_messages(duration=0.1)
    
    # Verify error was logged
    assert any(
        "Serial communication error" in record.getMessage()
        for record in publisher.logger.records
    )

def test_reset_serial(publisher, mock_serial):
    """Test serial port reset functionality"""
    publisher._reset_serial()
    
    # Verify serial port was closed and reopened
    mock_serial.close.assert_called_once()
    assert mock_serial.reset_input_buffer.called
    assert mock_serial.reset_output_buffer.called

def test_signal_handler(publisher):
    """Test signal handling"""
    publisher.signal_handler(None, None)
    assert publisher.running == False

def test_close(publisher):
    """Test cleanup on close"""
    publisher.close()
    
    # Verify all connections are closed
    assert publisher.mqtt_client.loop_stop.called
    assert publisher.mqtt_client.disconnect.called
    assert publisher.serial.close.called

@pytest.mark.parametrize("log_level,expected_level", [
    ("debug", logging.DEBUG),
    ("info", logging.INFO),
])
def test_logging_levels(mock_serial, mock_mqtt, log_level, expected_level):
    """Test different logging levels"""
    with patch('mqtt_mongo_pub.logging') as mock_logging:
        pub = UARTMQTTPublisher(
            port='/dev/ttyUSB0',
            log_level=log_level
        )
        mock_logging.getLogger.return_value.setLevel.assert_called_with(expected_level)

def test_invalid_uart_data(publisher, mock_serial):
    """Test handling of invalid UART data"""
    # Setup invalid data
    mock_serial.read.side_effect = [b'\x00'] * 10
    
    # Run with short duration
    publisher.receive_messages(duration=0.1)
    
    # Verify no MQTT publish occurred
    assert not publisher.mqtt_client.publish.called 