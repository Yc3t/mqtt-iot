import pytest
import serial
from unittest.mock import Mock,patch
from uart_buffer import UARTReceiver


@pytest.fixture
def mock_serial():
    with patch('serial.Serial') as mock:
        yield mock



@pytest.fixture
def uart_receiver(mock_serial):
    return UARTReceiver(port='COM21',baudrate=115200)

def test_init(uart_receiver):
    assert uart_receiver.sequence == 0
    assert uart_receiver.HEADER_MAGIC == b'\x55\x55\x55\x55'
    assert uart_receiver.HEADER_LENGTH == 8 
    assert uart_receiver.DEVICE_LENGTH == 42


def test_check_header(uart_receiver):
    valid_header = b'\x55\x55\x55\x55\x01\x02\x03'
    invalid_header = b'\x54\x55\x55\x55\x01\x02\x03'

    assert uart_receiver._check_header(valid_header) == True
    assert uart_receiver._check_header(invalid_header) == False


def test_parse_header_valid(uart_receiver):
    # Create valid header data
    header_data = (
        b'\x55\x55\x55\x55' #magic
        b'\x01'             #sequence
        b'\x02\x00'         #n_adv_raw (little endian)
        b'\x03'             # n_mac
    )

    result = uart_receiver._parse_header(header_data)

    assert result is not None
    assert result['sequence'] == 1
    assert result['n_adv_raw'] == 2
    assert result['n_mac'] == 3


def test_parse_header_invalid(uart_receiver):
    invalid_header = b'\x54\x55\x55\x55\x01\x02\x03'

    assert uart_receiver._parse_header(invalid_header) is None

def test_parse_device_valid(uart_receiver):
    device_data = (
        b'\x12\x34\x56\x78\x9A\xBC' +    # MAC
        b'\x01' +                         # addr_type
        b'\x02' +                         # adv_type
        b'\x80' +                         # rssi (-128)
        b'\x05' +                         # data_len
        b'Hello' + b'\x00' * 26 +        # data (31 bytes)
        b'\x03'                          # n_adv
    )

    result = uart_receiver._parse_device(device_data)

    assert result is not None
    assert result['mac'] == "12:34:56:78:9A:BC"
    assert result['addr_type'] == 1
    assert result['adv_type'] == 2
    assert result['rssi'] == -128
    assert result['data_len'] == 5
    assert result['n_adv'] == 3


def test_check_sequence(uart_receiver):
    uart_receiver.sequence = 0
    uart_receiver._check_sequence(1)
    assert uart_receiver.sequence == 1
    
    # Test sequence wrap-around
    uart_receiver.sequence = 255
    uart_receiver._check_sequence(0)
    assert uart_receiver.sequence == 0

@pytest.mark.parametrize("port,baudrate", [
    ("COM1", 9600),
    ("COM21", 115200),
    ("/dev/ttyUSB0", 57600)
])
def test_init_parameters(mock_serial, port, baudrate):
    receiver = UARTReceiver(port=port, baudrate=baudrate)
    mock_serial.assert_called_once_with(port, baudrate)

def test_close(uart_receiver):
    uart_receiver.serial.is_open = True
    uart_receiver.close()
    uart_receiver.serial.close.assert_called_once()





