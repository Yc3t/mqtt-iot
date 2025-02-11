import serial
from datetime import datetime
import time
import argparse
import json
import paho.mqtt.client as mqtt
from uart import UARTReceiver
import logging
import os
from enum import Enum
import signal
import sys

class LogLevel(str, Enum):
    INFO = "info"
    DEBUG = "debug"

class UARTMQTTPublisher(UARTReceiver):
    # UART Protocol Constants - Matching C definitions
    HEADER_MAGIC = b'\x55\x55\x55\x55'
    HEADER_LENGTH = 8  # 4 (magic) + 1 (sequence) + 2 (n_adv_raw) + 1 (n_mac)
    DEVICE_LENGTH = 42  # 6 + 1 + 1 + 1 + 1 + 31 + 1 = 42 bytes
    MAX_DEVICES = 64  # Match HASH_SIZE from C code (64)
    SAMPLING_INTERVAL = 5.0  # Match SAMPLING_INTERVAL_MS from C code (5000ms)

    def __init__(self, port='/dev/ttyUSB0', baudrate=115200,
                 mqtt_broker="localhost", mqtt_port=1883,
                 mqtt_topic="admin/reader", mqtt_username=None, mqtt_password=None,
                 log_level="info"):
        """Initialize UART receiver with MQTT publisher"""
        # Setup logging first
        self._setup_logging(log_level)
        self.logger.info(f"Script started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Initialize serial port with a 1-second timeout so that read(n) will wait for complete data
        try:
            self.serial = serial.Serial(
                port=port,
                baudrate=baudrate,
                timeout=1  # Blocking reads with 1 sec timeout
            )
            self.logger.info(f"Opened serial port {port} at {baudrate} baud")
            
            # Flush any existing data
            self.serial.reset_input_buffer()
            self.serial.reset_output_buffer()
            
        except serial.SerialException as e:
            self.logger.error(f"Failed to open serial port: {e}")
            raise
            
        self.port = port
        self.baudrate = baudrate
        self.running = True
        self.mqtt_topic = mqtt_topic
        
        # Add signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)
        signal.signal(signal.SIGQUIT, self.signal_handler)
        
        # Setup MQTT Client
        try:
            self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            
            if mqtt_username:
                self.logger.info("Using MQTT authentication")
                self.mqtt_client.username_pw_set(mqtt_username, mqtt_password)
            
            self.mqtt_client.on_connect = self.on_mqtt_connect
            self.mqtt_client.on_disconnect = self.on_mqtt_disconnect
            self.mqtt_client.on_publish = self.on_mqtt_publish
            
            self.logger.info(f"Connecting to MQTT broker at {mqtt_broker}:{mqtt_port}")
            self.mqtt_client.connect(mqtt_broker, mqtt_port, 60)
            self.mqtt_client.loop_start()
            self.logger.info("MQTT client setup complete")
        except Exception as e:
            self.logger.error(f"Error connecting to MQTT broker: {e}")
            raise

    def _setup_logging(self, log_level):
        """Configure logging system"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"uart_mqtt_{timestamp}.log")
        
        self.logger = logging.getLogger('UART_MQTT_Publisher')
        self.logger.setLevel(logging.DEBUG)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Script started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def _check_crash_recovery(self):
        """Check for unexpected termination"""
        log_dir = "logs"
        if os.path.exists(log_dir):
            log_files = sorted(
                [f for f in os.listdir(log_dir) if f.startswith("uart_mqtt_")],
                reverse=True
            )
            if log_files:
                last_log = os.path.join(log_dir, log_files[0])
                try:
                    with open(last_log, 'r') as f:
                        last_lines = f.readlines()[-3:]
                        if not any("Script finished" in line for line in last_lines):
                            self.logger.warning("Detected unexpected termination in previous run")
                except Exception:
                    pass

    def on_mqtt_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when the client receives a CONNACK response from the server"""
        if reason_code == 0:
            self.logger.info("Connected to MQTT Broker successfully")
        else:
            self.logger.error(f"Failed to connect to MQTT Broker with code: {reason_code}")

    def on_mqtt_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        """Callback for when the client disconnects from the server"""
        self.logger.warning(f"Disconnected from MQTT Broker with reason code: {reason_code}")
        if reason_code != 0:
            self.logger.warning("Unexpected disconnection. Attempting to reconnect...")

    def on_mqtt_publish(self, client, userdata, mid, reason_code=0, properties=None):
        """Callback for when a message is published"""
        if reason_code == 0:
            self.logger.debug(f"Message {mid} published successfully")
        else:
            self.logger.warning(f"Message {mid} failed to publish with reason code: {reason_code}")

    def _publish_buffer(self, raw_data):
        """Publish the raw buffer immediately to MQTT topic"""
        try:
            self.logger.debug(f"Publishing buffer of {len(raw_data)} bytes")
            result = self.mqtt_client.publish(self.mqtt_topic, raw_data, qos=1)
            
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.debug("Buffer published successfully")
                return True
            else:
                self.logger.error(f"Error publishing message: {result.rc}")
                return False
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}")
            return False

    def _reset_serial(self):
        """Reset and reopen serial port with a 20-second timeout"""
        try:
            if hasattr(self, 'serial') and self.serial.is_open:
                self.serial.close()
            
            self.logger.info(f"Attempting to reopen serial port {self.port}")
            # Increase timeout to 20.0 seconds to accommodate the 5-second buffer interval.
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=20.0
            )
            self.serial.reset_input_buffer()
            self.serial.reset_output_buffer()
            self.logger.info("Serial port reopened successfully")
            return True
        except serial.SerialException as e:
            self.logger.error(f"Failed to reset serial port: {e}")
            return False

    def signal_handler(self, signum, frame):
        """Signal handler for clean shutdown"""
        self.running = False
        self.logger.info("Termination signal received")
        
    def receive_messages(self, duration=None):
        """Receive UART buffers and publish them immediately to MQTT using a combined tracker approach."""
        start_time = time.time()
        processed_buffers = 0

        self.logger.info("Starting combined capture of BLE messages...")
        self.logger.debug(f"Expecting data every {self.SAMPLING_INTERVAL} seconds")

        while self.running:
            try:
                # Check for duration limit if provided
                if duration and (time.time() - start_time) >= duration:
                    self.logger.info(f"Execution time ({duration}s) completed")
                    break

                # --- Wait for header magic ---
                # Loop until we find the header start (0x55 x4)
                while True:
                    byte = self.serial.read(1)
                    if not byte:
                        # No data yet, wait a bit.
                        time.sleep(0.05)
                        continue
                    if byte == b'\x55':
                        potential_header = byte + self.serial.read(3)
                        if potential_header == self.HEADER_MAGIC:
                            self.logger.debug("Cabecera UART encontrada")
                            break

                # --- Read and parse the rest of the header ---
                header_rest = self.serial.read(self.HEADER_LENGTH - 4)
                if len(header_rest) != self.HEADER_LENGTH - 4:
                    self.logger.warning("Incomplete header received")
                    continue
                header_data = potential_header + header_rest
                header = self._parse_header(header_data)
                if not header:
                    self.logger.warning("Error parsing header")
                    continue
                self.logger.debug(f"Header parsed: {header}")

                # --- Read all device data ---
                expected_device_bytes = header['n_mac'] * self.DEVICE_LENGTH
                device_data = b""
                while len(device_data) < expected_device_bytes:
                    chunk = self.serial.read(expected_device_bytes - len(device_data))
                    if not chunk:
                        time.sleep(0.05)
                        continue
                    device_data += chunk
                if len(device_data) != expected_device_bytes:
                    self.logger.warning(
                        f"Incomplete device data: got {len(device_data)} bytes, expected {expected_device_bytes}"
                    )
                    continue

                # Combine header and device data to form the complete buffer
                complete_buffer = header_data + device_data

                # --- Publish the complete buffer ---
                if self._publish_buffer(complete_buffer):
                    processed_buffers += 1
                    self.logger.info(
                        f"Published buffer #{processed_buffers} with {header['n_mac']} devices"
                    )
                else:
                    self.logger.warning("Failed to publish buffer")

            except serial.SerialException as e:
                self.logger.error(f"Serial error: {e}")
                time.sleep(1)
                continue

            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                time.sleep(1)
                continue

        self.logger.info(f"Total buffers processed: {processed_buffers}")

    def close(self):
        """Close all connections"""
        try:
            super().close()  # Close serial port using parent method
            self.logger.info("Serial port closed")
            
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                self.logger.info("MQTT connection closed")
            except Exception as mqtt_e:
                self.logger.error(f"Error disconnecting MQTT: {mqtt_e}")

            self.logger.info(f"Script finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")

    def _parse_header(self, data):
        """Parse UART header data with new format"""
        try:
            if len(data) != self.HEADER_LENGTH:
                return None
            
            # Parse using uint16_t for n_adv_raw and n_mac
            sequence = int.from_bytes(data[4:5], byteorder='little')
            n_adv_raw = int.from_bytes(data[5:7], byteorder='little')  # 2 bytes
            n_mac = int.from_bytes(data[7:8], byteorder='little')      # 1 byte
            
            return {
                'sequence': sequence,
                'n_adv_raw': n_adv_raw,
                'n_mac': n_mac
            }
        except Exception as e:
            self.logger.error(f"Error parsing header: {e}")
            return None

    def _parse_device(self, data):
        """Parse device data from UART"""
        try:
            if len(data) != self.DEVICE_LENGTH:
                return None
            
            mac = ':'.join([f"{b:02X}" for b in data[0:6]])
            addr_type = int.from_bytes(data[6:7], byteorder='little')
            adv_type = int.from_bytes(data[7:8], byteorder='little')
            rssi = int.from_bytes(data[8:9], byteorder='little', signed=True)
            data_len = int.from_bytes(data[9:10], byteorder='little')
            adv_data = data[10:26]  # 16 bytes of data
            n_adv = int.from_bytes(data[26:27], byteorder='little')
            
            return {
                'mac': mac,
                'addr_type': addr_type,
                'adv_type': adv_type,
                'rssi': rssi,
                'data_len': data_len,
                'data': adv_data,
                'n_adv': n_adv
            }
        except Exception as e:
            self.logger.error(f"Error parsing device data: {e}")
            return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='BLE Scanner UART MQTT Publisher')
    parser.add_argument('--port', type=str, default='/dev/ttyUSB0',
                        help='Serial port (default: /dev/ttyUSB0)')
    parser.add_argument('--duration', type=int,
                        help='Capture duration in seconds')
    parser.add_argument('--mqtt-broker', type=str,
                        default="localhost",
                        help='MQTT broker address (default: localhost)')
    parser.add_argument('--mqtt-port', type=int,
                        default=1883,
                        help='MQTT broker port (default: 1883)')
    parser.add_argument('--mqtt-topic', type=str,
                        default="admin/reader",
                        help='MQTT topic (default: admin/reader)')
    parser.add_argument('--mqtt-username', type=str,
                        help='MQTT username (optional)')
    parser.add_argument('--mqtt-password', type=str,
                        help='MQTT password (optional)')
    parser.add_argument('--log-level', type=str,
                        choices=['info', 'debug'],
                        default='info',
                        help='Logging level (default: info)')
    
    args = parser.parse_args()
    
    try:
        publisher = UARTMQTTPublisher(
            port=args.port,
            mqtt_broker=args.mqtt_broker,
            mqtt_port=args.mqtt_port,
            mqtt_topic=args.mqtt_topic,
            mqtt_username=args.mqtt_username,
            mqtt_password=args.mqtt_password,
            log_level=args.log_level
        )
        publisher.logger.info("Starting capture %s", 
                              "indefinitely" if not args.duration else f"for {args.duration} seconds")
        publisher.receive_messages(duration=args.duration)
    except Exception as e:
        if hasattr(publisher, 'logger'):
            publisher.logger.error(f"Error: {e}")
        else:
            print(f"Error: {e}")
    finally:
        if hasattr(publisher, 'close'):
            publisher.close() 
