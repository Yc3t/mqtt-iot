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
import queue
import threading

class LogLevel(str, Enum):
    INFO = "info"
    DEBUG = "debug"

class UARTMQTTPublisher(UARTReceiver):
    # UART Protocol Constants
    HEADER_MAGIC = b'\x55\x55\x55\x55'
    HEADER_LENGTH = 8
    DEVICE_LENGTH = 32

    def __init__(self, port='/dev/ttyUSB0', baudrate=115200,
                 mqtt_broker="localhost", mqtt_port=1883,
                 mqtt_topic="admin/reader", mqtt_username=None, mqtt_password=None,
                 log_level="info"):
        """Initialize UART receiver with MQTT publisher"""
        # Store port, baudrate and MQTT topic as instance variables
        self.port = port
        self.baudrate = baudrate
        self.running = True
        self.mqtt_topic = mqtt_topic
        
        # Add signal handlers
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGHUP, self.signal_handler)
        signal.signal(signal.SIGQUIT, self.signal_handler)
        
        # Setup logging first
        self._setup_logging()
        
        # Check for crash recovery
        self._check_crash_recovery()
        
        self.logger.info("Starting UART MQTT Publisher")
        
        # Call parent class initialization
        super().__init__(port, baudrate)
        
        # Setup MQTT Client
        try:
            self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            
            # Set username and password if provided
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

        # Create a queue to decouple UART read and MQTT publish
        self.publish_queue = queue.Queue()
        self.publish_worker = threading.Thread(target=self._process_publish_queue, daemon=True)
        self.publish_worker.start()

    def _setup_logging(self):
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

    def _publish_buffer(self, header, devices):
        """Publish the buffer to MQTT topic"""
        try:
            document = {
                'timestamp': datetime.now().isoformat(),
                'sequence': header['sequence'],
                'n_adv_raw': header['n_adv_raw'],
                'n_mac': header['n_mac'],
                'devices': []
            }

            for device in devices:
                device_doc = {
                    'mac': device['mac'],
                    'addr_type': device['addr_type'],
                    'adv_type': device['adv_type'],
                    'rssi': device['rssi'],
                    'data_len': device['data_len'],
                    'data': device['data'].hex(),
                    'n_adv': device['n_adv']
                }
                document['devices'].append(device_doc)
            
            # Publish to MQTT
            message = json.dumps(document)
            result = self.mqtt_client.publish(self.mqtt_topic, message, qos=1)
            if result.rc == mqtt.MQTT_ERR_SUCCESS:
                self.logger.debug(f"Buffer queued for publishing - Sequence: {header['sequence']}, MACs: {len(devices)}")
                return True
            else:
                self.logger.error(f"Error queuing message for publish: {result.rc}")
                return False
        except Exception as e:
            self.logger.error(f"Error publishing to MQTT: {e}")
            return False

    def _process_publish_queue(self):
        """Worker thread to process publish queue and send buffers to MQTT"""
        while self.running or not self.publish_queue.empty():
            try:
                header, devices = self.publish_queue.get(timeout=1)
                self._publish_buffer(header, devices)
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error in publish worker: {e}")

    def _reset_serial(self):
        """Reset and reopen serial port"""
        try:
            if hasattr(self, 'serial') and self.serial.is_open:
                self.serial.close()
            
            self.logger.info(f"Attempting to reopen serial port {self.port}")
            self.serial = serial.Serial(
                port=self.port,
                baudrate=self.baudrate,
                timeout=1.0
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
        """Receive UART buffers and enqueue them for MQTT publishing"""
        start_time = time.time()
        processed_buffers = 0
        error_count = 0
        MAX_ERRORS = 3
        RETRY_DELAY = 7
        
        self.logger.info("Starting buffer reception...")
        
        while self.running:
            try:
                if duration and (time.time() - start_time) >= duration:
                    self.logger.info(f"Execution time ({duration}s) completed")
                    self.logger.info(f"Total buffers processed: {processed_buffers}")
                    break

                # Read with timeout
                byte = self.serial.read()
                if not byte:  # Timeout occurred
                    continue

                if byte == b'\x55':
                    potential_header = b'\x55' + self.serial.read(3)
                    if potential_header == self.HEADER_MAGIC:
                        self.logger.debug("UART header found")
                        error_count = 0  # Reset error count on successful read
                    else:
                        continue

                    header_data = potential_header + self.serial.read(self.HEADER_LENGTH - 4)
                    header = self._parse_header(header_data)
                    
                    if not header:
                        self.logger.warning("Error parsing header")
                        continue

                    devices = []
                    for i in range(header['n_mac']):
                        device_data = self.serial.read(self.DEVICE_LENGTH)
                        if not device_data or len(device_data) != self.DEVICE_LENGTH:
                            self.logger.warning(f"Incomplete device data received")
                            break
                        device = self._parse_device(device_data)
                        if device:
                            devices.append(device)
                            self.logger.debug(f"Device {i+1} parsed - MAC: {device['mac']}")

                    if devices:
                        # Enqueue the parsed buffer. The background thread will publish.
                        self.publish_queue.put((header, devices))
                        processed_buffers += 1
                        self.logger.debug(
                            f"Buffer #{processed_buffers} processed - "
                            f"Sequence: {header['sequence']}, "
                            f"Devices: {len(devices)}, "
                            f"N_ADV_RAW: {header['n_adv_raw']}"
                        )

            except serial.SerialException as e:
                error_count += 1
                self.logger.error(f"Serial communication error: {e}")
                if error_count >= MAX_ERRORS:
                    self.logger.error(f"Too many serial errors ({error_count}). Attempting reset...")
                    if not self._reset_serial():
                        self.logger.error("Failed to recover serial connection. Exiting.")
                        break
                    error_count = 0
                time.sleep(RETRY_DELAY)
            except KeyboardInterrupt:
                self.logger.info("Reception interrupted by user")
                break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                error_count += 1
                if error_count >= MAX_ERRORS:
                    self.logger.error(f"Too many errors ({error_count}). Exiting.")
                    break
                time.sleep(RETRY_DELAY)
                continue

        self.logger.info(f"Total buffers processed: {processed_buffers}")
        self.logger.info(f"Script finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def close(self):
        """Close all connections and wait for background threads"""
        try:
            super().close()  # Close serial port using parent method
            self.logger.info("Serial port closed")
            
            try:
                self.mqtt_client.loop_stop()
                self.mqtt_client.disconnect()
                self.logger.info("MQTT connection closed")
            except Exception as mqtt_e:
                self.logger.error(f"Error disconnecting MQTT: {mqtt_e}")
                
            # Wait for the publish worker thread to finish.
            if self.publish_worker.is_alive():
                self.publish_worker.join(timeout=5)

            self.logger.info(f"Script finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")

    def _parse_header(self, data):
        """Parse UART header data"""
        try:
            if len(data) != self.HEADER_LENGTH:
                return None
            
            sequence = int.from_bytes(data[4:6], byteorder='little')
            n_adv_raw = int.from_bytes(data[6:7], byteorder='little')
            n_mac = int.from_bytes(data[7:8], byteorder='little')
            
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
            n_adv = int.from_bytes(data[26:28], byteorder='little')
            
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
