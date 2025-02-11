import paho.mqtt.client as mqtt
from pymongo import MongoClient
import json
import argparse
import logging
import os
from datetime import datetime
import signal
import sys
import platform
import time
import queue
import threading

class MQTTMongoSubscriber:
    # UART Protocol Constants
    HEADER_MAGIC = b'\x55\x55\x55\x55'
    HEADER_LENGTH = 9
    DEVICE_LENGTH = 42
    MAX_DEVICES = 50

    def __init__(self, mqtt_broker="localhost", mqtt_port=1883,
                 mqtt_topic="admin/reader", mqtt_username=None, mqtt_password=None,
                 mongo_uri="mongodb://localhost:27017/",
                 log_level="info"):
        """Initialize MQTT subscriber with MongoDB connection
        Expects messages with updated format supporting up to 50 devices.
        Header format:
        - 4 bytes: Magic (0x55555555)
        - 1 byte:  Sequence
        - 2 bytes: n_adv_raw (uint16_t)
        - 2 bytes: n_mac (uint16_t)
        """
        self.running = True
        self.mqtt_topic = mqtt_topic
        self.messages_received = 0
        self.devices_processed = 0
        
        # Add signal handlers based on platform
        signal.signal(signal.SIGTERM, self.signal_handler)
        signal.signal(signal.SIGINT, self.signal_handler)
        
        # Add UNIX-specific signals only if not on Windows
        if platform.system() != 'Windows':
            signal.signal(signal.SIGHUP, self.signal_handler)
            signal.signal(signal.SIGQUIT, self.signal_handler)
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Connect to MongoDB
        try:
            self.mongo_client = MongoClient(mongo_uri,
                                            serverSelectionTimeoutMS=5000,
                                            connectTimeoutMS=5000,
                                            socketTimeoutMS=5000)
            self.mongo_client.server_info()  # Test connection
            self.db = self.mongo_client.ble_scanner
            self.collection = self.db.session3
            self.logger.info(f"Connected to MongoDB at {mongo_uri}")
        except Exception as e:
            self.logger.error(f"Error connecting to MongoDB: {e}")
            raise

        # Setup MQTT Client with Version 2 API
        try:
            self.mqtt_client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
            
            # Set username and password if provided
            if mqtt_username:
                self.logger.info("Using MQTT authentication")
                self.mqtt_client.username_pw_set(mqtt_username, mqtt_password)
            
            self.mqtt_client.on_connect = self.on_connect
            self.mqtt_client.on_message = self.on_message
            self.mqtt_client.on_disconnect = self.on_disconnect
            self.mqtt_client.on_subscribe = self.on_subscribe
            
            self.logger.info(f"Connecting to MQTT broker at {mqtt_broker}:{mqtt_port}")
            self.mqtt_client.connect(mqtt_broker, mqtt_port, 60)
            self.logger.info("MQTT client setup complete")
        except Exception as e:
            self.logger.error(f"Error connecting to MQTT broker: {e}")
            raise

        # Create a thread-safe queue and start a background worker for immediate MongoDB inserts.
        self.message_queue = queue.Queue()
        self.mongo_worker = threading.Thread(target=self._process_messages, daemon=True)
        self.mongo_worker.start()

    def _setup_logging(self, log_level):
        """Configure logging system"""
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)
            
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(log_dir, f"mqtt_mongo_{timestamp}.log")
        
        self.logger = logging.getLogger('MQTT_Mongo_Subscriber')
        self.logger.setLevel(logging.DEBUG if log_level.lower() == "debug" else logging.INFO)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if log_level.lower() == "debug" else logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Script started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    def on_subscribe(self, client, userdata, mid, reason_codes, properties):
        """Callback when subscription is confirmed"""
        self.logger.info(f"Subscription confirmed with reason codes: {reason_codes}")

    def on_connect(self, client, userdata, flags, reason_code, properties):
        """Callback for when the client receives a CONNACK response from the server"""
        if reason_code == 0:
            self.logger.info("Connected to MQTT Broker successfully")
            # Subscribe with QoS=1 to ensure reliable delivery.
            client.subscribe(self.mqtt_topic, qos=1)
            self.logger.info(f"Subscribed to topic: {self.mqtt_topic}")
        else:
            self.logger.error(f"Failed to connect to MQTT Broker with code: {reason_code}")

    def on_disconnect(self, client, userdata, reason_code, properties):
        """Callback for when the client disconnects from the server"""
        self.logger.warning(f"Disconnected from MQTT Broker with reason code: {reason_code}")
        if reason_code != 0:
            self.logger.warning("Unexpected disconnection. Attempting to reconnect...")

    def _parse_header(self, data):
        """Parse UART header data"""
        try:
            if len(data) != self.HEADER_LENGTH:
                return None
            
            sequence = int.from_bytes(data[4:5], byteorder='little')
            n_adv_raw = int.from_bytes(data[5:7], byteorder='little')
            n_mac = int.from_bytes(data[7:9], byteorder='little')
            
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
                'data': adv_data.hex(),
                'n_adv': n_adv
            }
        except Exception as e:
            self.logger.error(f"Error parsing device data: {e}")
            return None

    def _parse_buffer(self, raw_data):
        """Parse complete buffer from raw data"""
        try:
            if len(raw_data) < self.HEADER_LENGTH:
                self.logger.error("Buffer too short for header")
                return None

            # Verify header magic
            if raw_data[:4] != self.HEADER_MAGIC:
                self.logger.error("Invalid header magic")
                return None

            # Parse header
            header = self._parse_header(raw_data[:self.HEADER_LENGTH])
            if not header:
                return None

            # Parse devices
            devices = []
            offset = self.HEADER_LENGTH
            for i in range(header['n_mac']):
                if len(raw_data) < offset + self.DEVICE_LENGTH:
                    break
                device_data = raw_data[offset:offset + self.DEVICE_LENGTH]
                device = self._parse_device(device_data)
                if device:
                    devices.append(device)
                    self.logger.debug(f"Device {i+1} parsed - MAC: {device['mac']}")
                offset += self.DEVICE_LENGTH

            return {
                'timestamp': datetime.now().isoformat(),
                **header,
                'devices': devices
            }
        except Exception as e:
            self.logger.error(f"Error parsing buffer: {e}")
            return None

    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server"""
        try:
            self.logger.debug(f"Received raw buffer of {len(msg.payload)} bytes")
            self.messages_received += 1
            
            # Parse the raw buffer
            payload = self._parse_buffer(msg.payload)
            if not payload:
                raise ValueError("Failed to parse raw buffer")
            
            n_devices = len(payload['devices'])
            self.logger.info(
                f"Message #{self.messages_received} - "
                f"Sequence: {payload['sequence']}, "
                f"Devices: {n_devices}/{self.MAX_DEVICES}, "
                f"N_ADV_RAW: {payload['n_adv_raw']}"
            )
            
            # Enqueue the parsed payload for MongoDB insertion
            self.message_queue.put(payload)
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.logger.error(f"Raw message length: {len(msg.payload)} bytes")

    def _process_messages(self):
        """Worker thread to immediately insert each received message into MongoDB"""
        while self.running or not self.message_queue.empty():
            try:
                payload = self.message_queue.get(timeout=1)
                self.collection.insert_one(payload)
                new_devices = len(payload.get('devices', []))
                self.devices_processed += new_devices
                self.logger.info(f"Inserted message: Sequence: {payload.get('sequence', 'N/A')}, Devices: {new_devices}")
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error inserting record: {e}")

    def signal_handler(self, signum, frame):
        """Signal handler for clean shutdown"""
        self.running = False
        self.logger.info("Termination signal received")

    def start(self):
        """Start the MQTT client loop and log stats periodically"""
        self.logger.info("Starting MQTT subscriber...")
        self.mqtt_client.loop_start()
        
        # Keep the main thread running and log stats periodically
        try:
            while self.running:
                self.logger.info(
                    f"Status - Messages received: {self.messages_received}, "
                    f"Devices processed: {self.devices_processed}"
                )
                time.sleep(10)
        except KeyboardInterrupt:
            self.logger.info("Received keyboard interrupt")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
        finally:
            self.close()

    def close(self):
        """Close all connections and wait for background threads"""
        try:
            self.running = False
            self.mqtt_client.loop_stop()
            self.mqtt_client.disconnect()
            self.logger.info("MQTT connection closed")
            
            self.mongo_client.close()
            self.logger.info("MongoDB connection closed")
            
            if self.mongo_worker.is_alive():
                self.mongo_worker.join(timeout=5)

            self.logger.info(f"Script finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception as e:
            self.logger.error(f"Error closing connections: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MQTT Subscriber to MongoDB')
    parser.add_argument('--mqtt-broker', type=str,
                        default="localhost",
                        help='MQTT broker address (default: localhost)')
    parser.add_argument('--mqtt-port', type=int,
                        default=1883,
                        help='MQTT broker port (default: 1883)')
    parser.add_argument('--mqtt-topic', type=str,
                        default="admin/reader",
                        help='MQTT topic to subscribe to (default: admin/reader)')
    parser.add_argument('--mqtt-username', type=str,
                        help='MQTT username (optional)')
    parser.add_argument('--mqtt-password', type=str,
                        help='MQTT password (optional)')
    parser.add_argument('--mongo-uri', type=str,
                        default="mongodb://localhost:27017/",
                        help='MongoDB URI (default: mongodb://localhost:27017/)')
    parser.add_argument('--log-level', type=str,
                        choices=['info', 'debug'],
                        default='info',
                        help='Logging level (default: info)')
    
    args = parser.parse_args()
    
    try:
        subscriber = MQTTMongoSubscriber(
            mqtt_broker=args.mqtt_broker,
            mqtt_port=args.mqtt_port,
            mqtt_topic=args.mqtt_topic,
            mqtt_username=args.mqtt_username,
            mqtt_password=args.mqtt_password,
            mongo_uri=args.mongo_uri,
            log_level=args.log_level
        )
        subscriber.start()
    except Exception as e:
        if hasattr(subscriber, 'logger'):
            subscriber.logger.error(f"Error: {e}")
        else:
            print(f"Error: {e}")
    finally:
        if hasattr(subscriber, 'close'):
            subscriber.close() 