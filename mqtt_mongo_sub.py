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

    def on_message(self, client, userdata, msg):
        """Callback for when a PUBLISH message is received from the server"""
        try:
            self.logger.info(f"Received message on topic: {msg.topic}")
            self.messages_received += 1
            
            # Parse the JSON message
            payload = json.loads(msg.payload.decode())
            self.logger.debug(f"Raw message payload: {msg.payload.decode()[:200]}...")  # Log first 200 chars
            
            # Convert ISO format timestamp string back to datetime
            payload['timestamp'] = datetime.fromisoformat(payload['timestamp'])
            
            # Validate device count
            n_devices = len(payload.get('devices', []))
            if n_devices > 50:
                self.logger.warning(f"Received more devices than expected: {n_devices}")
            
            self.logger.info(
                f"Message #{self.messages_received} - "
                f"Sequence: {payload.get('sequence', 'N/A')}, "
                f"Devices: {n_devices}/50, "
                f"N_ADV_RAW: {payload.get('n_adv_raw', 'N/A')}"
            )
            
            # Enqueue the payload for immediate MongoDB insertion
            self.message_queue.put(payload)
            
        except json.JSONDecodeError as e:
            self.logger.error(f"Error decoding JSON message: {e}")
            self.logger.error(f"Raw message: {msg.payload}")
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.logger.error(f"Raw message: {msg.payload}")

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