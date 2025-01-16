import paho.mqtt.client as mqtt
from pymongo import MongoClient
import struct
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MQTT Broker configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "ble/scanner/data/raw"

# MongoDB configuration
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "ble_scanner_sim"
COLLECTION_NAME = "raw_data"

# Constants
UART_HEADER_MAGIC = b"\x55\x55\x55\x55"
DEVICE_DATA_SIZE = 42  # Size of each device entry in bytes

# MongoDB Client setup
def setup_mongodb():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        logger.info("Connected to MongoDB successfully.")
        return collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

# Parse buffer and store in MongoDB
def parse_and_store_buffer(buffer, collection):
    try:
        # Unpack header
        header = struct.unpack("<4sBBHB", buffer[:9])
        logger.info("\nBuffer Header:")
        logger.info(f"Magic: {header[0].hex()}")
        logger.info(f"Message Type: {header[1]}")
        logger.info(f"Sequence Number: {header[2]}")
        logger.info(f"Total Events: {header[3]}")
        logger.info(f"Unique MACs: {header[4]}")

        # Unpack device data
        device_data = buffer[9:]
        num_devices = header[4]

        # Prepare document for MongoDB
        document = {
            "timestamp": datetime.now(),
            "sequence": header[2],
            "n_adv_raw": header[3],
            "n_mac": header[4],
            "devices": [],
        }

        for i in range(num_devices):
            start = i * DEVICE_DATA_SIZE
            device = struct.unpack("<6sBBbB31sB", device_data[start:start + DEVICE_DATA_SIZE])
            device_doc = {
                "mac": device[0].hex(":"),
                "addr_type": device[1],
                "adv_type": device[2],
                "rssi": device[3],
                "data_length": device[4],
                "data": device[5].hex(),
                "n_adv": device[6],
            }
            document["devices"].append(device_doc)

        # Insert document into MongoDB
        result = collection.insert_one(document)
        logger.info(f"Buffer stored in MongoDB with ID: {result.inserted_id}")

    except struct.error as e:
        logger.error(f"Error unpacking buffer: {e}")
    except Exception as e:
        logger.error(f"Error storing buffer in MongoDB: {e}")

# MQTT on_message callback
def on_message(client, userdata, msg):
    logger.info(f"Received message on topic {msg.topic}")
    parse_and_store_buffer(msg.payload, collection)

# MQTT Client setup
def setup_mqtt_client():
    try:
        client = mqtt.Client()
        client.on_message = on_message
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.subscribe(MQTT_TOPIC)
        logger.info("Connected to MQTT broker successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        raise

# Main function
if __name__ == "__main__":
    try:
        # Initialize MongoDB collection
        collection = setup_mongodb()

        # Initialize MQTT client
        mqtt_client = setup_mqtt_client()
        logger.info("Starting MQTT subscriber...")

        # Start the MQTT loop
        mqtt_client.loop_forever()

    except KeyboardInterrupt:
        logger.info("\nSubscriber stopped.")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
