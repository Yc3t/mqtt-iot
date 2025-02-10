import paho.mqtt.client as mqtt
from pymongo import MongoClient
import struct
from datetime import datetime
import logging
import threading
from queue import Queue

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
        # Validate header magic
        if buffer[:4] != UART_HEADER_MAGIC:
            logger.error("Invalid header magic. Skipping buffer.")
            return

        # Unpack header: format <4sBBHB (9 bytes total)
        header = struct.unpack("<4sBBHB", buffer[:9])
        logger.info("Buffer Header:")
        logger.info(f"Magic: {header[0].hex()}")
        logger.info(f"Message Type: {header[1]}")
        logger.info(f"Sequence Number: {header[2]}")
        logger.info(f"Total Events: {header[3]}")
        logger.info(f"Unique MACs: {header[4]}")

        device_data = buffer[9:]
        num_devices = header[4]

        document = {
            "timestamp": datetime.now(),
            "sequence": header[2],
            "n_adv_raw": header[3],
            "n_mac": header[4],
            "devices": [],
        }

        for i in range(num_devices):
            start = i * DEVICE_DATA_SIZE
            end = start + DEVICE_DATA_SIZE
            # Validate that we have enough data
            if end > len(device_data):
                logger.warning("Buffer truncated. Stopping device parsing.")
                break
            device = struct.unpack("<6sBBbB31sB", device_data[start:end])
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

        result = collection.insert_one(document)
        logger.info(f"Buffer stored in MongoDB with ID: {result.inserted_id}")
    except struct.error as e:
        logger.error(f"Error unpacking buffer: {e}")
    except Exception as e:
        logger.error(f"Error storing buffer in MongoDB: {e}")

# --- MQTT and Worker Setup ---

# Use a work queue to decouple incoming MQTT messages from processing.
message_queue = Queue()

def mqtt_on_message(client, userdata, msg):
    logger.info(f"Received message on topic {msg.topic}")
    message_queue.put(msg.payload)

def setup_mqtt_client():
    try:
        client = mqtt.Client()
        client.on_message = mqtt_on_message
        client.connect(MQTT_BROKER, MQTT_PORT)
        # Subscribe with QoS 1 to ensure reliable delivery.
        client.subscribe(MQTT_TOPIC, qos=1)
        logger.info("Connected to MQTT broker successfully.")
        return client
    except Exception as e:
        logger.error(f"Failed to connect to MQTT broker: {e}")
        raise

def message_worker(collection):
    while True:
        payload = message_queue.get()
        if payload is None:
            break
        parse_and_store_buffer(payload, collection)
        message_queue.task_done()

def main():
    collection = setup_mongodb()
    mqtt_client = setup_mqtt_client()

    worker_thread = threading.Thread(target=message_worker, args=(collection,), daemon=True)
    worker_thread.start()

    logger.info("Starting MQTT subscriber loop...")
    try:
        mqtt_client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Subscriber stopped.")
    finally:
        message_queue.put(None)
        worker_thread.join()

if __name__ == "__main__":
    main()
