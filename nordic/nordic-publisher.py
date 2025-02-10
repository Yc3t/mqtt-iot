import time
import paho.mqtt.client as mqtt
import logging
import argparse
import random
import threading
import queue
from nordic_sim_2 import BLESimulator

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# MQTT Broker configuration
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "ble/scanner/data/raw"

# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Nordic SoC BLE Scanner Publisher")
    parser.add_argument(
        "--scan-time",
        type=int,
        default=7000,
        help="Scan time interval in milliseconds (default: 7000 ms)",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=1024,
        help="Buffer size in bytes (default: 1024 bytes)",
    )
    parser.add_argument(
        "--max-devices",
        type=int,
        default=50,
        help="Maximum number of devices per buffer (default: 50)",
    )
    return parser.parse_args()

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Connected to MQTT broker successfully")
    else:
        logger.error(f"MQTT connection failed with code {rc}")

def on_publish(client, userdata, mid):
    logger.debug(f"Message {mid} published.")

# MQTT Client setup
def setup_mqtt_client():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_publish = on_publish
    client.connect(MQTT_BROKER, MQTT_PORT)
    client.loop_start()
    return client

def publisher_worker(simulator, mqtt_client, publish_queue):
    """Worker thread to publish buffers from the publish_queue."""
    while True:
        buffer = publish_queue.get()
        if buffer is None:
            break

        # Publish with QoS 1
        result = mqtt_client.publish(MQTT_TOPIC, buffer, qos=1)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.error(f"Failed to publish message: {result.rc}")
        else:
            logger.info(f"Published buffer to {MQTT_TOPIC}")
        # Reset buffer after publishing.
        simulator.reset_buffer()
        publish_queue.task_done()

# Main function
if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Starting simulation with scan_time={args.scan_time} ms, buffer_size={args.buffer_size} bytes, max_devices={args.max_devices}")

    # Initialize MQTT client
    mqtt_client = setup_mqtt_client()

    # Initialize BLESimulator
    simulator = BLESimulator(
        scan_time_ms=args.scan_time,
        buffer_size=args.buffer_size,
        max_devices=args.max_devices,
    )

    # Set up a queue to decouple simulation buffer creation from publishing.
    publish_queue = queue.Queue()
    pub_thread = threading.Thread(target=publisher_worker, args=(simulator, mqtt_client, publish_queue), daemon=True)
    pub_thread.start()

    # Simulate Nordic SoC and publish data to MQTT
    try:
        while True:
            if not simulator.buffer_active:
                logger.info("Buffer is inactive. Skipping iteration.")
                time.sleep(simulator.scan_time_ms / 1000)
                continue

            # Generate random number of devices (1 to max_devices)
            num_devices = random.randint(1, simulator.max_devices)
            logger.info(f"Generating buffer with {num_devices} devices...")

            # Create buffer
            buffer = simulator.create_buffer(num_devices)

            # Print buffer info
            simulator.print_buffer_info(buffer)

            # Enqueue buffer for publishing
            publish_queue.put(buffer)

            # Wait for the next sampling interval
            time.sleep(simulator.scan_time_ms / 1000)

    except KeyboardInterrupt:
        logger.info("\nSimulation stopped.")
    finally:
        publish_queue.put(None)
        pub_thread.join()
        mqtt_client.loop_stop()
        mqtt_client.disconnect()
