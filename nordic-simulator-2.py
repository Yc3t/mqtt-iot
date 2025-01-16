import random
import time
import struct
import logging
import argparse
from enum import Enum

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
UART_HEADER_MAGIC = b"\x55\x55\x55\x55"
DEVICE_DATA_SIZE = 42  # Size of each device entry in bytes

# Enum for advertisement types
class AdvType(Enum):
    CONNECTABLE = 0
    NON_CONNECTABLE = 1
    SCANNABLE = 2
    DIRECTED = 3

class BLESimulator:
    def __init__(self, scan_time_ms, buffer_size, max_devices):
        self.sequence_number = 0
        self.n_adv_raw = 0
        self.devices = []
        self.buffer_active = True
        self.scan_time_ms = scan_time_ms
        self.buffer_size = buffer_size
        self.max_devices = max_devices

    def generate_random_mac(self):
        """Generate a random MAC address."""
        return [random.randint(0, 255) for _ in range(6)]

    def generate_random_adv_data(self, length=31):
        """Generate random advertisement data."""
        return [random.randint(0, 255) for _ in range(length)]

    def generate_device_data(self):
        """Generate random device data."""
        mac = self.generate_random_mac()
        adv_data = self.generate_random_adv_data()

        device = {
            "mac": mac,
            "addr_type": random.randint(0, 1),  # 0: Public, 1: Random
            "adv_type": random.choice(list(AdvType)).value,  # Random advertisement type
            "rssi": random.randint(-100, -30),  # Random RSSI value
            "data_length": 31,  # Fixed length for advertisement data
            "data": adv_data,  # Random advertisement data
            "n_adv": random.randint(1, 10),  # Number of advertisements
        }

        return device

    def create_buffer(self, num_devices=5):
        """Create a buffer with the specified number of devices."""
        if not self.buffer_active:
            return None

        # Generate header
        self.sequence_number = (self.sequence_number + 1) % 256
        self.n_adv_raw += sum(device["n_adv"] for device in self.devices)

        # Create devices
        self.devices = [self.generate_device_data() for _ in range(num_devices)]

        # Pack header
        header = struct.pack(
            "<4sBBHB",
            UART_HEADER_MAGIC,  # 4 bytes: Magic header
            0x01,  # 1 byte: Message type (advertisement data)
            self.sequence_number,  # 1 byte: Sequence number
            self.n_adv_raw,  # 2 bytes: Total reception events
            len(self.devices),  # 1 byte: Number of unique MACs
        )

        # Pack device data
        device_data = b""
        for device in self.devices:
            device_data += struct.pack(
                "<6sBBbB31sB",
                bytes(device["mac"]),  # 6 bytes: MAC address
                device["addr_type"],  # 1 byte: Address type
                device["adv_type"],  # 1 byte: Advertisement type
                device["rssi"],  # 1 byte: RSSI
                device["data_length"],  # 1 byte: Data length
                bytes(device["data"]),  # 31 bytes: Advertisement data
                device["n_adv"],  # 1 byte: Number of advertisements
            )

        return header + device_data

    def print_buffer_info(self, buffer):
        """Print detailed information about the buffer."""
        if not buffer:
            logger.warning("Buffer is empty or inactive.")
            return

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

        logger.info("\nDevices:")
        for i in range(num_devices):
            start = i * DEVICE_DATA_SIZE
            device = struct.unpack("<6sBBbB31sB", device_data[start:start + DEVICE_DATA_SIZE])
            logger.info(f"\nDevice {i + 1}:")
            logger.info(f"MAC: {device[0].hex(':')}")
            logger.info(f"Address Type: {device[1]}")
            logger.info(f"Advertisement Type: {AdvType(device[2]).name}")
            logger.info(f"RSSI: {device[3]} dBm")
            logger.info(f"Data Length: {device[4]}")
            logger.info(f"Advertisement Data: {device[5].hex()}")
            logger.info(f"N_Adv: {device[6]}")

    def reset_buffer(self):
        """Reset the buffer and clear all devices."""
        self.sequence_number = 0
        self.n_adv_raw = 0
        self.devices = []
        logger.info("Buffer reset.")

    def simulate_nordic(self):
        """Simulate the Nordic SoC behavior."""
        try:
            while True:
                if not self.buffer_active:
                    logger.info("Buffer is inactive. Skipping iteration.")
                    time.sleep(self.scan_time_ms / 1000)
                    continue

                # Generate random number of devices (1 to max_devices)
                num_devices = random.randint(1, self.max_devices)
                logger.info(f"Generating buffer with {num_devices} devices...")

                # Create buffer
                buffer = self.create_buffer(num_devices)

                # Print buffer info
                self.print_buffer_info(buffer)

                # Simulate sending buffer via UART
                logger.info("Sending buffer via UART...")

                # Reset buffer after sending
                self.reset_buffer()

                # Wait for the next sampling interval
                time.sleep(self.scan_time_ms / 1000)

        except KeyboardInterrupt:
            logger.info("\nSimulation stopped.")


# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Nordic SoC BLE Scanner Simulator")
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


# Main function
if __name__ == "__main__":
    args = parse_arguments()
    logger.info(f"Starting simulation with scan_time={args.scan_time} ms, buffer_size={args.buffer_size} bytes, max_devices={args.max_devices}")

    simulator = BLESimulator(
        scan_time_ms=args.scan_time,
        buffer_size=args.buffer_size,
        max_devices=args.max_devices,
    )
    simulator.simulate_nordic()
