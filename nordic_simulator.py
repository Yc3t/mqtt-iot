import random
import time
import struct


class BLESimulator:
    UART_HEADER_MAGIC = b"\x55\x55\x55\x55"

    def __init__(self):
        self.sequence_number = 0
        self.n_adv_raw = 0
        self.devices = []

    def generate_random_mac(self):
        return [random.randint(0, 255) for _ in range(6)]

    def generate_random_adv_data(self, length=31):
        return [random.randint(0, 255) for _ in range(length)]

    def generate_device_data(self):
        mac = self.generate_random_mac()
        adv_data = self.generate_random_adv_data()

        device = {
            "mac": mac,
            "addr_type": random.randint(0, 1),
            "adv_type": random.randint(0, 3),
            "rssi": random.randint(-100, -30),
            "data_length": 31,
            "data": adv_data,
            "n_adv": random.randint(1, 10),
        }

        return device

    def create_buffer(self, num_devices=5):
        # Generate header
        self.sequence_number = (self.sequence_number + 1) % 256
        self.n_adv_raw += sum(device["n_adv"] for device in self.devices)
        timestamp = int(time.time())

        # Create devices
        self.devices = [self.generate_device_data() for _ in range(num_devices)]

        # Pack header
        header = struct.pack(
            "<4sBBHBL",
            self.UART_HEADER_MAGIC,  # 4 bytes
            0x01,  # Message Type
            self.sequence_number,  # Sequence Number
            self.n_adv_raw,  # Total reception events
            len(self.devices),  # Number of unique MACs
            timestamp,  # Timestamp
        )

        # Pack device data

        device_data = b""
        for device in self.devices:
            device_data += struct.pack(
                "<6sBBbB31sB",
                bytes(device["mac"]),  # MAC Address (6 bytes)
                device["addr_type"],  # Address Type (1 byte)
                device["adv_type"],  # Advertisement Type (1 byte)
                device["rssi"],  # RSSI (1 byte)
                device["data_length"],  # Data Length (1 byte)
                bytes(device["data"]),  # Advertisement Data (31 bytes)
                device["n_adv"],  # Number of advertisements
            )

        return header + device_data



    def print_buffer_info(self,buffer):
        # Print header information
        header = struct.unpack('<4sBBHBL', buffer[:13]) #0-13
        print("\nBuffer Header:")
        print(f"Magic: {header[0].hex()}")
        print(f"Message Type: {header[1]}")
        print(f"Sequence Number :{header[2]}")
        print(f"Total Events: {header[3]}")
        print(f"Unique MACs : {header[4]}")
        print(f"Timestamp: {header[5]}")

        # Device info
        device_data = buffer[13:]
        num_devices = header[4]

        print("\n Devices:")
        for i in range(num_devices):
            start = i*42
            device = struct.unpack('<6sBBbB31sB', device_data[start:start+42])
            print(f"\n Device: {i+1}")
            print(f"MAC: {device[0].hex(':')}")
            print(f"Address type: {device[1]}")
            print(f"Advertisement type: {device[2]}")
            print(f"RSSI: {device[3]}")
            print(f"Data Length: {device[4]}")
            print(f"N_Adv:{device[6]}")



def simulate_nordic():
    simulator = BLESimulator()

    while True:
        #Generate random number of devices (1-10)

        num_devices = random.randint(1,10)
        # Create buffer

        buffer = simulator.create_buffer(num_devices)

        # Print Buffer info

        simulator.print_buffer_info(buffer)
        
        time.sleep(5)


try:
    simulate_nordic()
except KeyboardInterrupt:
    print("\n Simulation stopped")

