import serial
import struct
from datetime import datetime

class UARTReceiver:
    # UART Protocol Constants - Matching C definitions
    HEADER_MAGIC = b'\x55\x55\x55\x55'
    HEADER_LENGTH = 8  # 4 (magic) + 1 (sequence) + 2 (n_adv_raw) + 1 (n_mac)
    DEVICE_LENGTH = 42  # 6 + 1 + 1 + 1 + 1 + 31 + 1 = 42 bytes
    MAX_DEVICES = 50  # Match MAX_DEVICES from C code
    HASH_SIZE = 64    # Match HASH_SIZE from C code

    def __init__(self, port='/dev/ttyUSB0', baudrate=115200):
        """Initialize UART receiver"""
        self.serial = serial.Serial(
            port=port,
            baudrate=baudrate,
            timeout=0  # Non-blocking reads
        )
        self.sequence = 0

    def _parse_header(self, data):
        """Parse buffer header"""
        try:
            if len(data) != self.HEADER_LENGTH:
                return None
            
            # Verify magic header
            if data[:4] != self.HEADER_MAGIC:
                return None

            # Parse fields
            sequence = data[4]  # 1 byte
            n_adv_raw = int.from_bytes(data[5:7], byteorder='little')  # 2 bytes
            n_mac = data[7]  # 1 byte
            
            return {
                'sequence': sequence,
                'n_adv_raw': n_adv_raw,
                'n_mac': n_mac
            }
        except Exception as e:
            print(f"Error parsing header: {e}")
            return None

    def _parse_device(self, data):
        """Parse device data"""
        try:
            if len(data) != self.DEVICE_LENGTH:
                return None
                
            device = {}
            
            # MAC address (6 bytes)
            device['mac'] = ':'.join(f'{b:02X}' for b in data[0:6])
            
            # Address type (1 byte)
            device['addr_type'] = data[6]
            
            # Advertisement type (1 byte)
            device['adv_type'] = data[7]
            
            # RSSI (1 byte, signed)
            rssi_byte = data[8]
            device['rssi'] = -(256 - rssi_byte) if rssi_byte > 127 else -rssi_byte
            
            # Data length (1 byte)
            device['data_len'] = data[9]
            
            # Advertisement data (31 bytes)
            device['data'] = data[10:41]
            
            # Number of advertisements (1 byte)
            device['n_adv'] = data[41]
            
            return device

        except Exception as e:
            print(f"Error parsing device data: {e}")
            return None

    def _check_sequence(self, received_seq):
        """Verify message sequence"""
        expected = (self.sequence + 1) % 256
        if received_seq != expected:
            print(f"Sequence mismatch! Expected: {expected}, Received: {received_seq}")
        self.sequence = received_seq

    def receive_buffer(self):
        """Receive a single complete buffer"""
        try:
            # Read with non-blocking
            if not self.serial.in_waiting:
                return None, None

            # Look for header magic
            while self.serial.in_waiting >= 4:
                if self.serial.read() == b'\x55':
                    potential_header = b'\x55' + self.serial.read(3)
                    if potential_header == self.HEADER_MAGIC:
                        # Read rest of header
                        header_rest = self.serial.read(4)  # sequence + n_adv_raw + n_mac
                        if len(header_rest) != 4:
                            continue
                        
                        header = self._parse_header(potential_header + header_rest)
                        if not header:
                            continue

                        # Read devices
                        devices = []
                        for _ in range(header['n_mac']):
                            device_data = self.serial.read(self.DEVICE_LENGTH)
                            if len(device_data) != self.DEVICE_LENGTH:
                                return None, None
                            
                            device = self._parse_device(device_data)
                            if device:
                                devices.append(device)
                        
                        return header, devices

            return None, None

        except Exception as e:
            print(f"Error receiving buffer: {e}")
            return None, None

    def close(self):
        """Close serial connection"""
        if self.serial.is_open:
            self.serial.close()

if __name__ == "__main__":
    try:
        receiver = UARTReceiver(port='/dev/ttyUSB0')  
        header, devices = receiver.receive_buffer()
        if header and devices:
            print("\n=== Buffer Received ===")
            print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
            print(f"Sequence: {header['sequence']}")
            print(f"Total Advertisements: {header['n_adv_raw']}")
            print(f"Number of MACs: {header['n_mac']}")
            print("====================\n")

            for i, device in enumerate(devices):
                print(f"Device {i+1}:")
                print(f"  MAC: {device['mac']}")
                print(f"  RSSI: {device['rssi']} dBm")
                print(f"  Advertisements: {device['n_adv']}")
                print("--------------------")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        receiver.close()