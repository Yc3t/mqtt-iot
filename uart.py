import serial
import struct
from datetime import datetime

class UARTReceiver:
    def __init__(self, port='COM21', baudrate=115200):
        """Initialize UART receiver with updated buffer format"""
        self.serial = serial.Serial(port, baudrate)
        self.sequence = 0
        
        # Header format constants
        self.HEADER_MAGIC = b'\x55\x55\x55\x55'
        self.HEADER_FORMAT = {
            'header': 4,       # Magic bytes
            'sequence': 1,     # Sequence number
            'n_adv_raw': 2,    # Total advertisements counter (now uint16_t)
            'n_mac': 2,        # Number of unique MACs (now uint16_t)
        }
        
        # Device data format
        self.DEVICE_FORMAT = {
            'mac': 6,         # MAC address
            'addr_type': 1,   # Address type
            'adv_type': 1,    # Advertisement type
            'rssi': 1,        # RSSI value
            'data_len': 1,    # Data length
            'data': 31,       # Advertisement data
            'n_adv': 1,       # Number of advertisements from this MAC
        }
        
        self.HEADER_LENGTH = sum(self.HEADER_FORMAT.values())  # Now 9 bytes
        self.DEVICE_LENGTH = sum(self.DEVICE_FORMAT.values())  # Still 42 bytes
        self.MAX_DEVICES = 1024  # Updated to match main.c

    def _check_header(self, data):
        """Verify message header"""
        return data[:4] == self.HEADER_MAGIC

    def _parse_header(self, data):
        """Parse buffer header with new format"""
        try:
            offset = 0
            header = {}
            
            # Verify magic header
            if not self._check_header(data):
                return None
            offset += 4

            # Parse sequence number (1 byte)
            header['sequence'] = data[offset]
            offset += 1

            # Parse n_adv_raw (2 bytes, uint16_t)
            header['n_adv_raw'] = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2

            # Parse n_mac (2 bytes, uint16_t)
            header['n_mac'] = struct.unpack('<H', data[offset:offset+2])[0]
            
            return header

        except Exception as e:
            print(f"Error parsing header: {e}")
            return None

    def _parse_device(self, data):
        """Parse device data (format unchanged but validation updated)"""
        try:
            if len(data) != self.DEVICE_LENGTH:
                print(f"Invalid device data length: {len(data)} != {self.DEVICE_LENGTH}")
                return None
                
            offset = 0
            device = {}
            
            # Parse MAC address
            device['mac'] = ':'.join(f'{b:02X}' for b in data[offset:offset+6])
            offset += 6

            # Parse address type
            device['addr_type'] = data[offset]
            offset += 1

            # Parse advertisement type
            device['adv_type'] = data[offset]
            offset += 1

            # Parse RSSI (signed byte)
            rssi_byte = data[offset]
            device['rssi'] = -(256 - rssi_byte) if rssi_byte > 127 else -rssi_byte
            offset += 1

            # Parse data length
            device['data_len'] = data[offset]
            offset += 1

            # Parse advertisement data
            device['data'] = data[offset:offset+31]
            offset += 31

            # Parse number of advertisements
            device['n_adv'] = data[offset]
            
            return device

        except Exception as e:
            print(f"Error parsing device data: {e}")
            return None

    def _check_sequence(self, received_seq):
        """Verify message sequence with improved logging"""
        if received_seq != (self.sequence + 1) % 256:
            print(f"Sequence mismatch! Expected: {(self.sequence + 1) % 256}, "
                  f"Received: {received_seq}")
        self.sequence = received_seq

    def receive_messages(self, duration=None):
        """Receive and process messages with support for larger buffers"""
        print("Starting message reception...")
        start_time = datetime.now()
        
        while True:
            try:
                # Check duration if specified
                if duration and (datetime.now() - start_time).total_seconds() >= duration:
                    print(f"Duration {duration}s reached. Stopping.")
                    break

                # Search for header
                while True:
                    if self.serial.read() == b'\x55':
                        potential_header = b'\x55' + self.serial.read(3)
                        if potential_header == self.HEADER_MAGIC:
                            break

                # Read and parse header
                header_data = potential_header + self.serial.read(self.HEADER_LENGTH - 4)
                header = self._parse_header(header_data)
                
                if not header:
                    continue

                print("\n=== Buffer Received ===")
                print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                print(f"Sequence: {header['sequence']}")
                print(f"Total Advertisements: {header['n_adv_raw']}")
                print(f"Number of MACs: {header['n_mac']}")
                print("====================\n")

                # Validate n_mac
                if header['n_mac'] > self.MAX_DEVICES:
                    print(f"Warning: n_mac ({header['n_mac']}) exceeds MAX_DEVICES ({self.MAX_DEVICES})")
                    continue

                # Read and parse each device
                for i in range(header['n_mac']):
                    device_data = self.serial.read(self.DEVICE_LENGTH)
                    device = self._parse_device(device_data)
                    
                    if device:
                        print(f"Device {i+1}:")
                        print(f"  MAC: {device['mac']}")
                        print(f"  RSSI: {device['rssi']} dBm")
                        print(f"  Advertisements: {device['n_adv']}")
                        print("--------------------")

            except serial.SerialException as e:
                print(f"Serial communication error: {e}")
                break
            except KeyboardInterrupt:
                print("\nReception interrupted by user")
                break
            except Exception as e:
                print(f"Unexpected error: {e}")
                continue

    def close(self):
        """Close serial connection"""
        if self.serial.is_open:
            self.serial.close()

if __name__ == "__main__":
    try:
        receiver = UARTReceiver(port='COM21')  
        receiver.receive_messages()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        receiver.close()