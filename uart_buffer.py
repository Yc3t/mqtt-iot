import serial
import struct
from datetime import datetime

class UARTReceiver:
    def __init__(self,port="COM21",baudrate=115200):
        self.serial = serial.Serial(port,baudrate)
        self.sequence = 0

        #Buffer structure
        self.HEADER_MAGIC = b'\x55\x55\x55\x55'
        self.HEADER_FORMAT = {
            'header':4, # Magic bytes
            'sequence': 1, # Sequence number
            'n_adv_raw':2, # Total adv counter
            'n_jac': 1  # Number of unique MACs
        }

        # Device structure
        self.DEVICE_FORMAT = {
            'mac': 6,
            'addr_type': 1,
            'adv_type': 1,
            'rssi': 1,
            'data_len': 1,
            'data': 31,
            'n_adv': 1
        }

        self.HEADER_LENGTH = sum(self.HEADER_FORMAT.values())
        self.DEVICE_LENGTH = sum(self.DEVICE_FORMAT.values())


    def _check_header(self,data):
        return data.startswith(self.HEADER_MAGIC)


    def _parse_header(self,data):
        try:
            offset = 0 
            header = {}
            # Check Header MAGIC
            if not self._check_header(data):
                return None
            offset +=4

            header['sequence'] = data[offset]
            offset +=1

            header['n_adv_raw'] = struct.unpack('<H',data[offset:offset+2])[0]
            offset +=2

            header['n_mac'] = data[offset]

            return header

        except Exception as e:
            print(f"Error parsing header: {e}")

    def _parse_device(self,data):

        try:
            offset = 0
            device = {}

            #parse mac -> format each byte as a 2-digit hex

            device['mac'] = ':'.join(f'{b:02X}' for b in data[offset:offset+6])

            offset+=6

            device['addr_type'] = data[offset]
            offset+=1

            device['adv_type'] = data[offset]
            offset+=1

            rssi_byte = data[offset]
            device['rssi'] = -(256 - rssi_byte) if rssi_byte > 127 else -rssi_byte
            offset +=1

            device['data_len'] = data[offset]
            offset+=1

            device['data'] = data[offset:offset+31]
            offset +=31

            device['n_adv'] = data[offset]

            return device

        except Exception as e:
            print(f"Error parsing device data: {e}")
            return None   

    def _check_sequence(self,received_seq):
        if received_seq != (self.sequence + 1) %256:
            print(f"Message lost! Expected :{(self.sequence + 1) % 256}, Received: {received_seq}")
        self.sequence = received_seq


    def received_messages(self):
        print("Starting message reception...")

        while True:
            #Search header
            try:
                while True:
                    if self.serial.read()== b'\x55':
                        potential_header = b'\x55' + self.serial.read(3)
                        if potential_header == self.HEADER_MAGIC:
                            break

                header_data =  potential_header + self.serial.read(self.HEADER_LENGTH-4)
                header = self._parse_header(header_data)


                if not header:
                        continue

                print("\n=== Buffer Recibido ===")
                print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                print(f"Secuencia: {header['sequence']}")
                print(f"Total Advertisements: {header['n_adv_raw']}")
                print(f"Número de MACs: {header['n_mac']}")
                print("====================\n")


                # Parse each device

                for i in range(header['n_mac']):
                    device_data = self.serial.read(self.DEVICE_LENGTH)
                    device = self._parse_device(device_data)


                    if device:
                        print(f"Dispositivo {i+1}:")
                        print(f"  MAC: {device['mac']}")
                        print(f"  RSSI: {device['rssi']} dBm")
                        print(f"  Advertisements: {device['n_adv']}")
                        print("--------------------")
        
            except serial.SerialException as e:
                print(f"Error de comunicación serial: {e}")
                break
            except KeyboardInterrupt:
                print("\nRecepción interrumpida por el usuario")
                break
            except Exception as e:
                print(f"Error inesperado: {e}")
                continue

        
    def close(self):
            """Cierra la conexión serial"""
            if self.serial.is_open:
                self.serial.close()


if __name__ == "__main__":
    try:
        receiver = UARTReceiver(port = 'COM21')
        receiver.receive_messages()


    except Exception as e:
        print(f"Error: {e}") 

    finally:
        receiver.close()


