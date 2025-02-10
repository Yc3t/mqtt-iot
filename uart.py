import serial
import struct
from datetime import datetime

class UARTReceiver:
    def __init__(self, port='COM21', baudrate=115200):
        """Inicializa el receptor UART"""
        self.serial = serial.Serial(port, baudrate)
        self.sequence = 0
        
        # Formato del buffer header
        self.HEADER_MAGIC = b'\x55\x55\x55\x55'
        self.HEADER_FORMAT = {
            'header': 4,       # Magic bytes
            'sequence': 1,     # Sequence number
            'n_adv_raw': 2,    # Total advertisements counter
            'n_mac': 1,        # Number of unique MACs
        }
        
        # Formato de cada device_data
        self.DEVICE_FORMAT = {
            'mac': 6,         # MAC address
            'addr_type': 1,   # Address type
            'adv_type': 1,    # Advertisement type
            'rssi': 1,        # RSSI value
            'data_len': 1,    # Data length
            'data': 31,       # Advertisement data
            'n_adv': 1,       # Number of advertisements from this MAC
        }
        
        self.HEADER_LENGTH = sum(self.HEADER_FORMAT.values())
        self.DEVICE_LENGTH = sum(self.DEVICE_FORMAT.values())

    def _check_header(self, data):
        """Verifica la cabecera del mensaje"""
        return data[:4] == self.HEADER_MAGIC

    def _parse_header(self, data):
        """Parsea la cabecera del buffer"""
        try:
            offset = 0
            header = {}
            
            # Verifica cabecera mágica
            if not self._check_header(data):
                return None
            offset += 4

            header['sequence'] = data[offset]
            offset += 1

            header['n_adv_raw'] = struct.unpack('<H', data[offset:offset+2])[0]
            offset += 2

            header['n_mac'] = data[offset]
            
            return header

        except Exception as e:
            print(f"Error parsing header: {e}")
            return None

    def _parse_device(self, data):
        """Parsea los datos de un dispositivo"""
        try:
            offset = 0
            device = {}
            
            device['mac'] = ':'.join(f'{b:02X}' for b in data[offset:offset+6])
            offset += 6

            device['addr_type'] = data[offset]
            offset += 1

            device['adv_type'] = data[offset]
            offset += 1

            rssi_byte = data[offset]
            device['rssi'] = -(256 - rssi_byte) if rssi_byte > 127 else -rssi_byte
            offset += 1

            device['data_len'] = data[offset]
            offset += 1

            device['data'] = data[offset:offset+31]
            offset += 31

            device['n_adv'] = data[offset]
            
            return device

        except Exception as e:
            print(f"Error parsing device data: {e}")
            return None

    def _check_sequence(self, received_seq):
        """Verifica la secuencia del mensaje"""
        if received_seq != (self.sequence + 1) % 256:
            print(f"¡Pérdida de mensaje! Esperado: {(self.sequence + 1) % 256}, Recibido: {received_seq}")
        self.sequence = received_seq

    def receive_messages(self):
        """Recibe y procesa mensajes continuamente"""
        print("Iniciando recepción de mensajes...")
        
        while True:
            try:
                # Busca la cabecera
                while True:
                    if self.serial.read() == b'\x55':
                        potential_header = b'\x55' + self.serial.read(3)
                        if potential_header == self.HEADER_MAGIC:
                            break

                # Lee y parsea la cabecera
                header_data = potential_header + self.serial.read(self.HEADER_LENGTH - 4)
                header = self._parse_header(header_data)
                
                if not header:
                    continue

                print("\n=== Buffer Recibido ===")
                print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]}")
                print(f"Secuencia: {header['sequence']}")
                print(f"Total Advertisements: {header['n_adv_raw']}")
                print(f"Número de MACs: {header['n_mac']}")
                print("====================\n")

                # Lee y parsea cada dispositivo
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
        receiver = UARTReceiver(port='COM21')  
        receiver.receive_messages()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        receiver.close()