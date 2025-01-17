import random
import time
import struct
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import binascii
from nordic_simulator import BLESimulator

class MQTTHandler:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_publish = self.on_publish
        
        # Connect to broker
        try:
            self.client.connect("localhost", 1883, 60)
            self.client.loop_start()
        except Exception as e:
            print(f"Failed to connect to MQTT broker: {e}")
    
    def on_connect(self, client, userdata, flags, rc,properties):
        if rc == 0:
            print("Connected to MQTT broker")
        else:
            print(f"Failed to connect to MQTT broker with code: {rc}")
    
    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published successfully")
    
    def publish_data(self, buffer):
        # Convert binary buffer to JSON format
        data = self.parse_buffer_to_json(buffer)
        
        # Publish raw BLE data
        self.client.publish("raw-adv-ble", json.dumps(data))
        
        # Publish reader status every 5 minutes
        if int(time.time()) % 300 < 5:  # Every 5 minutes
            status = {
                "timestamp": datetime.now().isoformat(),
                "status": "active",
                "reader_id": "simulator_001"
            }
            self.client.publish("admin-reader", json.dumps(status))
    
    def parse_buffer_to_json(self, buffer):
        # Parse header
        header = struct.unpack('<4sBBHBL', buffer[:13])
        
        # Parse devices
        devices = []
        device_data = buffer[13:]
        num_devices = header[4]
        
        for i in range(num_devices):
            start = i * 42
            device = struct.unpack('<6sBBbB31sB', device_data[start:start+42])
            
            device_json = {
                "mac": binascii.hexlify(device[0]).decode('ascii'),
                "addr_type": device[1],
                "adv_type": device[2],
                "rssi": device[3],
                "data_length": device[4],
                "data": binascii.hexlify(device[5]).decode('ascii'),
                "n_adv": device[6]
            }
            devices.append(device_json)
        
        return {
            "timestamp": datetime.now().isoformat(),
            "header": {
                "magic": binascii.hexlify(header[0]).decode('ascii'),
                "message_type": header[1],
                "sequence": header[2],
                "total_events": header[3],
                "unique_macs": header[4],
                "timestamp": header[5]
            },
            "devices": devices
        }

def main():
    simulator = BLESimulator()
    mqtt_handler = MQTTHandler()
    
    try:
        while True:
            # Generate random number of devices (1-10)
            num_devices = random.randint(1, 10)
            
            # Create buffer
            buffer = simulator.create_buffer(num_devices)
            
            # Print buffer information (optional, for debugging)
            simulator.print_buffer_info(buffer)
            
            # Publish to MQTT
            mqtt_handler.publish_data(buffer)
            
            # Wait 5 seconds
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nSimulation stopped by user")
    finally:
        mqtt_handler.client.loop_stop()
        mqtt_handler.client.disconnect()

if __name__ == "__main__":
    main()
