from collections.abc import KeysView
import paho.mqtt.client as mqtt
import time
import random
import json

def on_connect(client,userdata,flags,rc,properties):
    print("Connected: " + str(rc))



# create client

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.on_connect = on_connect

client.connect("localhost", 1883,60)
client.loop_start()


try:
    while True:
        temp_data = {
            "value": round(random.uniform(20,30),2),
            "unit": "C",
            "timestamp":time.time()
        }

        humid_data = {
            "value" : round(random.uniform(20,30),2),
            "unit": "%",
            "timestamp": time.time()
        }


        client.publish("test/temperature", json.dumps(temp_data))
        client.publish("test/humidity", json.dumps(temp_data))

        print(f"Published: Temp={temp_data['value']}°C, Humidity={humid_data['value']}%")
        time.sleep(2)



except KeyboardInterrupt:
    print ("\n Stopping publisher...")
    client.loop_stop()
    client.disconnect()



