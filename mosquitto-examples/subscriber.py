import paho.mqtt.client as mqtt
import json
import time

def on_connect(client,userdata,flags,rc,properties):
    print ("Connected!. Result code: " +str(rc))
    # Subscribe to topics
    client.subscribe("test/temperature")
    client.subscribe("test/humidity")
    

def on_message(client,userdata,msg):
    try:
        #parse as json
        data = json.loads(msg.payload.decode())
        print(f"Topic: {msg.topic}")
        print(f"Message: {data}")
    except:
        #if not json, just print the raw!
        print(f"Topic: {msg.topic}")
        print(f"Message: {msg.payload.decode()}")

try:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

#Pass the function reference
    client.on_connect = on_connect 
    client.on_message = on_message

# Connect to broker
    client.connect("localhost",1883,60)

    print("Subscriber is listening, he's subscribed to test/temperature and test/humidity")
    client.loop_start()


    # Keep the main thread alive

    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nDisconnecting from broker")
    client.disconnect()
    client.loop_stop()
   
