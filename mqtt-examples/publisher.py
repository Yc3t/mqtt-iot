from collections.abc import KeysView
import paho.mqtt.client as mqtt
import time
import random
import json

# Callback que se ejecuta cuando el cliente se conecta al broker
def on_connect(client, userdata, flags, rc, properties):
    print("Conectado con código de resultado: " + str(rc))
    # rc = 0: Conexión exitosa
    # rc > 0: Varios tipos de error en la conexión

# Crear instancia del cliente MQTT
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

# Asignar la función callback para el evento de conexión
client.on_connect = on_connect


# Conectar al broker local
# Parámetros: (host, puerto, keepalive en segundos)
client.connect("localhost", 1883, 60)

# Iniciar el loop de eventos en segundo plano
client.loop_start()

def get_test_data():
    """Genera datos de prueba fijos para testing"""
    temp_data = {
        "value": 25.50,    # Temperatura fija para testing
        "unit": "C",
        "timestamp": int(time.time())
    }
    
    humid_data = {
        "value": 45.00,    # Humedad fija para testing
        "unit": "%",
        "timestamp": int(time.time())
    }
    
    return temp_data, humid_data

try:
    sequence = 0  # Contador de secuencia para testing
    while True:
        temp_data, humid_data = get_test_data()
        
        # Agregar número de secuencia para tracking
        temp_data["sequence"] = sequence
        humid_data["sequence"] = sequence
        
        client.publish("test/temperature", json.dumps(temp_data), retain=True)
        client.publish("test/humidity", json.dumps(humid_data), retain=True)
        
        print(f"[Seq:{sequence}] Publicado: Temp={temp_data['value']}°C, Humedad={humid_data['value']}%")
        sequence = (sequence + 1) % 256  # Simular byte de secuencia
        
        time.sleep(2)

except KeyboardInterrupt:
    print("\nDeteniendo el publicador...")
    
    # Detener el loop de eventos
    client.loop_stop()
    
    # Desconectar del broker
    client.disconnect()



