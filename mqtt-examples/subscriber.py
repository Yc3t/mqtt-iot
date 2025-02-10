import paho.mqtt.client as mqtt
import json
import time

# Callback que se ejecuta cuando el cliente se conecta exitosamente al broker
def on_connect(client,userdata,flags,rc,properties):
    print("¡Conectado! Código de resultado: " +str(rc))
    # Suscribirse a los topics de interés
    client.subscribe("test/temperature")  # Topic para temperatura
    client.subscribe("test/humidity")     # Topic para humedad
    
# Callback que se ejecuta cuando se recibe un mensaje en algún topic suscrito
def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        
        # Verificar secuencia
        if hasattr(on_message, 'last_sequence'):
            expected_sequence = (on_message.last_sequence + 1) % 256
            if data['sequence'] != expected_sequence:
                print(f"⚠️ Error de secuencia: esperado {expected_sequence}, recibido {data['sequence']}")
        
        on_message.last_sequence = data['sequence']
        
        # Verificar valores dentro de rangos esperados
        if msg.topic == "test/temperature":
            if not (20 <= data['value'] <= 30):
                print(f"⚠️ Temperatura fuera de rango: {data['value']}")
        
        print(f"Topic: {msg.topic}")
        print(f"Mensaje: {data}")
        
    except json.JSONDecodeError:
        print(f"⚠️ Error: JSON inválido en {msg.topic}")
        print(f"Payload: {msg.payload}")
    except Exception as e:
        print(f"⚠️ Error inesperado: {str(e)}")

# Inicializar último número de secuencia
on_message.last_sequence = -1

try:
    # Crear instancia del cliente MQTT 
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    # Asignar las funciones callback
    client.on_connect = on_connect  # Para evento de conexión
    client.on_message = on_message  # Para recepción de mensajes

    # Conectar al broker local
    client.connect("localhost",1883,60)

    print("Suscriptor escuchando, suscrito a test/temperature y test/humidity")
    
    # Iniciar el loop de eventos en segundo plano
    client.loop_start()

    # Mantener el programa principal en ejecución
    while True:
        time.sleep(1)

except KeyboardInterrupt:
    print("\nDesconectando del broker")
    client.disconnect()     
    client.loop_stop()      # Detener el loop de eventos
   
