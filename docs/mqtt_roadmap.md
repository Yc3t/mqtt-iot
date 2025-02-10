# Roadmap de Aprendizaje MQTT

## 1. Fundamentos MQTT con MQTT Explorer (2-3 días)
- Instalación de MQTT Explorer
- Conexión a broker público (ej: test.mosquitto.org)
- Pruebas básicas de publicación/suscripción
- Entender conceptos: topics, QoS, retain messages
- **[Screenshot 1: Interfaz MQTT Explorer mostrando conexión exitosa]**
- **[Screenshot 2: Ejemplo de publicación/suscripción en MQTT Explorer]**

## 2. Mosquitto Local (3-4 días)
### 2.1 Instalación y Configuración
- Instalación de Mosquitto Broker
- Configuración básica de seguridad
- Pruebas de conectividad local
- **[Screenshot 3: Terminal mostrando Mosquitto corriendo]**

### 2.2 Prototipo Python con Paho-MQTT
- Desarrollo de publisher básico
- Desarrollo de subscriber básico
- Implementación de callbacks
- Manejo de mensajes JSON
- **[Screenshot 4: Ejecución de scripts publisher/subscriber]**

## 3. EMQX Broker (4-5 días)
### 3.1 Configuración EMQX
- Instalación de EMQX
- Configuración de autenticación
- Gestión de topics y ACLs
- **[Screenshot 5: Dashboard EMQX]**

### 3.2 Integración con MongoDB
- Configuración de la conexión EMQX-MongoDB
- Reglas de transformación de datos
- Verificación de almacenamiento
- **[Screenshot 6: Reglas EMQX para MongoDB]**

## 4. Implementación del Prototipo Reader BLE (5-7 días)
### 4.1 Simulador Nordic
- Desarrollo del emulador de datos BLE
- Implementación de patrones de error
- Pruebas de secuencia y pérdida de datos
- **[Screenshot 7: Logs del simulador]**

### 4.2 Sistema Completo
- Integración Publisher-Subscriber
- Implementación de mensajes de estado (heartbeat)
- Almacenamiento en MongoDB
- Pruebas de sistema completo
- **[Screenshot 8: Sistema completo en funcionamiento]**

## Notas de Implementación
- Crear directorio `docs/images/` para screenshots
- Nombrar screenshots siguiendo el patrón: `mqtt_screenshot_XX.png`
- Documentar problemas encontrados y soluciones
- Mantener registro de configuraciones importantes

## Recursos Adicionales
- Documentación oficial MQTT: [mqtt.org](https://mqtt.org)
- Documentación Paho-MQTT: [Eclipse Paho](https://www.eclipse.org/paho/)
- Guía EMQX: [EMQX Docs](https://www.emqx.io/docs)
- Tutorial Mosquitto: [Mosquitto Documentation](https://mosquitto.org/documentation/) 