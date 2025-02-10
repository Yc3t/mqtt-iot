#include <zephyr/kernel.h>
#include <zephyr/bluetooth/bluetooth.h>
#include <zephyr/bluetooth/hci.h>
#include <zephyr/logging/log.h>
#include <zephyr/drivers/uart.h>

LOG_MODULE_REGISTER(ble_scanner, LOG_LEVEL_INF);

#define UART_HEADER_MAGIC      0x55    /* Patrón de sincronización: 01010101 */
#define UART_HEADER_LENGTH     4       /* Longitud de la cabecera */
#define MSG_TYPE_ADV_DATA      0x01    /* Tipo mensaje: datos advertisement */
#define MAX_DEVICES 1024                /* Now allow up to 1024 unique devices */
#define SAMPLING_INTERVAL_MS 7000      /* Interval of sampling: 7 seconds */
#define HASH_SIZE 1024                  /* Increased hash table size to 1024 */
#define HASH_MASK (HASH_SIZE - 1)

/* Estados posibles de cada entrada en la tabla hash */
enum entry_state {
    ENTRY_EMPTY,     // Slot nunca usado
    ENTRY_OCCUPIED,  // Slot con datos válidos
    ENTRY_DELETED    // Slot antes usado pero ahora borrado
};

struct __packed device_data {
    uint8_t addr[6];          /* Dirección MAC */
    uint8_t addr_type;        /* Tipo de dirección */
    uint8_t adv_type;         /* Tipo de advertisement */
    int8_t  rssi;             /* Valor RSSI */
    uint8_t data_len;         /* Longitud de datos */
    uint8_t data[31];         /* Datos del advertisement */
    uint8_t n_adv;            /* Número de advertisements de esta MAC */
};

/* Estructura de la cabecera del buffer */
struct __packed buffer_header {
    uint8_t header[UART_HEADER_LENGTH];    /* [0x55, 0x55, 0x55, 0x55] */
    uint8_t sequence;                      /* Número de secuencia */
    uint16_t n_adv_raw;                    /* Contador eventos de recepción */
    uint16_t n_mac;                         /* Nº MACs únicas en buffer */
};

/* Estructura para cada entrada en la tabla hash */
struct hash_entry {
    enum entry_state state;
    struct device_data device;
};

static struct hash_entry hash_table[HASH_SIZE];
static struct buffer_header buffer_header;
static bool buffer_active = false;
static const struct device *uart_dev;
static uint8_t msg_sequence = 0;
static uint16_t hash_entries = 0;

/* Calcular índice a partir de MAC */
static uint32_t hash_mac(const uint8_t *mac) {
    uint32_t hash = 0;
    for (int i = 0; i < 6; i++) {
        hash = (hash << 5) + hash + mac[i];  // hash * 33 + mac[i]
    }
    return hash & HASH_MASK;
}

/* Buscar o añadir dispositivo en la tabla hash */
static struct device_data *find_or_add_device(const bt_addr_le_t *addr) {
    uint32_t index = hash_mac(addr->a.val);
    uint32_t original_index = index;
    
    do {
        // Dispositivo encontrado
        if (hash_table[index].state == ENTRY_OCCUPIED &&
            memcmp(hash_table[index].device.addr, addr->a.val, 6) == 0) {
            return &hash_table[index].device;
        }
        
        // Slot libre encontrado
        if (hash_table[index].state != ENTRY_OCCUPIED) {
            // Verificar límites
            if (buffer_header.n_mac >= MAX_DEVICES) {
                LOG_WRN("Buffer lleno: n_mac = %d", MAX_DEVICES);
                return NULL;
            }
            
            if (hash_entries >= MAX_DEVICES) {
                LOG_WRN("Buffer lleno: MAX_DEVICES alcanzado (%d)", MAX_DEVICES);
                return NULL;
            }
            
            // Insertar nuevo dispositivo
            hash_table[index].state = ENTRY_OCCUPIED;
            memcpy(hash_table[index].device.addr, addr->a.val, 6);
            hash_table[index].device.n_adv = 0;
            hash_entries++;
            buffer_header.n_mac++;
            return &hash_table[index].device;
        }
        
        // Probar siguiente slot
        index = (index + 1) & HASH_MASK;
    } while (index != original_index);
    
    LOG_WRN("Tabla hash llena");
    return NULL;
}

static void scan_cb(const bt_addr_le_t *addr, int8_t rssi,
                   uint8_t adv_type, struct net_buf_simple *buf)
{
    if (!buffer_active) {
        return;
    }

    buffer_header.n_adv_raw++;

    struct device_data *device = find_or_add_device(addr);
    if (device == NULL) {
        return;
    }

    // Actualizar datos del dispositivo
    device->addr_type = addr->type;
    device->adv_type = adv_type;
    device->rssi = rssi;
    device->data_len = MIN(buf->len, sizeof(device->data));
    
    // Primero limpiamos todo el buffer con ceros
    memset(device->data, 0, sizeof(device->data));
    // Luego copiamos los datos reales
    memcpy(device->data, buf->data, device->data_len);
    
    device->n_adv++;
}

/* Enviar buffer por UART */
static void send_buffer(void) {
    // Enviar cabecera
    const uint8_t *header_data = (const uint8_t *)&buffer_header;
    for (size_t i = 0; i < sizeof(struct buffer_header); i++) {
        uart_poll_out(uart_dev, header_data[i]);
    }

    // Enviar solo entradas ocupadas
    for (size_t i = 0; i < HASH_SIZE; i++) {
        if (hash_table[i].state == ENTRY_OCCUPIED) {
            const uint8_t *device_data = (const uint8_t *)&hash_table[i].device;
            for (size_t j = 0; j < sizeof(struct device_data); j++) {
                uart_poll_out(uart_dev, device_data[j]);
            }
        }
    }
}

/* Reset del buffer */
static void reset_buffer(void) {
    memset(&buffer_header, 0, sizeof(struct buffer_header));
    memset(hash_table, 0, sizeof(hash_table));
    memset(buffer_header.header, UART_HEADER_MAGIC, UART_HEADER_LENGTH);
    buffer_header.sequence = msg_sequence++;
    buffer_header.n_mac = 0;     
    buffer_header.n_adv_raw = 0; 
    hash_entries = 0;
}

/* Manejador del timer de muestreo */
static void sampling_timer_handler(struct k_work *work) {
    buffer_active = false;
    send_buffer();
    reset_buffer();
    buffer_active = true;
}

/* Work item para el timer */
K_WORK_DEFINE(sampling_work, sampling_timer_handler);

static void timer_expiry_function(struct k_timer *timer) {
    k_work_submit(&sampling_work);
}

K_TIMER_DEFINE(sampling_timer, timer_expiry_function, NULL);

/* Inicialización del UART */
static int uart_init(void)
{
    uart_dev = DEVICE_DT_GET(DT_NODELABEL(uart0));
    if (!device_is_ready(uart_dev)) {
        LOG_ERR("UART no está listo");
        return -1;
    }
    return 0;
}

static struct bt_le_scan_param scan_param = {
    .type       = BT_LE_SCAN_TYPE_PASSIVE,
    .options    = BT_LE_SCAN_OPT_NONE,
    .interval   = BT_GAP_ADV_FAST_INT_MIN_2,  // 0x00a0 (100ms)
    .window     = BT_GAP_ADV_FAST_INT_MIN_2   // 0x00a0 (100ms)
};

/* Función principal */
int main(void)
{
    int err;

    // Inicializar UART
    err = uart_init();
    if (err) {
        LOG_ERR("Falló inicialización UART (err %d)", err);
        return err;
    }

    LOG_INF("Iniciando Escáner BLE con buffer hash...");

    // Inicializar Bluetooth
    err = bt_enable(NULL);
    if (err) {
        LOG_ERR("Falló inicialización Bluetooth (err %d)", err);
        return err;
    }

    // Preparar buffer y comenzar escaneo
    reset_buffer();
    buffer_active = true;

    // Iniciar timer de muestreo
    k_timer_start(&sampling_timer, K_MSEC(SAMPLING_INTERVAL_MS), 
                 K_MSEC(SAMPLING_INTERVAL_MS));

    // Comenzar escaneo BLE
    err = bt_le_scan_start(&scan_param, scan_cb);
    if (err) {
        LOG_ERR("Falló inicio de escaneo (err %d)", err);
        return err;
    }

    LOG_INF("Escaneo iniciado exitosamente");

    // Bucle principal
    while (1) {
        k_sleep(K_SECONDS(1));
    }

    return 0;
}
