import random
import time
import struct
import logging
import argparse
from enum import Enum
import tkinter as tk
from tkinter import ttk, scrolledtext
import threading
import queue
from datetime import datetime

# Configure logging with more detailed settings
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # This ensures output goes to console
    ]
)
logger = logging.getLogger(__name__)

# Constants
UART_HEADER_MAGIC = b"\x55\x55\x55\x55"
DEVICE_DATA_SIZE = 42  # Size of each device entry in bytes

# Enum for advertisement types
class AdvType(Enum):
    CONNECTABLE = 0
    NON_CONNECTABLE = 1
    SCANNABLE = 2
    DIRECTED = 3

class ErrorSimulationConfig:
    def __init__(self):
        self.sequence_error_rate = 0
        self.data_corruption_rate = 0
        self.header_error_rate = 0
        self.enable_sequence_errors = False
        self.enable_data_corruption = False
        self.enable_header_errors = False
        self.sequence_jump_range = (1, 5)
        # Add flags for manual error triggers
        self.trigger_sequence_error = False
        self.trigger_corruption_error = False
        self.trigger_header_error = False

    def reset_triggers(self):
        """Reset all manual triggers after they're used"""
        self.trigger_sequence_error = False
        self.trigger_corruption_error = False
        self.trigger_header_error = False

class BLESimulator:
    def __init__(self, scan_time_ms, buffer_size, max_devices):
        self.sequence_number = 0
        self.n_adv_raw = 0
        self.devices = []
        self.buffer_active = True
        self.scan_time_ms = scan_time_ms
        self.buffer_size = buffer_size
        self.max_devices = max_devices
        self.error_config = ErrorSimulationConfig()
        self.error_simulation = True

    def generate_random_mac(self):
        """Generate a random MAC address."""
        return [random.randint(0, 255) for _ in range(6)]

    def generate_random_adv_data(self, length=31):
        """Generate random advertisement data."""
        return [random.randint(0, 255) for _ in range(length)]

    def generate_device_data(self):
        """Genera datos fijos de dispositivo para testing"""
        return {
            "mac": [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC],
            "addr_type": 1,
            "adv_type": AdvType.CONNECTABLE.value,
            "rssi": -75,
            "data_length": 31,
            "data": [i % 256 for i in range(31)],  # Patrón predecible
            "n_adv": 5
        }

    def create_buffer(self, num_devices=5):
        """Create a buffer with the specified number of devices."""
        if not self.buffer_active:
            return None

        # Generate header
        self.sequence_number = (self.sequence_number + 1) % 256
        self.n_adv_raw += sum(device["n_adv"] for device in self.devices)

        # Create devices
        self.devices = [self.generate_device_data() for _ in range(num_devices)]

        # Pack header
        header = struct.pack(
            "<4sBBHB",
            UART_HEADER_MAGIC,  # 4 bytes: Magic header
            0x01,  # 1 byte: Message type (advertisement data)
            self.sequence_number,  # 1 byte: Sequence number
            self.n_adv_raw,  # 2 bytes: Total reception events
            len(self.devices),  # 1 byte: Number of unique MACs
        )

        # Pack device data
        device_data = b""
        for device in self.devices:
            device_data += struct.pack(
                "<6sBBbB31sB",
                bytes(device["mac"]),  # 6 bytes: MAC address
                device["addr_type"],  # 1 byte: Address type
                device["adv_type"],  # 1 byte: Advertisement type
                device["rssi"],  # 1 byte: RSSI
                device["data_length"],  # 1 byte: Data length
                bytes(device["data"]),  # 31 bytes: Advertisement data
                device["n_adv"],  # 1 byte: Number of advertisements
            )

        buffer = header + device_data
        return self.simulate_errors(buffer)

    def simulate_errors(self, buffer):
        """Simulate different types of errors in the buffer based on configuration"""
        if not self.error_simulation:
            return buffer
            
        buffer_modified = False
        buffer_list = bytearray(buffer)
        
        # Check manual triggers first
        if self.error_config.trigger_sequence_error:
            old_seq = self.sequence_number
            self.sequence_number += random.randint(*self.error_config.sequence_jump_range)
            # Update sequence number in buffer
            buffer_list[5] = self.sequence_number % 256
            logger.warning(f"\n=== MANUAL SEQUENCE ERROR ===")
            logger.warning(f"Sequence jump: {old_seq} -> {self.sequence_number}")
            logger.warning(f"New sequence value in buffer: 0x{buffer_list[5]:02X}")
            self.error_config.trigger_sequence_error = False
            buffer_modified = True
            
        elif self.error_config.trigger_corruption_error:
            pos = random.randint(9, len(buffer)-1)
            original_value = buffer_list[pos]
            buffer_list[pos] = random.randint(0, 255)
            logger.warning(f"\n=== MANUAL DATA CORRUPTION ===")
            logger.warning(f"Position: {pos}")
            logger.warning(f"Value change: 0x{original_value:02X} -> 0x{buffer_list[pos]:02X}")
            self.error_config.trigger_corruption_error = False
            buffer_modified = True
            
        elif self.error_config.trigger_header_error:
            original_header = buffer_list[0:4].hex()
            buffer_list[0:4] = b'\x54\x54\x54\x54'
            logger.warning(f"\n=== MANUAL HEADER CORRUPTION ===")
            logger.warning(f"Header change: 0x{original_header} -> 0x{buffer_list[0:4].hex()}")
            self.error_config.trigger_header_error = False
            buffer_modified = True
            
        # Then check percentage-based errors
        else:
            if self.error_config.enable_sequence_errors and random.random() < self.error_config.sequence_error_rate:
                old_seq = self.sequence_number
                self.sequence_number += random.randint(*self.error_config.sequence_jump_range)
                buffer_list[5] = self.sequence_number % 256
                logger.warning(f"\n=== RANDOM SEQUENCE ERROR ===")
                logger.warning(f"Sequence jump: {old_seq} -> {self.sequence_number}")
                logger.warning(f"New sequence value in buffer: 0x{buffer_list[5]:02X}")
                buffer_modified = True
                
            if self.error_config.enable_data_corruption and random.random() < self.error_config.data_corruption_rate:
                pos = random.randint(9, len(buffer)-1)
                original_value = buffer_list[pos]
                buffer_list[pos] = random.randint(0, 255)
                logger.warning(f"\n=== RANDOM DATA CORRUPTION ===")
                logger.warning(f"Position: {pos}")
                logger.warning(f"Value change: 0x{original_value:02X} -> 0x{buffer_list[pos]:02X}")
                buffer_modified = True
                
            if self.error_config.enable_header_errors and random.random() < self.error_config.header_error_rate:
                original_header = buffer_list[0:4].hex()
                buffer_list[0:4] = b'\x54\x54\x54\x54'
                logger.warning(f"\n=== RANDOM HEADER CORRUPTION ===")
                logger.warning(f"Header change: 0x{original_header} -> 0x{buffer_list[0:4].hex()}")
                buffer_modified = True

        if buffer_modified:
            logger.info("\n=== ERROR SIMULATION SUMMARY ===")
            logger.info("Buffer was modified due to error simulation")
            return bytes(buffer_list)
        return buffer

    def print_buffer_info(self, buffer):
        """Print detailed information about the buffer."""
        if not buffer:
            logger.warning("Buffer is empty or inactive.")
            return

        try:
            # First verify header magic
            header_magic = buffer[:4]
            if header_magic != UART_HEADER_MAGIC:
                logger.error(f"\n=== INVALID HEADER DETECTED ===")
                logger.error(f"Expected: {UART_HEADER_MAGIC.hex(':')}")
                logger.error(f"Received: {header_magic.hex(':')}")
                logger.error("Rejecting buffer due to invalid header")
                return  # Exit early on invalid header

            # Unpack header
            header = struct.unpack("<4sBBHB", buffer[:9])
            
            # Log header information with clear formatting
            logger.info("\n=== Buffer Header Information ===")
            logger.info(f"Magic Header: {header[0].hex(':')}")
            logger.info(f"Message Type: 0x{header[1]:02X}")
            logger.info(f"Sequence Number: {header[2]} (0x{header[2]:02X})")
            logger.info(f"Total Events (n_adv_raw): {header[3]}")
            logger.info(f"Number of Unique MACs: {header[4]}")

            # Verify sequence number for jumps
            if hasattr(self, '_last_sequence'):
                expected_seq = (self._last_sequence + 1) % 256
                if header[2] != expected_seq:
                    logger.error(f"\n=== SEQUENCE ERROR DETECTED ===")
                    logger.error(f"Expected sequence: {expected_seq}")
                    logger.error(f"Received sequence: {header[2]}")
            self._last_sequence = header[2]

            # Unpack device data
            device_data = buffer[9:]
            num_devices = header[4]

            logger.info("\n=== Device Information ===")
            for i in range(num_devices):
                start = i * DEVICE_DATA_SIZE
                device = struct.unpack("<6sBBbB31sB", device_data[start:start + DEVICE_DATA_SIZE])
                
                logger.info(f"\nDevice {i + 1}:")
                logger.info(f"  MAC: {':'.join([f'{b:02X}' for b in device[0]])}")
                logger.info(f"  Address Type: {device[1]}")
                logger.info(f"  Advertisement Type: {AdvType(device[2]).name}")
                logger.info(f"  RSSI: {device[3]} dBm")
                logger.info(f"  Data Length: {device[4]}")
                logger.info(f"  Advertisement Data: {device[5].hex()}")
                logger.info(f"  N_Adv: {device[6]}")

            # Add a separator for better readability
            logger.info("\n" + "="*40 + "\n")

        except Exception as e:
            logger.error(f"Error parsing buffer: {e}")

    def reset_buffer(self):
        """Reset the buffer and clear all devices."""
        self.sequence_number = 0
        self.n_adv_raw = 0
        self.devices = []
        logger.info("Buffer reset.")

    def simulate_nordic(self):
        """Simulate the Nordic SoC behavior."""
        try:
            while True:
                if not self.buffer_active:
                    logger.info("Buffer is inactive. Skipping iteration.")
                    time.sleep(self.scan_time_ms / 1000)
                    continue

                # Generate random number of devices (1 to max_devices)
                num_devices = random.randint(1, self.max_devices)
                logger.info(f"Generating buffer with {num_devices} devices...")

                # Create buffer
                buffer = self.create_buffer(num_devices)

                # Print buffer info
                self.print_buffer_info(buffer)

                # Simulate sending buffer via UART
                logger.info("Sending buffer via UART...")

                # Reset buffer after sending
                self.reset_buffer()

                # Wait for the next sampling interval
                time.sleep(self.scan_time_ms / 1000)

        except KeyboardInterrupt:
            logger.info("\nSimulation stopped.")

class BLESimulatorGUI:
    def __init__(self, simulator):
        self.simulator = simulator
        self.root = tk.Tk()
        self.root.title("BLE Simulator Control Panel")
        self.root.geometry("1200x800")
        self.running = False
        self.log_queue = queue.Queue()
        self.setup_gui()

    def setup_gui(self):
        # Create main frame
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Control Panel
        control_frame = ttk.LabelFrame(main_frame, text="Control Panel", padding="5")
        control_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Basic Controls
        self.setup_basic_controls(control_frame)

        # Error Simulation Panel
        error_frame = ttk.LabelFrame(main_frame, text="Error Simulation", padding="5")
        error_frame.grid(row=0, column=1, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)
        
        self.setup_error_controls(error_frame)

        # Replace log frame with new tree view frame
        log_frame = ttk.LabelFrame(main_frame, text="Log Output", padding="5")
        log_frame.grid(row=1, column=0, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S), padx=5, pady=5)

        # Create tree view for structured logging with adjusted column widths
        self.tree = ttk.Treeview(log_frame, height=20, style="Custom.Treeview")
        self.tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Add horizontal scrollbar
        h_scrollbar = ttk.Scrollbar(log_frame, orient="horizontal", command=self.tree.xview)
        h_scrollbar.grid(row=1, column=0, sticky=(tk.W, tk.E))
        
        # Add vertical scrollbar
        v_scrollbar = ttk.Scrollbar(log_frame, orient="vertical", command=self.tree.yview)
        v_scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Configure scrollbars
        self.tree.configure(
            yscrollcommand=v_scrollbar.set,
            xscrollcommand=h_scrollbar.set
        )

        # Configure tree columns with adjusted widthk
        self.tree["columns"] = ("timestamp", "level", "message")
        self.tree.column("#0", width=80, minwidth=80)  # ID column
        self.tree.column("timestamp", width=100, minwidth=100)
        self.tree.column("level", width=100, minwidth=100)
        self.tree.column("message", width=800, minwidth=400)  # Message column with minimum width

        # Configure column headings
        self.tree.heading("#0", text="ID", anchor=tk.W)
        self.tree.heading("timestamp", text="Time", anchor=tk.W)
        self.tree.heading("level", text="Level", anchor=tk.W)
        self.tree.heading("message", text="Message", anchor=tk.W)

        # Configure style for better visibility
        style = ttk.Style()
        style.configure("Custom.Treeview", rowheight=25)  # Increase row height
        
        # Configure tags for different message types
        self.tree.tag_configure('error', foreground='red')
        self.tree.tag_configure('warning', foreground='orange')
        self.tree.tag_configure('info', foreground='black')
        self.tree.tag_configure('buffer_header', background='#E8E8E8')

        # Keep track of buffer groups
        self.current_buffer_id = 0
        self.error_count = 0

        # Configure grid weights
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(1, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        # Setup logging
        self.setup_logging()

    def setup_basic_controls(self, parent):
        # Number of devices
        ttk.Label(parent, text="Number of Devices:").grid(row=0, column=0, padx=5, pady=5)
        self.num_devices_var = tk.StringVar(value="5")
        self.num_devices_entry = ttk.Entry(parent, textvariable=self.num_devices_var, width=10)
        self.num_devices_entry.grid(row=0, column=1, padx=5, pady=5)

        # Scan time
        ttk.Label(parent, text="Scan Time (ms):").grid(row=1, column=0, padx=5, pady=5)
        self.scan_time_var = tk.StringVar(value="7000")
        self.scan_time_entry = ttk.Entry(parent, textvariable=self.scan_time_var, width=10)
        self.scan_time_entry.grid(row=1, column=1, padx=5, pady=5)

        # Start/Stop button
        self.start_stop_button = ttk.Button(parent, text="Start", command=self.toggle_simulation)
        self.start_stop_button.grid(row=2, column=0, columnspan=2, padx=5, pady=5)

        # Add Clear Logs button
        ttk.Button(parent, text="Clear Logs", 
                  command=self.clear_logs).grid(row=4, column=0, columnspan=2, padx=5, pady=5)

    def setup_error_controls(self, parent):
        # Create a frame for manual triggers
        trigger_frame = ttk.LabelFrame(parent, text="Manual Error Triggers", padding="5")
        trigger_frame.grid(row=0, column=0, columnspan=2, sticky=(tk.W, tk.E), padx=5, pady=5)

        # Add trigger buttons
        ttk.Button(
            trigger_frame,
            text="Trigger Sequence Error",
            command=lambda: self.trigger_error('sequence')
        ).grid(row=0, column=0, padx=5, pady=5)

        ttk.Button(
            trigger_frame,
            text="Trigger Data Corruption",
            command=lambda: self.trigger_error('corruption')
        ).grid(row=0, column=1, padx=5, pady=5)

        ttk.Button(
            trigger_frame,
            text="Trigger Header Error",
            command=lambda: self.trigger_error('header')
        ).grid(row=1, column=0, columnspan=2, padx=5, pady=5)

        # Separator
        ttk.Separator(parent, orient='horizontal').grid(
            row=1, column=0, columnspan=2, sticky=(tk.W, tk.E), pady=10)

        # Random Error Controls (existing controls)
        ttk.Label(parent, text="Random Error Controls").grid(
            row=2, column=0, columnspan=2, pady=5)

        # Master error simulation toggle
        self.error_sim_var = tk.BooleanVar(value=True)
        self.error_sim_check = ttk.Checkbutton(
            parent, 
            text="Enable Random Error Simulation",
            variable=self.error_sim_var,
            command=self.toggle_error_simulation
        )
        self.error_sim_check.grid(row=3, column=0, columnspan=2, padx=5, pady=5)

        # Sequence Error Controls
        ttk.Label(parent, text="Sequence Errors").grid(row=4, column=0, padx=5, pady=5)
        self.seq_error_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent,
            variable=self.seq_error_var,
            command=lambda: self.update_error_config('sequence')
        ).grid(row=4, column=1)
        
        ttk.Label(parent, text="Sequence Error Rate (%):").grid(row=5, column=0, padx=5, pady=5)
        self.seq_rate_var = tk.StringVar(value="5")
        ttk.Entry(
            parent,
            textvariable=self.seq_rate_var,
            width=10
        ).grid(row=5, column=1)

        # Data Corruption Controls
        ttk.Label(parent, text="Data Corruption").grid(row=6, column=0, padx=5, pady=5)
        self.corrupt_error_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent,
            variable=self.corrupt_error_var,
            command=lambda: self.update_error_config('corruption')
        ).grid(row=6, column=1)
        
        ttk.Label(parent, text="Corruption Rate (%):").grid(row=7, column=0, padx=5, pady=5)
        self.corrupt_rate_var = tk.StringVar(value="3")
        ttk.Entry(
            parent,
            textvariable=self.corrupt_rate_var,
            width=10
        ).grid(row=7, column=1)

        # Header Error Controls
        ttk.Label(parent, text="Header Errors").grid(row=8, column=0, padx=5, pady=5)
        self.header_error_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(
            parent,
            variable=self.header_error_var,
            command=lambda: self.update_error_config('header')
        ).grid(row=8, column=1)
        
        ttk.Label(parent, text="Header Error Rate (%):").grid(row=9, column=0, padx=5, pady=5)
        self.header_rate_var = tk.StringVar(value="2")
        ttk.Entry(
            parent,
            textvariable=self.header_rate_var,
            width=10
        ).grid(row=9, column=1)

        # Sequence Jump Range
        ttk.Label(parent, text="Sequence Jump Range:").grid(row=10, column=0, padx=5, pady=5)
        range_frame = ttk.Frame(parent)
        range_frame.grid(row=10, column=1, padx=5, pady=5)
        
        self.seq_jump_min_var = tk.StringVar(value="1")
        ttk.Entry(range_frame, textvariable=self.seq_jump_min_var, width=5).pack(side=tk.LEFT)
        ttk.Label(range_frame, text="-").pack(side=tk.LEFT, padx=2)
        self.seq_jump_max_var = tk.StringVar(value="5")
        ttk.Entry(range_frame, textvariable=self.seq_jump_max_var, width=5).pack(side=tk.LEFT)

        # Apply Button
        ttk.Button(
            parent,
            text="Apply Error Settings",
            command=self.apply_error_settings
        ).grid(row=11, column=0, columnspan=2, pady=10)

    def update_error_config(self, error_type):
        """Update error configuration based on checkbox changes"""
        if not hasattr(self.simulator, 'error_config'):
            self.simulator.error_config = ErrorSimulationConfig()

        if error_type == 'sequence':
            self.simulator.error_config.enable_sequence_errors = self.seq_error_var.get()
        elif error_type == 'corruption':
            self.simulator.error_config.enable_data_corruption = self.corrupt_error_var.get()
        elif error_type == 'header':
            self.simulator.error_config.enable_header_errors = self.header_error_var.get()

    def apply_error_settings(self):
        """Apply all error settings"""
        try:
            # Update error rates (convert percentage to decimal)
            self.simulator.error_config.sequence_error_rate = float(self.seq_rate_var.get()) / 100
            self.simulator.error_config.data_corruption_rate = float(self.corrupt_rate_var.get()) / 100
            self.simulator.error_config.header_error_rate = float(self.header_rate_var.get()) / 100
            
            # Update sequence jump range
            self.simulator.error_config.sequence_jump_range = (
                int(self.seq_jump_min_var.get()),
                int(self.seq_jump_max_var.get())
            )
            
            logging.info("Error settings updated successfully")
        except ValueError as e:
            logging.error(f"Invalid error settings: {e}")

    def setup_logging(self):
        """Configure logging to display in GUI"""
        class TreeHandler(logging.Handler):
            def __init__(self, tree_view):
                super().__init__()
                self.tree_view = tree_view

            def emit(self, record):
                try:
                    # Format the message
                    message = self.format(record)
                    # Add to tree view
                    self.tree_view.add_log_entry(record.created, record.levelname, message)
                except Exception as e:
                    print(f"Error in log handler: {e}")

        # Create and configure handler
        handler = TreeHandler(self)
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)

        # Get root logger and add handler
        root_logger = logging.getLogger()
        root_logger.addHandler(handler)

    def add_log_entry(self, timestamp, level, message):
        """Add a new log entry to the tree with improved formatting"""
        timestamp_str = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
        
        # Determine message type and formatting
        if "=== Buffer Header Information ===" in message:
            self.current_buffer_id += 1
            buffer_id = f"buffer_{self.current_buffer_id}"
            return self.tree.insert("", 0, buffer_id, text=f"#{self.current_buffer_id}", 
                                  values=(timestamp_str, "INFO", f"Buffer #{self.current_buffer_id}"),
                                  tags=('buffer_header',))
        
        elif "ERROR" in level:
            self.error_count += 1
            error_id = f"error_{self.error_count}"
            item_id = self.tree.insert("", 0, error_id, text="❌", 
                                     values=(timestamp_str, level, message.strip()),
                                     tags=('error',))
            self.tree.see(item_id)
            return item_id
            
        elif "WARNING" in level:
            self.error_count += 1
            warn_id = f"warn_{self.error_count}"
            item_id = self.tree.insert("", 0, warn_id, text="⚠️", 
                                     values=(timestamp_str, level, message.strip()),
                                     tags=('warning',))
            self.tree.see(item_id)
            return item_id
            
        else:
            # Format regular messages
            if hasattr(self, 'current_buffer_id'):
                parent_id = f"buffer_{self.current_buffer_id}"
                # Indent and clean up the message
                cleaned_message = message.strip()
                return self.tree.insert(parent_id, "end", 
                                      values=(timestamp_str, level, cleaned_message),
                                      tags=('info',))

    def clear_logs(self):
        """Clear all logs from the tree view"""
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.current_buffer_id = 0
        self.error_count = 0

    def toggle_error_simulation(self):
        """Toggle error simulation"""
        self.simulator.error_simulation = self.error_sim_var.get()

    def toggle_simulation(self):
        """Start or stop the simulation"""
        if not self.running:
            try:
                num_devices = int(self.num_devices_var.get())
                scan_time = int(self.scan_time_var.get())
                
                self.simulator.scan_time_ms = scan_time
                self.simulator.max_devices = num_devices
                
                self.running = True
                self.start_stop_button.config(text="Stop")
                
                # Start simulation in a separate thread
                self.sim_thread = threading.Thread(target=self.run_simulation)
                self.sim_thread.daemon = True
                self.sim_thread.start()
            except ValueError as e:
                logging.error(f"Invalid input: {e}")
        else:
            self.running = False
            self.start_stop_button.config(text="Start")

    def run_simulation(self):
        """Run the simulation loop"""
        while self.running:
            try:
                num_devices = min(int(self.num_devices_var.get()), self.simulator.max_devices)
                buffer = self.simulator.create_buffer(num_devices)
                if buffer:
                    self.simulator.print_buffer_info(buffer)
                    # Force GUI update
                    self.root.update()
                time.sleep(self.simulator.scan_time_ms / 1000)
            except Exception as e:
                logging.error(f"Simulation error: {e}")
                self.running = False
                self.root.after(0, lambda: self.start_stop_button.config(text="Start"))

    def run(self):
        """Start the GUI main loop"""
        self.root.mainloop()

    def trigger_error(self, error_type):
        """Trigger a manual error"""
        if not hasattr(self.simulator, 'error_config'):
            self.simulator.error_config = ErrorSimulationConfig()

        if error_type == 'sequence':
            self.simulator.error_config.trigger_sequence_error = True
            logging.info("Manual sequence error triggered")
        elif error_type == 'corruption':
            self.simulator.error_config.trigger_corruption_error = True
            logging.info("Manual data corruption triggered")
        elif error_type == 'header':
            self.simulator.error_config.trigger_header_error = True
            logging.info("Manual header error triggered")

# Parse command-line arguments
def parse_arguments():
    parser = argparse.ArgumentParser(description="Nordic SoC BLE Scanner Simulator")
    parser.add_argument(
        "--scan-time",
        type=int,
        default=7000,
        help="Scan time interval in milliseconds (default: 7000 ms)",
    )
    parser.add_argument(
        "--buffer-size",
        type=int,
        default=1024,
        help="Buffer size in bytes (default: 1024 bytes)",
    )
    parser.add_argument(
        "--max-devices",
        type=int,
        default=50,
        help="Maximum number of devices per buffer (default: 50)",
    )
    parser.add_argument(
        "--gui",
        type=bool,
        default=False,
        help="Start with GUI interface (default: False)",
    )
    return parser.parse_args()

# Main function
if __name__ == "__main__":
    args = parse_arguments()
    
    simulator = BLESimulator(
        scan_time_ms=args.scan_time,
        buffer_size=args.buffer_size,
        max_devices=args.max_devices,
    )

    if args.gui:
        # Start GUI version
        gui = BLESimulatorGUI(simulator)
        gui.run()
    else:
        # Start console version
        logger.info(f"Starting simulation with scan_time={args.scan_time} ms, "
                   f"buffer_size={args.buffer_size} bytes, "
                   f"max_devices={args.max_devices}")
        simulator.simulate_nordic()
