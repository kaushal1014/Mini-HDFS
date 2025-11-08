# --- 1. ZERO TIER IPs: EDIT THIS SECTION ---
# Replace these with your team's actual "Managed IPs" from ZeroTier
NAMENODE_HOST = '192.168.191.127'    # IP of the person running namenode.py
DATANODE_0_HOST = '192.168.191.185'   # IP of the person running datanode.py datanode_0
DATANODE_1_HOST = '192.168.191.74'   # IP of the person running datanode.py datanode_1
DATANODE_2_HOST = '192.168.191.127'
CLIENT_HOST = '192.168.191.109'      # IP of the person running client/app.py
DATANODE_2_HOST = '192.168.191.127'
# -------------------------------------------

# --- 2. Namenode Configuration ---
NAMENODE_PORT_CLIENT = 50070   # Port for clients to connect
NAMENODE_PORT_HEARTBEAT = 50071 # Port for Datanodes to send heartbeats

# --- 3. Datanode Configuration ---
DATANODES = {
    "datanode_0": {
        "host": DATANODE_0_HOST,
        "port": 50075,  # Port for Namenode/Client to send commands
        "storage_dir": "data/datanode_0"
    },
    "datanode_1": {
        "host": DATANODE_1_HOST,
        "port": 50076,
        "storage_dir": "data/datanode_1"
    },
    "datanode_2": {
        "host": DATANODE_2_HOST,
        "port": 50077, # <-- New, unique port
        "storage_dir": "data/datanode_2"
    }
}

# --- 4. Client UI Configuration ---
CLIENT_PORT = 8080

# --- 5. File System Configuration ---
CHUNK_SIZE_MB = 2
CHUNK_SIZE_BYTES = CHUNK_SIZE_MB * 1024 * 1024
REPLICATION_FACTOR = 2
HEARTBEAT_INTERVAL_SEC = 5

# --- 6. Secondary Namenode Configuration ---
SNN_ID = "secondary_namenode_0"
SNN_DIR = "snn_checkpoints"
SNN_CHECKPOINT_INTERVAL_SEC = 30
