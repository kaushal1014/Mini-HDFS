import socket
import threading
import time
import config
import json
import math
import uuid
import random
import os

# -----------------------------------------------------------------
# --- THIS IS THE BLOCK WITH THE FIX ---
#
# The variable name is now FSIMAGE_FILE, which matches the
# rest of the code.
# -----------------------------------------------------------------
FSIMAGE_FILE = 'namenode_fsimage.json' # <-- RENAMED from METADATA_FILE
EDITLOG_FILE = 'namenode_editlog.jsonl' 

metadata_lock = threading.Lock()
metadata = {} 
# -----------------------------------------------------------------


# --- (These functions are unchanged) ---
def _apply_transaction(meta_dict, transaction):
    op = transaction.get('op')
    if op == 'UPLOAD':
        filename = transaction.get('filename')
        chunks = transaction.get('chunks')
        chunk_map = transaction.get('chunk_map')
        meta_dict['files'][filename] = {"chunks": chunks, "status": "pending"}
        meta_dict['chunks'].update(chunk_map)
    elif op == 'COMMIT':
        filename = transaction.get('filename')
        if filename in meta_dict['files']:
            meta_dict['files'][filename]["status"] = "committed"

def load_metadata():
    # This line will now work correctly
    if os.path.exists(FSIMAGE_FILE):
        try:
            with open(FSIMAGE_FILE, 'r') as f:
                meta = json.load(f)
                print(f"[Namenode] FSImage snapshot loaded from {FSIMAGE_FILE}")
        except Exception as e:
            print(f"[Namenode] Error loading FSImage: {e}. Starting fresh.")
            meta = {"files": {}, "chunks": {}, "datanodes": {}}
    else:
        print(f"[Namenode] No FSImage found. Starting fresh.")
        meta = {"files": {}, "chunks": {}, "datanodes": {}}
    meta.setdefault('files', {})
    meta.setdefault('chunks', {})
    meta.setdefault('datanodes', {})
    if os.path.exists(EDITLOG_FILE):
        print(f"[Namenode] Replaying transactions from EditLog: {EDITLOG_FILE}...")
        try:
            with open(EDITLOG_FILE, 'r') as f:
                for line in f:
                    if line.strip():
                        transaction = json.loads(line)
                        _apply_transaction(meta, transaction)
            print("[Namenode] EditLog replay complete.")
        except Exception as e:
            print(f"[Namenode] CRITICAL: Error replaying EditLog: {e}.")
    return meta

def log_transaction(transaction_data):
    with metadata_lock:
        _apply_transaction(metadata, transaction_data)
        try:
            with open(EDITLOG_FILE, 'a') as f:
                f.write(json.dumps(transaction_data) + '\n')
        except Exception as e:
            print(f"[Namenode] CRITICAL: Failed to write to EditLog: {e}")

# (This replication-check thread is unchanged)
def check_and_replicate_chunks():
    while True:
        time.sleep(15) 
        print("[Namenode] Replication check thread running...")
        with metadata_lock:
            cutoff_time = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
            alive_datanodes = {dn_id for dn_id, last_hb in metadata["datanodes"].items() if last_hb > cutoff_time}
            chunks_to_replicate = []
            for filename, file_meta in metadata["files"].items():
                if file_meta["status"] != "committed": continue
                for chunk_id in file_meta["chunks"]:
                    if chunk_id not in metadata["chunks"]: continue 
                    chunk_locations = metadata["chunks"][chunk_id]
                    alive_replicas = [loc for loc in chunk_locations if loc["id"] in alive_datanodes]
                    if 0 < len(alive_replicas) < config.REPLICATION_FACTOR:
                        source_location = alive_replicas[0] 
                        potential_target_ids = [dn_id for dn_id in alive_datanodes if dn_id not in [loc["id"] for loc in alive_replicas]]
                        if not potential_target_ids:
                            print(f"[Namenode] Warning: Chunk {chunk_id} is under-replicated but no available targets!")
                            continue
                        target_id = random.choice(potential_target_ids)
                        target_info = { "id": target_id, "host": config.DATANODES[target_id]["host"], "port": config.DATANODES[target_id]["port"] }
                        print(f"[Namenode] Scheduling {chunk_id} to be copied from {source_location['id']} to {target_id}")
                        chunks_to_replicate.append({ "chunk_id": chunk_id, "source": source_location, "target": target_info })
                        metadata["chunks"][chunk_id].append(target_info)
        
        for task in chunks_to_replicate:
            print(f"[Namenode] Triggering replication of {task['chunk_id']} from {task['source']['id']} to {task['target']['id']}")
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((task["source"]["host"], task["source"]["port"]))
                    command = {
                        "command": "REPLICATE_CHUNK", "chunk_id": task["chunk_id"],
                        "target_host": task["target"]["host"], "target_port": task["target"]["port"],
                        "target_id": task["target"]["id"]
                    }
                    header_json = json.dumps(command) + '\n'
                    s.sendall(header_json.encode())
            except Exception as e:
                print(f"[Namenode] Error sending replication command to {task['source']['id']}: {e}")
                with metadata_lock:
                    metadata["chunks"][task['chunk_id']].pop()

def perform_checkpoint():
    """
    This function performs the actual checkpointing logic.
    It's called when a CHECKPOINT_REQUEST is received.
    """
    print("[Checkpoint] Received request. Acquiring lock...")
    with metadata_lock:
        print("[Checkpoint] Acquired lock. Starting checkpoint...")
        
        try:
            # This line will now work correctly
            with open(FSIMAGE_FILE, 'w') as f:
                json.dump(metadata, f, indent=4)
            print(f"[Checkpoint] New FSImage snapshot saved to {FSIMAGE_FILE}")
        except Exception as e:
            print(f"[Checkpoint] CRITICAL: Failed to save FSImage: {e}")

        try:
            with open(EDITLOG_FILE, 'w') as f:
                f.truncate(0)
            print("[Checkpoint] EditLog has been cleared.")
        except Exception as e:
            print(f"[Checkpoint] CRITICAL: Failed to clear EditLog: {e}")
            
        print("[Checkpoint] Checkpoint complete. Releasing lock.")
    return True

def handle_client_request(conn, addr):
    """Handles an incoming client request in a new thread."""
    try:
        data = conn.recv(1024).decode()
        if not data: return
        
        request = json.loads(data)
        command = request.get('command')

        if command == 'UPLOAD_REQUEST':
            filename = request.get('filename')
            filesize = request.get('filesize')
            num_chunks = math.ceil(filesize / config.CHUNK_SIZE_BYTES)
            chunk_plan = {}
            chunk_ids = []
            with metadata_lock:
                cutoff_time = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
                alive_datanodes = [dn for dn, last_hb in metadata["datanodes"].items() if last_hb > cutoff_time]
            if len(alive_datanodes) < config.REPLICATION_FACTOR:
                response = {"status": "ERROR", "message": "Not enough alive datanodes for replication."}
                conn.sendall(json.dumps(response).encode())
                return
            for i in range(num_chunks):
                chunk_id = str(uuid.uuid4())
                chunk_ids.append(chunk_id)
                selected_datanodes = random.sample(alive_datanodes, config.REPLICATION_FACTOR)
                chunk_locations = []
                for dn_id in selected_datanodes:
                    chunk_locations.append({
                        "id": dn_id, "host": config.DATANODES[dn_id]["host"],
                        "port": config.DATANODES[dn_id]["port"]
                    })
                chunk_plan[chunk_id] = chunk_locations
            
            transaction = {
                "op": "UPLOAD", "filename": filename,
                "chunks": chunk_ids, "chunk_map": chunk_plan
            }
            log_transaction(transaction)
            response = {"status": "SUCCESS", "plan": chunk_plan, "chunk_ids": chunk_ids}
            conn.sendall(json.dumps(response).encode())
            print(f"[Namenode] Created plan for {filename} with {num_chunks} chunks.")

        elif command == 'COMMIT_UPLOAD':
            filename = request.get('filename')
            transaction = { "op": "COMMIT", "filename": filename }
            log_transaction(transaction)
            response = {"status": "SUCCESS", "message": "File committed."}
            print(f"[Namenode] Committed file: {filename}")
            conn.sendall(json.dumps(response).encode())

        elif command == 'GET_SYSTEM_STATUS':
            print("[Namenode] Received system status request.")
            with metadata_lock:
                cutoff_time = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
                alive_datanodes = {
                    dn_id: "Alive" for dn_id, last_hb in metadata["datanodes"].items() if last_hb > cutoff_time
                }
                all_datanodes = {dn_id: "Dead" for dn_id in config.DATANODES}
                all_datanodes.update(alive_datanodes) 
                committed_files = {
                    filename: meta for filename, meta in metadata["files"].items() 
                    if meta.get('status') == 'committed'
                }
            response = { "status": "SUCCESS", "files": committed_files, "datanodes": all_datanodes }
            conn.sendall(json.dumps(response).encode())

        elif command == 'DOWNLOAD_REQUEST':
            filename = request.get('filename')
            print(f"[Namenode] Received download request for {filename}")
            with metadata_lock:
                file_info = metadata.get("files", {}).get(filename)
            if not file_info or file_info.get("status") != "committed":
                response = {"status": "ERROR", "message": "File not found or not committed."}
                conn.sendall(json.dumps(response).encode())
                return
            chunk_ids = file_info.get("chunks", [])
            chunk_locations = {}
            with metadata_lock:
                for chunk_id in chunk_ids:
                    chunk_locations[chunk_id] = metadata.get("chunks", {}).get(chunk_id, [])
            response = { "status": "SUCCESS", "chunk_ids": chunk_ids, "chunk_locations": chunk_locations }
            conn.sendall(json.dumps(response).encode())
            print(f"[Namenode] Sent download plan for {filename}")

        elif command == 'CHECKPOINT_REQUEST':
            print("[Namenode] Received CHECKPOINT_REQUEST from Secondary Namenode.")
            perform_checkpoint()
            conn.sendall(json.dumps({"status": "SUCCESS", "message": "Checkpoint complete."}).encode())

    except Exception as e:
        print(f"[Namenode] Error handling client {addr}: {e}")
    finally:
        conn.close()

def listen_for_clients():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
        s.listen()
        print(f"[Namenode] Client listener started on {config.NAMENODE_HOST}:{config.NAMENODE_PORT_CLIENT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client_request, args=(conn, addr), daemon=True).start()

def listen_for_heartbeats():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((config.NAMENODE_HOST, config.NAMENODE_PORT_HEARTBEAT))
        s.listen()
        print(f"[Namenode] Heartbeat listener started on {config.NAMENODE_HOST}:{config.NAMENODE_PORT_HEARTBEAT}")
        while True:
            conn, addr = s.accept()
            try:
                data = conn.recv(1024).decode()
                if ':' in data:
                    datanode_id = data.split(':')[1]
                    with metadata_lock:
                        metadata["datanodes"][datanode_id] = time.time()
                else:
                    print(f"[Namenode] Received invalid heartbeat: {data}")
            except Exception as e:
                print(f"[Namenode] Error handling heartbeat: {e}")
            finally:
                conn.close()

if __name__ == "__main__":
    metadata = load_metadata()
    print("[Namenode] Starting...")

    client_thread = threading.Thread(target=listen_for_clients, daemon=True)
    heartbeat_thread = threading.Thread(target=listen_for_heartbeats, daemon=True)
    repl_thread = threading.Thread(target=check_and_replicate_chunks, daemon=True)
    
    client_thread.start()
    heartbeat_thread.start()
    repl_thread.start()
    
    print(f"[Namenode] Running. (Press Ctrl+C to stop)")
    try:
        while True:
            time.sleep(10) 
    except KeyboardInterrupt:
        print("\n[Namenode] Shutting down.")
        print("[Namenode] Shutdown complete.")
