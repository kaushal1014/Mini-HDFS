import socket
import threading
import time
import config
import json
import math
import uuid
import random
import os

# --- Persistent metadata files ---
FSIMAGE_FILE = 'namenode_fsimage.json'
EDITLOG_FILE = 'namenode_editlog.jsonl'

metadata_lock = threading.Lock()
metadata = {}

# ---------------------------- Metadata utils ----------------------------
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
    if os.path.exists(FSIMAGE_FILE):
        try:
            with open(FSIMAGE_FILE, 'r') as f:
                meta = json.load(f)
                print(f"[Namenode] FSImage snapshot loaded from {FSIMAGE_FILE}")
        except Exception as e:
            print(f"[Namenode] Error loading FSImage: {e}. Starting fresh.")
            meta = {"files": {}, "chunks": {}, "datanodes": {}}
    else:
        print("[Namenode] No FSImage found. Starting fresh.")
        meta = {"files": {}, "chunks": {}, "datanodes": {}}

    meta.setdefault('files', {})
    meta.setdefault('chunks', {})
    meta.setdefault('datanodes', {})

    if os.path.exists(EDITLOG_FILE):
        print(f"[Namenode] Replaying transactions from {EDITLOG_FILE}...")
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

# ---------------------------- Replication Monitor ----------------------------
def check_and_replicate_chunks():
    while True:
        time.sleep(15)
        print("[Namenode] Replication check thread running...")
        with metadata_lock:
            cutoff_time = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
            alive_datanodes = {dn_id for dn_id, last_hb in metadata["datanodes"].items() if last_hb > cutoff_time}
            chunks_to_replicate = []

            for filename, file_meta in metadata["files"].items():
                if file_meta["status"] != "committed":
                    continue
                for chunk_id in file_meta["chunks"]:
                    if chunk_id not in metadata["chunks"]:
                        continue
                    chunk_locations = metadata["chunks"][chunk_id]
                    alive_replicas = [loc for loc in chunk_locations if loc["id"] in alive_datanodes]
                    if 0 < len(alive_replicas) < config.REPLICATION_FACTOR:
                        source_location = alive_replicas[0]
                        potential_target_ids = [dn for dn in alive_datanodes if dn not in [loc["id"] for loc in alive_replicas]]
                        if not potential_target_ids:
                            print(f"[Namenode] Warning: Chunk {chunk_id} under-replicated, no targets.")
                            continue
                        target_id = random.choice(potential_target_ids)
                        target_info = {
                            "id": target_id,
                            "host": config.DATANODES[target_id]["host"],
                            "port": config.DATANODES[target_id]["port"]
                        }
                        print(f"[Namenode] Scheduling {chunk_id} copy {source_location['id']} → {target_id}")
                        chunks_to_replicate.append({"chunk_id": chunk_id, "source": source_location, "target": target_info})
                        metadata["chunks"][chunk_id].append(target_info)

        for task in chunks_to_replicate:
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.connect((task["source"]["host"], task["source"]["port"]))
                    cmd = {
                        "command": "REPLICATE_CHUNK",
                        "chunk_id": task["chunk_id"],
                        "target_host": task["target"]["host"],
                        "target_port": task["target"]["port"],
                        "target_id": task["target"]["id"]
                    }
                    s.sendall((json.dumps(cmd) + '\n').encode())
            except Exception as e:
                print(f"[Namenode] Error sending replication command: {e}")
                with metadata_lock:
                    metadata["chunks"][task['chunk_id']].pop()

# ---------------------------- Checkpoint (manual) ----------------------------
def perform_checkpoint():
    print("[Checkpoint] Received request. Acquiring lock...")
    with metadata_lock:
        print("[Checkpoint] Starting checkpoint...")
        try:
            with open(FSIMAGE_FILE, 'w') as f:
                json.dump(metadata, f, indent=4)
            print(f"[Checkpoint] Saved FSImage to {FSIMAGE_FILE}")
        except Exception as e:
            print(f"[Checkpoint] CRITICAL: Failed to save FSImage: {e}")
        try:
            open(EDITLOG_FILE, 'w').close()
            print("[Checkpoint] Cleared EditLog.")
        except Exception as e:
            print(f"[Checkpoint] CRITICAL: Failed to clear EditLog: {e}")
        print("[Checkpoint] Complete.")
    return True

# ---------------------------- Client & SNN Commands ----------------------------
def handle_client_request(conn, addr):
    try:
        buffer = ""
        while True:
            chunk = conn.recv(1024).decode()
            if not chunk:
                break
            buffer += chunk
            # Stop once we believe JSON object is complete
            if buffer.strip().endswith('}'):
                break

        if not buffer:
            return

        try:
            request = json.loads(buffer)
        except json.JSONDecodeError as e:
            print(f"[Namenode] JSON decode error from {addr}: {e} | Raw: {buffer}")
            conn.close()
            return

        command = request.get("command")

        if command == 'UPLOAD_REQUEST':
            filename = request['filename']
            filesize = request['filesize']
            num_chunks = math.ceil(filesize / config.CHUNK_SIZE_BYTES)
            with metadata_lock:
                cutoff = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
                alive = [dn for dn, hb in metadata["datanodes"].items() if hb > cutoff]
            if len(alive) < config.REPLICATION_FACTOR:
                conn.sendall(json.dumps({"status": "ERROR", "message": "Not enough Datanodes"}).encode())
                return
            plan, ids = {}, []
            for _ in range(num_chunks):
                cid = str(uuid.uuid4())
                ids.append(cid)
                sel = random.sample(alive, config.REPLICATION_FACTOR)
                plan[cid] = [
                    {"id": dn, "host": config.DATANODES[dn]["host"], "port": config.DATANODES[dn]["port"]}
                    for dn in sel
                ]
            log_transaction({"op": "UPLOAD", "filename": filename, "chunks": ids, "chunk_map": plan})
            conn.sendall(json.dumps({"status": "SUCCESS", "plan": plan, "chunk_ids": ids}).encode())
            print(f"[Namenode] Upload plan for {filename} with {num_chunks} chunks.")

        elif command == 'COMMIT_UPLOAD':
            fname = request['filename']
            log_transaction({"op": "COMMIT", "filename": fname})
            conn.sendall(json.dumps({"status": "SUCCESS"}).encode())
            print(f"[Namenode] Committed {fname}")

        elif command == 'GET_SYSTEM_STATUS':
            with metadata_lock:
                cutoff = time.time() - (config.HEARTBEAT_INTERVAL_SEC * 2)
                alive = {dn: "Alive" for dn, hb in metadata["datanodes"].items() if hb > cutoff}
                status = {dn: "Dead" for dn in config.DATANODES}
                status.update(alive)
                files = {
                    f: meta for f, meta in metadata["files"].items()
                    if meta["status"] == "committed"
                }
            conn.sendall(json.dumps({"status": "SUCCESS", "files": files, "datanodes": status}).encode())

        elif command == 'DOWNLOAD_REQUEST':
            fname = request['filename']
            print(f"[Namenode] Download request for {fname}")
            with metadata_lock:
                fmeta = metadata["files"].get(fname)
            if not fmeta or fmeta["status"] != "committed":
                conn.sendall(json.dumps({"status": "ERROR", "message": "File not found"}).encode())
                return
            with metadata_lock:
                chunks = fmeta["chunks"]
                locs = {cid: metadata["chunks"][cid] for cid in chunks}
            conn.sendall(json.dumps({"status": "SUCCESS", "chunk_ids": chunks, "chunk_locations": locs}).encode())
            print(f"[Namenode] Sent download plan for {fname}")

        elif command == 'CHECKPOINT_REQUEST':
            print("[Namenode] Manual checkpoint trigger from SNN.")
            perform_checkpoint()
            conn.sendall(json.dumps({"status": "SUCCESS"}).encode())

        elif command == 'FETCH_METADATA':
            print("[Namenode] FETCH_METADATA from SNN.")
            try:
                with metadata_lock:
                    fsimage_text = open(FSIMAGE_FILE).read() if os.path.exists(FSIMAGE_FILE) else json.dumps(metadata)
                    edits_text = open(EDITLOG_FILE).read() if os.path.exists(EDITLOG_FILE) else ""
                conn.sendall(json.dumps({
                    "status": "SUCCESS",
                    "fsimage": fsimage_text,
                    "edits": edits_text
                }).encode())
            except Exception as e:
                conn.sendall(json.dumps({"status": "ERROR", "message": str(e)}).encode())

        elif command == 'UPDATE_FSIMAGE':
            print("[Namenode] UPDATE_FSIMAGE from SNN.")
            try:
                new_img = json.loads(request.get("fsimage", "{}"))
                with metadata_lock:
                    with open(FSIMAGE_FILE, "w") as f:
                        f.write(json.dumps(new_img, indent=2))
                    metadata.clear()
                    metadata.update(new_img)
                    open(EDITLOG_FILE, "w").close()
                conn.sendall(json.dumps({"status": "SUCCESS", "message": "Checkpoint applied"}).encode())
            except Exception as e:
                conn.sendall(json.dumps({"status": "ERROR", "message": str(e)}).encode())

        else:
            msg = f"Unknown command: {command}"
            print(f"[Namenode] {msg}")
            conn.sendall(json.dumps({"status": "ERROR", "message": msg}).encode())

    except Exception as e:
        print(f"[Namenode] Error handling {addr}: {e}")
    finally:
        conn.close()


# ---------------------------- Heartbeats ----------------------------
def listen_for_heartbeats():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((config.NAMENODE_HOST, config.NAMENODE_PORT_HEARTBEAT))
        s.listen()
        print(f"[Namenode] Heartbeat listener {config.NAMENODE_HOST}:{config.NAMENODE_PORT_HEARTBEAT}")
        while True:
            conn, _ = s.accept()
            try:
                msg = conn.recv(1024).decode()
                if ':' in msg:
                    dn_id = msg.split(':')[1]
                    with metadata_lock:
                        metadata["datanodes"][dn_id] = time.time()
            except Exception as e:
                print(f"[Namenode] Heartbeat error: {e}")
            finally:
                conn.close()

# ---------------------------- Client listener ----------------------------
def listen_for_clients():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
        s.listen()
        print(f"[Namenode] Client listener {config.NAMENODE_HOST}:{config.NAMENODE_PORT_CLIENT}")
        while True:
            conn, addr = s.accept()
            threading.Thread(target=handle_client_request, args=(conn, addr), daemon=True).start()

# ---------------------------- Main entry ----------------------------
if __name__ == "__main__":
    metadata = load_metadata()
    print("[Namenode] Starting...")
    threading.Thread(target=listen_for_clients, daemon=True).start()
    threading.Thread(target=listen_for_heartbeats, daemon=True).start()
    threading.Thread(target=check_and_replicate_chunks, daemon=True).start()
    print("[Namenode] Running. (Ctrl+C to stop)")
    try:
        while True:
            time.sleep(10)
    except KeyboardInterrupt:
        print("\n[Namenode] Shutdown complete.")
