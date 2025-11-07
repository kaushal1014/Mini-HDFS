import socket
import threading
import time
import os
import sys
import config
import json
import hashlib # 

MY_INFO = {}
MY_ID = ""

def send_heartbeat():
    while True:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_HEARTBEAT))
                message = f"HEARTBEAT:{MY_ID}"
                s.sendall(message.encode())
        except ConnectionRefusedError:
            print(f"[{MY_ID}] Error: Namenode is down. Retrying...")
        except Exception as e:
            print(f"[{MY_ID}] Error sending heartbeat: {e}")
        time.sleep(config.HEARTBEAT_INTERVAL_SEC)

def send_chunk_to_datanode(datanode_host, datanode_port, datanode_id, chunk_id, chunk_data):
    """Helper function to send a chunk AND ITS CHECKSUM to another datanode."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((datanode_host, datanode_port))
            
            chunk_checksum = hashlib.md5(chunk_data).hexdigest()
            
            header = {
                "command": "STORE_CHUNK",
                "chunk_id": chunk_id,
                "size": len(chunk_data),
                "checksum": chunk_checksum 
            }
            header_json = json.dumps(header) + '\n'
            
            s.sendall(header_json.encode())
            s.sendall(chunk_data)
            
            response = json.loads(s.recv(1024).decode())
            if response.get("status") == "SUCCESS":
                print(f"[{MY_ID}] Successfully replicated {chunk_id} to {datanode_id}")
                return True
            else:
                print(f"[{MY_ID}] Error replicating {chunk_id} to {datanode_id}: {response.get('message')}")
                return False
                
    except Exception as e:
        print(f"[{MY_ID}] Failed to replicate chunk {chunk_id} to {datanode_id}: {e}")
        return False

def handle_command(conn, addr):
    """Handles an incoming command (e.g., from a client or namenode)"""
    try:
        header_data = b""
        buffer = b""
        while b'\n' not in header_data:
            chunk = conn.recv(1024)
            if not chunk:
                print(f"[{MY_ID}] Connection from {addr} closed prematurely.")
                return
            if b'\n' in chunk:
                header_part, rest_part = chunk.split(b'\n', 1)
                header_data += header_part
                buffer = rest_part 
                break 
            else:
                header_data += chunk
        
        header_str = header_data.decode()
        header = json.loads(header_str)
        command = header.get('command')

        if command == 'STORE_CHUNK':
            chunk_id = header.get('chunk_id')
            chunk_size = header.get('size')
            chunk_path = os.path.join(MY_INFO["storage_dir"], chunk_id)
            
            client_checksum = header.get('checksum')
            
            print(f"[{MY_ID}] Receiving chunk {chunk_id} ({chunk_size} bytes)")
            
            m = hashlib.md5()
            with open(chunk_path, 'wb') as f:
                f.write(buffer)
                m.update(buffer) 
                bytes_received = len(buffer)
                
                while bytes_received < chunk_size:
                    data_to_read = min(4096, chunk_size - bytes_received)
                    data = conn.recv(data_to_read)
                    if not data:
                        break
                    f.write(data)
                    m.update(data) 
                    bytes_received += len(data)
            
            local_checksum = m.hexdigest()
            
            if bytes_received != chunk_size:
                print(f"[{MY_ID}] Error: Incomplete chunk {chunk_id}. Expected {chunk_size} got {bytes_received}")
                os.remove(chunk_path) 
                conn.sendall(json.dumps({"status": "ERROR", "message": "Incomplete chunk"}).encode())
                
            elif local_checksum != client_checksum:
                print(f"[{MY_ID}] FATAL: CHUNK CORRUPTION DETECTED on {chunk_id}")
                print(f"    Client   MD5: {client_checksum}")
                print(f"    Local MD5: {local_checksum}")
                os.remove(chunk_path) 
                conn.sendall(json.dumps({"status": "ERROR", "message": "Checksum mismatch"}).encode())
                
            else:
                print(f"[{MY_ID}] Successfully stored chunk {chunk_id} (Checksum OK)")
                conn.sendall(json.dumps({"status": "SUCCESS"}).encode())

        elif command == 'RETRIEVE_CHUNK':
            chunk_id = header.get('chunk_id')
            chunk_path = os.path.join(MY_INFO["storage_dir"], chunk_id)
            
            print(f"[{MY_ID}] Received request for chunk {chunk_id}")
            
            if os.path.exists(chunk_path):
                with open(chunk_path, 'rb') as f:
                    while True:
                        data = f.read(4096)
                        if not data:
                            break
                        conn.sendall(data)
                print(f"[{MY_ID}] Successfully sent chunk {chunk_id}")
            else:
                print(f"[{MY_ID}] Error: Chunk {chunk_id} not found.")

        elif command == 'REPLICATE_CHUNK':
            print(f"[{MY_ID}] Received replication command from Namenode.")
            chunk_id = header.get('chunk_id')
            target_host = header.get('target_host')
            target_port = header.get('target_port')
            target_id = header.get('target_id')
            
            chunk_path = os.path.join(MY_INFO["storage_dir"], chunk_id)
            
            if not os.path.exists(chunk_path):
                print(f"[{MY_ID}] Error: Cannot replicate chunk {chunk_id}, I don't have it.")
                return

            with open(chunk_path, 'rb') as f:
                chunk_data = f.read()
            
            send_chunk_to_datanode(target_host, target_port, target_id, chunk_id, chunk_data)

    except json.JSONDecodeError:
        print(f"[{MY_ID}] Error: Received invalid JSON header from {addr}.")
    except Exception as e:
        print(f"[{MY_ID}] Error handling command from {addr}: {e}")
    finally:
        conn.close()

def listen_for_commands():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
        s.bind((MY_INFO["host"], MY_INFO["port"]))
        s.listen()
        print(f"[{MY_ID}] Listening for commands on {MY_INFO['host']}:{MY_INFO['port']}")
        while True:
            conn, addr = s.accept()
            print(f"[{MY_ID}] Connection from {addr}")
            threading.Thread(target=handle_command, args=(conn, addr), daemon=True).start()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python datanode.py <datanode_id>")
        print("Example: python datanode.py datanode_0")
        sys.exit(1)
        
    MY_ID = sys.argv[1]
    if MY_ID not in config.DATANODES:
        print(f"Error: Unknown datanode_id '{MY_ID}'. Check config.py")
        sys.exit(1)
        
    MY_INFO = config.DATANODES[MY_ID]
    os.makedirs(MY_INFO["storage_dir"], exist_ok=True)
    print(f"[{MY_ID}] Starting...")
    
    heartbeat_thread = threading.Thread(target=send_heartbeat, daemon=True)
    listener_thread = threading.Thread(target=listen_for_commands, daemon=True)
    
    heartbeat_thread.start()
    listener_thread.start()
    
    print(f"[{MY_ID}] Running. (Press Ctrl+C to stop)")
    while True:
        try:
            time.sleep(1)
        except KeyboardInterrupt:
            print(f"\n[{MY_ID}] Shutting down.")
            break