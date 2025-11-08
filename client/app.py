from flask import Flask, render_template, request, redirect, url_for, flash, send_file
import socket
import os
import math
import sys
import os
import io 
import hashlib 

import json
import threading


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import config

app = Flask(__name__) 
app.secret_key = 'supersecretkey'
app.config['UPLOAD_FOLDER'] = 'uploads/'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

def send_chunk_to_datanode(datanode_info, chunk_id, chunk_data, chunk_checksum):
    """Connects to a Datanode and sends a single chunk with its checksum."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((datanode_info['host'], datanode_info['port']))
            
            
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
                print(f"[ClientUI] Successfully sent {chunk_id} to {datanode_info['id']}")
                return True
            else:
                print(f"[ClientUI] Error sending {chunk_id} to {datanode_info['id']}: {response.get('message')}")
                return False
                
    except Exception as e:
        print(f"[ClientUI] Failed to send chunk {chunk_id} to {datanode_info['id']}: {e}")
        return False

def retrieve_chunk_from_datanode(datanode_info, chunk_id):
    """Connects to a Datanode and retrieves a single chunk."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((datanode_info['host'], datanode_info['port']))
            
            header = { "command": "RETRIEVE_CHUNK", "chunk_id": chunk_id }
            header_json = json.dumps(header) + '\n'
            s.sendall(header_json.encode())
            
    
            chunk_data = b""
            while True:
                data = s.recv(4096)
                if not data: break
                chunk_data += data
            
            print(f"[ClientUI] Successfully retrieved {chunk_id} from {datanode_info['id']}")
            return chunk_data
            
    except Exception as e:
        print(f"[ClientUI] Failed to retrieve chunk {chunk_id} from {datanode_info['id']}: {e}")
        return None


@app.route('/')
def index():
    """Main dashboard page."""
    file_list = {}
    datanode_status = {}
    try:
        
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
            request_data = {"command": "GET_SYSTEM_STATUS"}
            s.sendall(json.dumps(request_data).encode())
            response_data = s.recv(8192).decode()
            response = json.loads(response_data)
            
            if response.get("status") == "SUCCESS":
                file_list = response.get("files", {})
                datanode_status = response.get("datanodes", {})
            else:
                flash("Could not retrieve status from Namenode.")
                
    except Exception as e:
        
        flash(f"Error: Could not connect to Namenode. Is it running?")
        
    return render_template('index.html', files=file_list, datanodes=datanode_status)


@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        flash("No file part")
        return redirect(url_for('index'))
    file = request.files['file']
    if file.filename == '':
        flash("No selected file")
        return redirect(url_for('index'))
    
    if file:
        filename = file.filename
        local_filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(local_filepath)
        filesize = os.path.getsize(local_filepath)
        
        print(f"[ClientUI] Upload requested for {filename}, size {filesize} bytes")

        try:
            
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_namenode:
                s_namenode.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
                request_data = {
                    "command": "UPLOAD_REQUEST",
                    "filename": filename,
                    "filesize": filesize
                }
                s_namenode.sendall(json.dumps(request_data).encode())
                response_data = s_namenode.recv(4096).decode()
                response = json.loads(response_data)
                
                if response.get("status") == "SUCCESS":
                    plan = response.get("plan")
                    chunk_ids = response.get("chunk_ids")
                    print(f"[ClientUI] Received plan. Starting chunk transfer...")
                    
                    chunks_sent_count = 0
                    all_chunks_sent = True 
                    
                    with open(local_filepath, 'rb') as f:
                        for chunk_id in chunk_ids:
                            chunk_data = f.read(config.CHUNK_SIZE_BYTES)
                            if not chunk_data: break
                            
                            chunk_checksum = hashlib.md5(chunk_data).hexdigest()
                            
                            datanode_locations = plan[chunk_id]
                            threads = []
                            for dn_info in datanode_locations:
                                t = threading.Thread(target=send_chunk_to_datanode,
                                                     args=(dn_info, chunk_id, chunk_data, chunk_checksum))
                                threads.append(t)
                                t.start()
                            
                            for t in threads:
                                t.join() 
                            
                            chunks_sent_count += 1
                    
                    if all_chunks_sent:
                    
                        print(f"[ClientUI] Committing upload for {filename}...")
                        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_commit:
                            s_commit.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
                            commit_data = {"command": "COMMIT_UPLOAD", "filename": filename}
                            s_commit.sendall(json.dumps(commit_data).encode())
                            commit_resp = json.loads(s_commit.recv(1024).decode())
                            
                            if commit_resp.get("status") == "SUCCESS":
                                flash(f"File {filename} uploaded and committed successfully!")
                                print(f"[ClientUI] Commit successful.")
                            else:
                                flash(f"File uploaded, but commit failed: {commit_resp.get('message')}")
                                print(f"[ClientUI] Commit failed.")
                    else:
                        flash(f"Error sending chunks for {filename}.")
                else:
                    flash(f"Error from Namenode: {response.get('message')}")
        except Exception as e:
            flash(f"Error connecting to Namenode: {e}")
            print(f"[ClientUI] Error: {e}")
        finally:
            
            if os.path.exists(local_filepath):
                os.remove(local_filepath)
        return redirect(url_for('index'))


@app.route('/download/<filename>')
def download_file(filename):
    print(f"[ClientUI] Received download request for {filename}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_namenode:
            s_namenode.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
            request_data = {"command": "DOWNLOAD_REQUEST", "filename": filename}
            s_namenode.sendall(json.dumps(request_data).encode())
            response_data = s_namenode.recv(8192).decode()
            response = json.loads(response_data)
        
        if response.get("status") != "SUCCESS":
            flash(f"Error getting download plan: {response.get('message')}")
            return redirect(url_for('index'))
            
        chunk_ids = response.get("chunk_ids", [])
        chunk_locations = response.get("chunk_locations", {})
        
        full_file_data = b"" 
        for chunk_id in chunk_ids:
            locations = chunk_locations.get(chunk_id)
            if not locations:
                flash(f"Error: No locations found for chunk {chunk_id}")
                return redirect(url_for('index'))
            
            chunk_data = None
           
            for dn_info in locations:
                print(f"[ClientUI] Attempting to retrieve {chunk_id} from {dn_info['id']}...")
                chunk_data = retrieve_chunk_from_datanode(dn_info, chunk_id)
                if chunk_data:
                    break 
                else:
                    print(f"[ClientUI] Failed to retrieve {chunk_id} from {dn_info['id']}. Trying next replica.")
            
            if chunk_data:
                full_file_data += chunk_data 
            else:
                flash(f"Error: All replicas for chunk {chunk_id} failed. Cannot download file.")
                return redirect(url_for('index'))

        
        print(f"[ClientUI] Serving file {filename} ({len(full_file_data)} bytes)")
        return send_file(
            io.BytesIO(full_file_data),
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        print(f"[ClientUI] Error during download: {e}")
        flash(f"Error during download: {e}")
        return redirect(url_for('index'))


@app.route('/file/<filename>')
def file_info(filename):
    """Displays chunk map for a single file."""
    print(f"[ClientUI] Received info request for {filename}")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_namenode:
            s_namenode.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
            request_data = {"command": "DOWNLOAD_REQUEST", "filename": filename}
            s_namenode.sendall(json.dumps(request_data).encode())
            response_data = s_namenode.recv(8192).decode()
            response = json.loads(response_data)
        
        if response.get("status") != "SUCCESS":
            flash(f"Error getting file info: {response.get('message')}")
            return redirect(url_for('index'))
            
        chunk_ids = response.get("chunk_ids", [])
        chunk_locations = response.get("chunk_locations", {})
        
        
        return render_template('file_info.html', 
                               filename=filename, 
                               chunk_ids=chunk_ids, 
                               chunk_locations=chunk_locations)
    except Exception as e:
        print(f"[ClientUI] Error getting file info: {e}")
        flash(f"Error getting file info: {e}")
        return redirect(url_for('index'))

if __name__ == "__main__":
    print(f"[ClientUI] Starting web server on http://{config.CLIENT_HOST}:{config.CLIENT_PORT}")
    app.run(host=config.CLIENT_HOST, port=config.CLIENT_PORT, debug=True)
