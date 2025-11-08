# secondary_namenode.py
import socket
import json
import time
import os
import threading

import config  


CHECKPOINT_INTERVAL = getattr(config, "SNN_CHECKPOINT_INTERVAL_SEC", 30)  # 5 min default
SNN_ID = getattr(config, "SNN_ID", "secondary_namenode_0")
SNN_DIR = getattr(config, "SNN_DIR", "snn_checkpoints")
os.makedirs(SNN_DIR, exist_ok=True)


def _recv_full_json(sock: socket.socket) -> dict:
    """Safely receive JSON messages from a socket until full JSON is detected."""
    buffer = ""
    while True:
        chunk = sock.recv(4096).decode()
        if not chunk:
            break
        buffer += chunk
        if buffer.strip().endswith('}'):
            break

    if not buffer.strip():
        return {}

    try:
        return json.loads(buffer)
    except json.JSONDecodeError as e:
        print(f"[SNN] ERROR: Could not parse JSON response: {e} | Raw: {buffer}")
        return {}


def _send_json_and_get_response(payload: dict) -> dict:
    """Send JSON payload to Namenode and read complete JSON response."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.connect((config.NAMENODE_HOST, config.NAMENODE_PORT_CLIENT))
            s.sendall((json.dumps(payload) + "\n").encode())
            return _recv_full_json(s)
    except Exception as e:
        print(f"[SNN] ERROR: Communication with Namenode failed: {e}")
        return {}


def _apply_transaction(meta_dict: dict, transaction: dict):
    op = transaction.get('op')
    if op == 'UPLOAD':
        filename = transaction.get('filename')
        chunks = transaction.get('chunks')
        chunk_map = transaction.get('chunk_map')
        meta_dict.setdefault('files', {})
        meta_dict.setdefault('chunks', {})
        meta_dict['files'][filename] = {"chunks": chunks, "status": "pending"}
        meta_dict['chunks'].update(chunk_map)
    elif op == 'COMMIT':
        filename = transaction.get('filename')
        if filename in meta_dict.get('files', {}):
            meta_dict['files'][filename]["status"] = "committed"


def _merge_fsimage_and_edits(fsimage_obj: dict, edits_lines: str) -> dict:
    """Merge existing fsimage with pending edit log lines."""
    fsimage_obj.setdefault('files', {})
    fsimage_obj.setdefault('chunks', {})
    fsimage_obj.setdefault('datanodes', {})

    for line in edits_lines.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            txn = json.loads(line)
            _apply_transaction(fsimage_obj, txn)
        except Exception as e:
            print(f"[SNN] Warning: failed to apply edit line: {e}")
    return fsimage_obj


def perform_checkpoint_once() -> bool:
    try:
        print("[SNN] Requesting metadata from Namenode...")
        fetch_resp = _send_json_and_get_response({"command": "FETCH_METADATA", "snn_id": SNN_ID})
        if fetch_resp.get("status") != "SUCCESS":
            print(f"[SNN] ERROR: Namenode refused FETCH_METADATA: {fetch_resp.get('message')}")
            return False

        fsimage_text = fetch_resp.get("fsimage", "")
        edits_text = fetch_resp.get("edits", "")

        # Save raw copies
        stamp = int(time.time())
        src_fsimage_path = os.path.join(SNN_DIR, f"fsimage_{stamp}.json")
        src_edits_path = os.path.join(SNN_DIR, f"editlog_{stamp}.jsonl")
        with open(src_fsimage_path, "w", encoding="utf-8") as f:
            f.write(fsimage_text or "{}")
        with open(src_edits_path, "w", encoding="utf-8") as f:
            f.write(edits_text or "")

        try:
            fsimage_obj = json.loads(fsimage_text) if fsimage_text.strip() else {}
        except Exception as e:
            print(f"[SNN] ERROR: Invalid fsimage JSON from Namenode: {e}")
            return False

        print("[SNN] Merging fsimage + editlog locally...")
        merged = _merge_fsimage_and_edits(fsimage_obj, edits_text or "")

        ckpt_path = os.path.join(SNN_DIR, f"fsimage.ckpt_{stamp}.json")
        with open(ckpt_path, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2)
        print(f"[SNN] Checkpoint created locally at {ckpt_path}")

        print("[SNN] Uploading checkpoint back to Namenode...")
        update_payload = {
            "command": "UPDATE_FSIMAGE",
            "snn_id": SNN_ID,
            "fsimage": json.dumps(merged)
        }
        update_resp = _send_json_and_get_response(update_payload)

        if not update_resp:
            print("[SNN] ERROR: Namenode did not respond to UPDATE_FSIMAGE.")
            return False
        if update_resp.get("status") != "SUCCESS":
            print(f"[SNN] ERROR: Namenode refused UPDATE_FSIMAGE: {update_resp.get('message')}")
            return False

        print("[SNN] ✅ Checkpoint successfully applied on Namenode (edit log cleared).")
        return True

    except ConnectionRefusedError:
        print("[SNN] ERROR: Could not connect to Namenode. Is it running?")
        return False
    except Exception as e:
        print(f"[SNN] ERROR performing checkpoint: {e}")
        return False


def run_scheduler():
    print(f"[SNN] Secondary Namenode started. Interval = {CHECKPOINT_INTERVAL}s")
    while True:
        started = time.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[SNN] ---- Checkpoint cycle start @ {started} ----")
        perform_checkpoint_once()
        time.sleep(CHECKPOINT_INTERVAL)


if __name__ == "__main__":
    run_scheduler()
