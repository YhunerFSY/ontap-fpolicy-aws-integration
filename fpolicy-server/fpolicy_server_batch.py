"""
NetApp ONTAP FPolicy External Server (Batch mode)
Logs file events to local directory /mnt/fsxn_log
"""

import socket
import threading
import os
import json
import struct
import re
from datetime import datetime

# Configuration
FPOLICY_PORT = 9898
LOG_DIR = "/mnt/fsxn_log"

# FPolicy Protocol Constants
XML_DECL  = b'<?xml version="1.0"?>'
SEPARATOR = b'\n\n'


def ensure_log_dir():
    """Create log directory if it doesn't exist"""
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
        print(f"[Init] Created log directory: {LOG_DIR}")


def write_event_log(file_path, operation="unknown"):
    """
    Write file event to local log file
    Creates one log file per day
    
    Args:
        file_path: Full path of the file (e.g., /vol_onpre/test.dat)
        operation: File operation type (e.g., create, write, close, delete)
    """
    try:
        # Generate log filename based on current date
        today = datetime.now().strftime("%Y-%m-%d")
        log_file = os.path.join(LOG_DIR, f"fpolicy_{today}.log")
        
        # Prepare log entry
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = {
            "timestamp": timestamp,
            "operation": operation,
            "file_path": file_path,
            "source": "FSxN FPolicy"
        }
        
        # Write log entry (one JSON object per line)
        with open(log_file, 'a') as f:
            f.write(json.dumps(log_entry) + "\n")
        
        print(f"[Log] {operation}: {file_path}")
        
    except Exception as e:
        print(f"[Log Error] {e}")


def recvall(sock, n):
    """Receive exactly n bytes from socket"""
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)


def read_fpolicy_message(conn):
    """Read FPolicy message with TCP framing"""
    while True:
        b = recvall(conn, 1)
        if not b:
            return None
        if b == b'"':
            break

    len_bytes = recvall(conn, 4)
    if not len_bytes:
        return None
    msg_len = struct.unpack('>I', len_bytes)[0]

    closing_quote = recvall(conn, 1)
    if msg_len == 0 or msg_len > 10 * 1024 * 1024:
        return None

    return recvall(conn, msg_len)


def parse_header_and_body(raw_bytes):
    """Parse FPolicy message into Header and Body"""
    parts = raw_bytes.split(b'\n\n', 1)
    header_str = parts[0].strip().decode('utf-8', errors='ignore')
    body_str   = parts[1].strip(b'\x00\n\r').decode('utf-8', errors='ignore') if len(parts) > 1 else ''
    return header_str, body_str


def send_nego_resp(conn, session_id, selected_version, vs_uuid, policy_name):
    """Send NEGO_RESP - the ONLY response we send"""
    body_xml = (
        "<HandshakeResp>"
        "<VsUUID>" + vs_uuid + "</VsUUID>"
        "<PolicyName>" + policy_name + "</PolicyName>"
        "<SessionId>" + session_id + "</SessionId>"
        "<ProtVersion>" + selected_version + "</ProtVersion>"
        "</HandshakeResp>"
    )
    body_part   = XML_DECL + body_xml.encode('utf-8')
    content_len = len(body_part)

    header_xml = (
        "<Header>"
        "<NotfType>NEGO_RESP</NotfType>"
        "<ContentLen>" + str(content_len) + "</ContentLen>"
        "<DataFormat>XML</DataFormat>"
        "</Header>"
    )
    header_part = XML_DECL + header_xml.encode('utf-8')
    payload = header_part + SEPARATOR + body_part + b'\x00'
    frame = b'"' + struct.pack('>I', len(payload)) + b'"' + payload
    conn.sendall(frame)
    print("[Send] NEGO_RESP | Version=" + selected_version)


def handle_client(conn, addr):
    """Handle FPolicy client connection"""
    print("\n[+] Connection from " + str(addr))
    conn.settimeout(120.0)
    
    try:
        while True:
            xml_data = read_fpolicy_message(conn)
            if not xml_data:
                print("[-] Connection closed: " + str(addr))
                break

            text_content = xml_data.decode('utf-8', errors='ignore').rstrip('\x00')
            header_str, body_str = parse_header_and_body(xml_data)

            # Handle NEGO_REQ
            if '<NotfType>NEGO_REQ</NotfType>' in header_str:
                session_match = re.search(r'<SessionId>(.*?)</SessionId>', body_str)
                policy_match  = re.search(r'<PolicyName>(.*?)</PolicyName>', body_str)
                vs_uuid_match = re.search(r'<VsUUID>(.*?)</VsUUID>', body_str)
                
                session_id  = session_match.group(1) if session_match else ""
                policy_name = policy_match.group(1) if policy_match else ""
                vs_uuid     = vs_uuid_match.group(1) if vs_uuid_match else ""
                
                vers_matches = re.findall(r'<Vers>(.*?)</Vers>', body_str)
                preferred    = ["1.2", "1.1", "1.0", "2.0", "3.0"]
                selected_version = "1.0"
                for v in preferred:
                    if v in vers_matches:
                        selected_version = v
                        break
                
                print("[Handshake] Policy=" + policy_name + " | Session=" + session_id)
                send_nego_resp(conn, session_id, selected_version, vs_uuid, policy_name)

            # Handle KEEP_ALIVE
            elif '<NotfType>KEEP_ALIVE_REQ</NotfType>' in header_str or \
                 '<NotfType>KEEP_ALIVE</NotfType>' in header_str:
                pass  # Silent

            # Handle ALERT_MSG
            elif '<NotfType>ALERT_MSG</NotfType>' in header_str:
                alert_msg = re.search(r'<AlertMsg>(.*?)</AlertMsg>', text_content)
                if alert_msg:
                    print("[ALERT] " + alert_msg.group(1))

            # Handle NOTI_REQ
            elif '<NotfType>NOTI_REQ</NotfType>' in header_str:
                path_match = re.search(r'<Path>(.*?)</Path>', body_str)
                # Extract operation type from NOTI_REQ if available
                op_type_match = re.search(r'<OpType>(.*?)</OpType>', body_str)
                op_type = op_type_match.group(1) if op_type_match else "unknown"
                
                if path_match:
                    ontap_path = path_match.group(1)
                    write_event_log(ontap_path, op_type)

            # Handle SCREEN_REQ
            elif '<NotfType>SCREEN_REQ</NotfType>' in header_str:
                path_match = re.search(r'<PathName>(.*?)</PathName>', body_str)
                # Extract ReqType which contains the actual operation (NFS_CREAT, NFS_WR, etc.)
                req_type_match = re.search(r'<ReqType>(.*?)</ReqType>', body_str)
                req_type = req_type_match.group(1) if req_type_match else "unknown"
                
                # Map FPolicy ReqType to readable operation
                operation_map = {
                    "NFS_CREAT": "create",
                    "NFS_WR": "write",
                    "NFS_RD": "read",
                    "NFS_CLOSE": "close",
                    "NFS_SETATTR": "setattr",
                    "NFS_RENAME": "rename",
                    "NFS_UNLINK": "delete"
                }
                operation = operation_map.get(req_type, req_type.lower())
                
                if path_match:
                    file_name = path_match.group(1).replace('\\', '/').lstrip('/')
                    ontap_path = "/vol_onpre/" + file_name
                    write_event_log(ontap_path, operation)

    except socket.timeout:
        print("[-] Timeout: " + str(addr))
    except Exception as e:
        print("[Error] " + str(e))
    finally:
        conn.close()


def start_server():
    """Start FPolicy server"""
    ensure_log_dir()
    
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', FPOLICY_PORT))
    server.listen(5)
    
    print("FPolicy Server listening on port " + str(FPOLICY_PORT))
    print("Log directory: " + LOG_DIR)
    print("Mode: ASYNCHRONOUS (local logging)")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    start_server()
