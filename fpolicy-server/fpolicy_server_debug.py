"""
NetApp ONTAP FPolicy External Server - DEBUG VERSION
"""

import socket
import threading
import boto3
import json
import struct
import re
from datetime import datetime

# Configuration
FPOLICY_PORT = 9898
SQS_URL = "$AWS SQS ARN"
AWS_REGION = "$Region"

XML_DECL  = b'<?xml version="1.0"?>'
SEPARATOR = b'\n\n'


def log(msg):
    """Print timestamped log message"""
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def send_sqs_message(file_path):
    try:
        sqs = boto3.client('sqs', region_name=AWS_REGION)
        s3_key = file_path.strip("/")
        message_body = {
            "Records": [{
                "eventVersion": "2.1",
                "eventSource": "aws:s3",
                "eventName": "ObjectCreated:Put",
                "s3": {
                    "bucket": {"name": "fsxn-mock-bucket"},
                    "object": {"key": s3_key, "size": 1024}
                }
            }]
        }
        response = sqs.send_message(QueueUrl=SQS_URL, MessageBody=json.dumps(message_body))
        log(f"[SQS] ✓ Message sent: {s3_key} | MessageId: {response.get('MessageId', 'N/A')}")
    except Exception as e:
        log(f"[SQS] ✗ Error: {e}")


def recvall(sock, n):
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return bytes(data)


def read_fpolicy_message(conn):
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
    if closing_quote != b'"':
        log(f"[Warning] Expected closing quote")

    if msg_len == 0 or msg_len > 10 * 1024 * 1024:
        log(f"[Warning] Invalid message length: {msg_len}")
        return None

    return recvall(conn, msg_len)


def parse_header_and_body(raw_bytes):
    parts = raw_bytes.split(b'\n\n', 1)
    header_str = parts[0].strip().decode('utf-8', errors='ignore')
    body_str   = parts[1].strip(b'\x00\n\r').decode('utf-8', errors='ignore') if len(parts) > 1 else ''
    return header_str, body_str


def send_fpolicy_response(conn, session_id, selected_version, vs_uuid, policy_name):
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

    payload     = header_part + SEPARATOR + body_part + b'\x00'
    payload_len = len(payload)

    frame = b'"' + struct.pack('>I', payload_len) + b'"' + payload
    conn.sendall(frame)

    log(f"[Send] NEGO_RESP | Session={session_id} | Version={selected_version}")


def handle_client(conn, addr):
    log(f"\n[+] Connection from {addr}")
    conn.settimeout(120.0)
    
    try:
        while True:
            xml_data = read_fpolicy_message(conn)
            if not xml_data:
                log(f"[-] Connection closed: {addr}")
                break

            text_content = xml_data.decode('utf-8', errors='ignore').rstrip('\x00')
            header_str, body_str = parse_header_and_body(xml_data)
            
            notf_match = re.search(r'<NotfType>(.*?)</NotfType>', header_str)
            msg_type = notf_match.group(1) if notf_match else "UNKNOWN"
            log(f"[Recv] {msg_type} ({len(xml_data)}B)")

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
                
                log(f"[Handshake] Policy={policy_name} | Session={session_id} | Version={selected_version}")
                send_fpolicy_response(conn, session_id, selected_version, vs_uuid, policy_name)

            elif '<NotfType>KEEP_ALIVE_REQ</NotfType>' in header_str or \
                 '<NotfType>KEEP_ALIVE</NotfType>' in header_str:
                log("[KeepAlive] Heartbeat received")

            elif '<NotfType>ALERT_MSG</NotfType>' in header_str:
                log(f"[DEBUG] ALERT Body:\n{body_str}\n")
                alert_msg = re.search(r'<AlertMsg>(.*?)</AlertMsg>', text_content)
                severity = re.search(r'<Severity>(.*?)</Severity>', text_content)
                log(f"[ALERT] {severity.group(1) if severity else 'UNKNOWN'}: " + 
                    f"{alert_msg.group(1) if alert_msg else 'No message'}")

            elif '<NotfType>NOTI_REQ</NotfType>' in header_str:
                log(f"[DEBUG] NOTI_REQ Body:\n{body_str}\n")
                
                path_match = re.search(r'<Path>(.*?)</Path>', body_str)
                if path_match:
                    ontap_path = path_match.group(1)
                    log(f"[Event] NOTI_REQ: {ontap_path}")
                    send_sqs_message(ontap_path)
                    log("[Info] NOTI_REQ - no response sent")

            elif '<NotfType>SCREEN_REQ</NotfType>' in header_str:
                log(f"[DEBUG] SCREEN_REQ Body:\n{body_str}\n")
                
                req_id_match = re.search(r'<ReqId>(.*?)</ReqId>', body_str)
                req_id = req_id_match.group(1) if req_id_match else "0"
                
                req_type_match = re.search(r'<ReqType>(.*?)</ReqType>', body_str)
                req_type = req_type_match.group(1) if req_type_match else "UNKNOWN"
                
                path_match = re.search(r'<PathName>(.*?)</PathName>', body_str)
                if path_match:
                    file_name = path_match.group(1).replace('\\', '/').lstrip('/')
                    ontap_path = "/vol_onpre/" + file_name
                    log(f"[Event] SCREEN_REQ: {ontap_path} (ReqId={req_id}, ReqType={req_type})")
                    send_sqs_message(ontap_path)
                
                # Build SCREEN_RESP with ReqId and ReqType
                response_body = "<FscreenResp><ReqId>" + req_id + "</ReqId><ReqType>" + req_type + "</ReqType><ResStatus>ALLOW</ResStatus></FscreenResp>"
                body_part = XML_DECL + response_body.encode('utf-8')
                content_len = len(body_part)
                
                header_xml = (
                    "<Header>"
                    "<NotfType>SCREEN_RESP</NotfType>"
                    "<ContentLen>" + str(content_len) + "</ContentLen>"
                    "<DataFormat>XML</DataFormat>"
                    "</Header>"
                )
                header_part = XML_DECL + header_xml.encode('utf-8')
                payload = header_part + SEPARATOR + body_part + b'\x00'
                frame = b'"' + struct.pack('>I', len(payload)) + b'"' + payload
                
                log(f"[DEBUG] Sending SCREEN_RESP:")
                log(f"Response Body: {response_body}")
                payload_str = payload.rstrip(b'\x00').decode('utf-8', errors='replace')
                log(f"Full Payload ({len(payload)}B):\n{payload_str}\n")
                
                conn.sendall(frame)
                log(f"[Send] SCREEN_RESP | ReqId={req_id} | ReqType={req_type} | Status=ALLOW")
            
            else:
                log(f"[Unknown] {msg_type}")
                log(f"[DEBUG] Full message:\n{text_content}\n")

    except socket.timeout:
        log(f"[-] Connection timeout: {addr}")
    except Exception as e:
        log(f"[Error] {e}")
        import traceback
        log(f"[Traceback]\n{traceback.format_exc()}")
    finally:
        conn.close()


def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', FPOLICY_PORT))
    server.listen(5)
    log(f"FPolicy Server (DEBUG) listening on port {FPOLICY_PORT}...")
    log(f"SQS URL: {SQS_URL}")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    start_server()
