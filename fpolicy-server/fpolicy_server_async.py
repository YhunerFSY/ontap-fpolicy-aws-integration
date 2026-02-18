"""
NetApp ONTAP FPolicy External Server (Real-time mode)
"""

import socket
import threading
import boto3
import json
import struct
import re

# Configuration
FPOLICY_PORT = 9898
SQS_URL = "https://sqs.ap-northeast-1.amazonaws.com/178625946981/FPolicy_Q"
AWS_REGION = "ap-northeast-1"

XML_DECL  = b'<?xml version="1.0"?>'
SEPARATOR = b'\n\n'


def send_sqs_message(file_path):
    """Send S3 event notification to SQS queue"""
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
        sqs.send_message(QueueUrl=SQS_URL, MessageBody=json.dumps(message_body))
        print("[SQS] Message sent: " + s3_key)
    except Exception as e:
        print("[SQS Error] " + str(e))


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
    """Send NEGO_RESP - the ONLY response we send (Splunk style)"""
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
    """Handle FPolicy client connection (Splunk style - minimal responses)"""
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

            # Handle NEGO_REQ - ONLY message type we respond to
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

            # Handle KEEP_ALIVE - just log, no response
            elif '<NotfType>KEEP_ALIVE_REQ</NotfType>' in header_str or \
                 '<NotfType>KEEP_ALIVE</NotfType>' in header_str:
                print("[KeepAlive] Received")

            # Handle ALERT_MSG - just log
            elif '<NotfType>ALERT_MSG</NotfType>' in header_str:
                alert_msg = re.search(r'<AlertMsg>(.*?)</AlertMsg>', text_content)
                print("[ALERT] " + (alert_msg.group(1) if alert_msg else "No message"))

            # Handle NOTI_REQ - process but NO response (async mode)
            elif '<NotfType>NOTI_REQ</NotfType>' in header_str:
                path_match = re.search(r'<Path>(.*?)</Path>', body_str)
                if path_match:
                    ontap_path = path_match.group(1)
                    print("[Event] " + ontap_path)
                    send_sqs_message(ontap_path)

            # Handle SCREEN_REQ - process but NO response (Splunk style)
            elif '<NotfType>SCREEN_REQ</NotfType>' in header_str:
                path_match = re.search(r'<PathName>(.*?)</PathName>', body_str)
                if path_match:
                    file_name = path_match.group(1).replace('\\', '/').lstrip('/')
                    ontap_path = "/vol_onpre/" + file_name
                    print("[Event] " + ontap_path)
                    send_sqs_message(ontap_path)
                # NOTE: No response sent - this is key!

    except socket.timeout:
        print("[-] Timeout: " + str(addr))
    except Exception as e:
        print("[Error] " + str(e))
    finally:
        conn.close()


def start_server():
    """Start FPolicy server (Splunk style - asynchronous)"""
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', FPOLICY_PORT))
    server.listen(5)
    print("FPolicy Server (Async - Splunk Style) listening on port " + str(FPOLICY_PORT))
    print("SQS: " + SQS_URL)
    print("Mode: ASYNCHRONOUS (no SCREEN_RESP)")
    
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.daemon = True
        thread.start()


if __name__ == "__main__":
    start_server()
