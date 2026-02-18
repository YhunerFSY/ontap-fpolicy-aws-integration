"""
AWS Lambda Function: FPolicy Log Processor

Reads FPolicy log files from FSxN S3 Access Point, extracts 'create' operations,
and sends them to SQS for downstream processing.

Environment Variables:
- S3_ACCESS_POINT_ARN: arn:aws:s3:ap-northeast-1:178625946981:accesspoint/fsxn-fpolicy-log-bucket
- SQS_QUEUE_URL: https://sqs.ap-northeast-1.amazonaws.com/178625946981/FPolicy_Q
- LOG_FILE_PREFIX: fpolicy_ (optional, default: fpolicy_)

Trigger: EventBridge (CloudWatch Events) - Scheduled (e.g., every 5 minutes)
"""

import json
import boto3
import os
from datetime import datetime

# Configuration from environment variables
S3_ACCESS_POINT_ARN = os.environ.get('S3_ACCESS_POINT_ARN', 'arn:aws:s3:ap-northeast-1:178625946981:accesspoint/fsxn-fpolicy-log-bucket')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL', 'https://sqs.ap-northeast-1.amazonaws.com/178625946981/FPolicy_Q')
LOG_FILE_PREFIX = os.environ.get('LOG_FILE_PREFIX', 'fpolicy_')

# AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def lambda_handler(event, context):
    """
    Main Lambda handler
    
    Processes FPolicy log files from S3 Access Point and sends create events to SQS
    
    Returns:
        dict: Summary of processing results
    """
    print(f"Starting FPolicy log processing")
    print(f"S3 Access Point: {S3_ACCESS_POINT_ARN}")
    print(f"SQS Queue: {SQS_QUEUE_URL}")
    
    stats = {
        'files_processed': 0,
        'total_events': 0,
        'create_events': 0,
        'sqs_messages_sent': 0,
        'errors': 0
    }
    
    try:
        # List log files from S3 Access Point
        log_files = list_log_files()
        print(f"Found {len(log_files)} log files to process")
        
        for log_file in log_files:
            try:
                print(f"Processing: {log_file}")
                
                # Read and process log file
                create_events = process_log_file(log_file)
                
                stats['files_processed'] += 1
                stats['total_events'] += create_events['total']
                stats['create_events'] += create_events['creates']
                
                # Send create events to SQS
                for file_path in create_events['files']:
                    try:
                        send_to_sqs(file_path)
                        stats['sqs_messages_sent'] += 1
                    except Exception as e:
                        print(f"Error sending to SQS: {file_path} - {e}")
                        stats['errors'] += 1
                
            except Exception as e:
                print(f"Error processing {log_file}: {e}")
                stats['errors'] += 1
        
        print(f"Processing complete: {json.dumps(stats)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'FPolicy logs processed successfully',
                'stats': stats
            })
        }
        
    except Exception as e:
        print(f"Critical error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'stats': stats
            })
        }


def list_log_files():
    """
    List FPolicy log files from S3 Access Point
    
    Returns:
        list: List of log file keys
    """
    log_files = []
    
    try:
        # List objects using S3 Access Point ARN
        response = s3.list_objects_v2(
            Bucket=S3_ACCESS_POINT_ARN,
            Prefix=LOG_FILE_PREFIX
        )
        
        if 'Contents' in response:
            for obj in response['Contents']:
                # Only process .log files
                if obj['Key'].endswith('.log'):
                    log_files.append(obj['Key'])
        
        return log_files
        
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        raise


def process_log_file(log_file_key):
    """
    Read and process a single FPolicy log file
    
    Args:
        log_file_key: S3 object key
        
    Returns:
        dict: Processing results with create event file paths
    """
    result = {
        'total': 0,
        'creates': 0,
        'files': []
    }
    
    try:
        # Read log file from S3 Access Point
        response = s3.get_object(
            Bucket=S3_ACCESS_POINT_ARN,
            Key=log_file_key
        )
        
        log_content = response['Body'].read().decode('utf-8')
        
        # Process each line (each line is a JSON event)
        for line in log_content.strip().split('\n'):
            if not line:
                continue
                
            try:
                event = json.loads(line)
                result['total'] += 1
                
                # Filter for 'create' operations
                if event.get('operation') == 'create':
                    file_path = event.get('file_path', '')
                    
                    if file_path:
                        # Convert ONTAP path to S3 path
                        s3_path = convert_to_s3_path(file_path)
                        result['files'].append(s3_path)
                        result['creates'] += 1
                        
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON line: {line[:100]} - {e}")
        
        return result
        
    except Exception as e:
        print(f"Error reading log file {log_file_key}: {e}")
        raise


def convert_to_s3_path(ontap_path):
    """
    Convert ONTAP file path to S3 path
    
    Args:
        ontap_path: ONTAP path (e.g., /vol_onpre/test.dat)
        
    Returns:
        str: S3 path (e.g., test.dat or vol_onpre/test.dat)
    """
    # Remove leading /vol_onpre/ or /vol_onpre
    # Examples:
    #   /vol_onpre/test.dat -> test.dat
    #   /vol_onpre/folder/file.txt -> folder/file.txt
    
    if ontap_path.startswith('/vol_onpre/'):
        return ontap_path[len('/vol_onpre/'):]
    elif ontap_path.startswith('vol_onpre/'):
        return ontap_path[len('vol_onpre/'):]
    else:
        # If path doesn't start with expected prefix, return as-is (minus leading /)
        return ontap_path.lstrip('/')


def send_to_sqs(s3_path):
    """
    Send S3 path to SQS queue
    
    Args:
        s3_path: S3 object path
    """
    # Create S3 event message format (compatible with existing processors)
    message_body = {
        "Records": [{
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {
                    "name": "fsxn-fpolicy-bucket",
                    "arn": S3_ACCESS_POINT_ARN
                },
                "object": {
                    "key": s3_path,
                    "size": 0  # Size unknown from FPolicy log
                }
            }
        }]
    }
    
    response = sqs.send_message(
        QueueUrl=SQS_QUEUE_URL,
        MessageBody=json.dumps(message_body)
    )
    
    print(f"Sent to SQS: {s3_path} | MessageId: {response.get('MessageId')}")
    
    return response


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {}
    test_context = {}
    
    result = lambda_handler(test_event, test_context)
    print(json.dumps(result, indent=2))
