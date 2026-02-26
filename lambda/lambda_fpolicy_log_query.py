"""
AWS Lambda Function: FPolicy Log Query API

On-demand query and replay of FPolicy logs based on API request parameters.
Filters logs by time range, path, and operation type, then sends to SQS.

API Gateway Integration:
- Trigger: API Gateway (REST API)
- Method: POST
- Path: /query-logs

Request Body:
{
  "start_time": "2026-02-18 00:00:00",
  "end_time": "2026-02-18 23:59:59",
  "path_filter": "/vol_onpre/data/",  // optional
  "operations": ["create", "close"],  // optional
  "limit": 1000                       // optional, default 1000
}

Response:
{
  "status": "success",
  "records_found": 150,
  "records_sent": 150,
  "sqs_messages": 150,
  "filters": {...}
}

Environment Variables:
- S3_ACCESS_POINT_ARN: arn:aws:s3:ap-northeast-1:178625946981:accesspoint/fsxn-fpolicy-log-bucket
- SQS_QUEUE_URL: https://sqs.ap-northeast-1.amazonaws.com/178625946981/FPolicy_Q
- LOG_FILE_PREFIX: fpolicy_
- MAX_RESULTS: 10000 (optional, default limit)
"""

import json
import boto3
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional

# Configuration
S3_ACCESS_POINT_ARN = os.environ.get('S3_ACCESS_POINT_ARN', 'arn:aws:s3:ap-northeast-1:178625946981:accesspoint/fsxn-fpolicy-log-bucket')
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL', 'https://sqs.ap-northeast-1.amazonaws.com/178625946981/FPolicy_Q')
LOG_FILE_PREFIX = os.environ.get('LOG_FILE_PREFIX', 'fpolicy_')
MAX_RESULTS = int(os.environ.get('MAX_RESULTS', '10000'))

# AWS clients
s3 = boto3.client('s3')
sqs = boto3.client('sqs')


def lambda_handler(event, context):
    """
    Main Lambda handler for API Gateway requests
    
    Args:
        event: API Gateway event
        context: Lambda context
        
    Returns:
        dict: API Gateway response
    """
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse request body
        if isinstance(event.get('body'), str):
            body = json.loads(event['body'])
        else:
            body = event.get('body', {})
        
        # Validate and parse parameters
        params = parse_request_parameters(body)
        
        # Query and filter logs
        results = query_fpolicy_logs(params)
        
        # Send to SQS
        sqs_count = send_results_to_sqs(results['records'])
        
        # Prepare response
        response_body = {
            'status': 'success',
            'records_found': results['total_found'],
            'records_sent': len(results['records']),
            'sqs_messages_sent': sqs_count,
            'filters': {
                'time_range': {
                    'start': params['start_time'].strftime('%Y-%m-%d %H:%M:%S'),
                    'end': params['end_time'].strftime('%Y-%m-%d %H:%M:%S')
                },
                'path_filter': params.get('path_filter'),
                'operations': params.get('operations'),
                'limit': params.get('limit')
            }
        }
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'  # CORS
            },
            'body': json.dumps(response_body)
        }
        
    except ValueError as e:
        # Validation error
        return {
            'statusCode': 400,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'error',
                'error': 'Invalid request parameters',
                'message': str(e)
            })
        }
    except Exception as e:
        # Internal error
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'headers': {'Content-Type': 'application/json'},
            'body': json.dumps({
                'status': 'error',
                'error': 'Internal server error',
                'message': str(e)
            })
        }


def parse_request_parameters(body: Dict) -> Dict:
    """
    Parse and validate request parameters
    
    Args:
        body: Request body
        
    Returns:
        dict: Validated parameters
        
    Raises:
        ValueError: If parameters are invalid
    """
    # Parse time range
    start_time_str = body.get('start_time')
    end_time_str = body.get('end_time')
    
    if not start_time_str or not end_time_str:
        raise ValueError("start_time and end_time are required")
    
    try:
        start_time = datetime.strptime(start_time_str, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_time_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        raise ValueError("Invalid time format. Use: YYYY-MM-DD HH:MM:SS")
    
    if start_time >= end_time:
        raise ValueError("start_time must be before end_time")
    
    # Check time range (max 7 days)
    if (end_time - start_time).days > 7:
        raise ValueError("Time range cannot exceed 7 days")
    
    # Parse optional parameters
    path_filter = body.get('path_filter')
    operations = body.get('operations')
    limit = body.get('limit', 1000)
    
    # Validate limit
    if limit > MAX_RESULTS:
        raise ValueError(f"Limit cannot exceed {MAX_RESULTS}")
    
    return {
        'start_time': start_time,
        'end_time': end_time,
        'path_filter': path_filter,
        'operations': operations if operations else None,
        'limit': limit
    }


def query_fpolicy_logs(params: Dict) -> Dict:
    """
    Query FPolicy logs from S3 based on filters
    
    Args:
        params: Query parameters
        
    Returns:
        dict: Query results with matching records
    """
    start_time = params['start_time']
    end_time = params['end_time']
    path_filter = params.get('path_filter')
    operations = params.get('operations')
    limit = params['limit']
    
    print(f"Querying logs from {start_time} to {end_time}")
    
    # Get log files for the date range
    log_files = get_log_files_for_date_range(start_time, end_time)
    print(f"Found {len(log_files)} log files to scan")
    
    matching_records = []
    total_scanned = 0
    
    for log_file in log_files:
        if len(matching_records) >= limit:
            break
            
        records = scan_log_file(
            log_file, 
            start_time, 
            end_time, 
            path_filter, 
            operations
        )
        
        total_scanned += records['scanned']
        matching_records.extend(records['matches'])
        
        # Trim to limit
        if len(matching_records) > limit:
            matching_records = matching_records[:limit]
    
    return {
        'total_found': total_scanned,
        'records': matching_records
    }


def get_log_files_for_date_range(start_time: datetime, end_time: datetime) -> List[str]:
    """
    Get list of log files that cover the specified date range
    
    Args:
        start_time: Start of time range
        end_time: End of time range
        
    Returns:
        list: Log file keys
    """
    log_files = []
    current_date = start_time.date()
    end_date = end_time.date()
    
    # Generate list of expected log file names
    while current_date <= end_date:
        log_filename = f"{LOG_FILE_PREFIX}{current_date.strftime('%Y-%m-%d')}.log"
        log_files.append(log_filename)
        current_date += timedelta(days=1)
    
    # Verify files exist in S3
    existing_files = []
    
    try:
        response = s3.list_objects_v2(
            Bucket=S3_ACCESS_POINT_ARN,
            Prefix=LOG_FILE_PREFIX
        )
        
        if 'Contents' in response:
            s3_files = {obj['Key'] for obj in response['Contents']}
            existing_files = [f for f in log_files if f in s3_files]
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        raise
    
    return existing_files


def scan_log_file(
    log_file_key: str,
    start_time: datetime,
    end_time: datetime,
    path_filter: Optional[str],
    operations: Optional[List[str]]
) -> Dict:
    """
    Scan a log file and return matching records
    
    Args:
        log_file_key: S3 object key
        start_time: Start of time range
        end_time: End of time range
        path_filter: Path prefix filter (optional)
        operations: List of operations to include (optional)
        
    Returns:
        dict: Scan results with matches
    """
    try:
        # Read log file
        response = s3.get_object(
            Bucket=S3_ACCESS_POINT_ARN,
            Key=log_file_key
        )
        
        log_content = response['Body'].read().decode('utf-8')
        
        matches = []
        scanned = 0
        
        # Parse each line
        for line in log_content.strip().split('\n'):
            if not line:
                continue
                
            try:
                record = json.loads(line)
                scanned += 1
                
                # Filter by timestamp
                record_time = datetime.strptime(record['timestamp'], '%Y-%m-%d %H:%M:%S')
                if record_time < start_time or record_time > end_time:
                    continue
                
                # Filter by operation
                if operations and record.get('operation') not in operations:
                    continue
                
                # Filter by path
                if path_filter:
                    file_path = record.get('file_path', '')
                    if not file_path.startswith(path_filter):
                        continue
                
                # Record matches all filters
                matches.append(record)
                
            except json.JSONDecodeError:
                print(f"Skipping invalid JSON line: {line[:100]}")
                continue
        
        return {
            'scanned': scanned,
            'matches': matches
        }
        
    except Exception as e:
        print(f"Error scanning log file {log_file_key}: {e}")
        return {
            'scanned': 0,
            'matches': []
        }


def send_results_to_sqs(records: List[Dict]) -> int:
    """
    Send filtered records to SQS queue
    
    Args:
        records: List of log records
        
    Returns:
        int: Number of messages sent
    """
    sent_count = 0
    
    for record in records:
        try:
            # Convert ONTAP path to S3 path
            file_path = record.get('file_path', '')
            s3_path = convert_to_s3_path(file_path)
            
            # Create S3 event message
            message_body = {
                "Records": [{
                    "eventVersion": "2.1",
                    "eventSource": "aws:s3",
                    "eventName": "ObjectCreated:Put",
                    "eventTime": record.get('timestamp'),
                    "s3": {
                        "bucket": {
                            "name": "fsxn-fpolicy-bucket",
                            "arn": S3_ACCESS_POINT_ARN
                        },
                        "object": {
                            "key": s3_path,
                            "size": 0
                        }
                    }
                }]
            }
            
            # Send to SQS
            sqs.send_message(
                QueueUrl=SQS_QUEUE_URL,
                MessageBody=json.dumps(message_body)
            )
            
            sent_count += 1
            
        except Exception as e:
            print(f"Error sending message to SQS: {e}")
            continue
    
    print(f"Sent {sent_count} messages to SQS")
    return sent_count


def convert_to_s3_path(ontap_path: str) -> str:
    """
    Convert ONTAP file path to S3 path
    
    Args:
        ontap_path: ONTAP path (e.g., /vol_onpre/test.dat)
        
    Returns:
        str: S3 path (e.g., test.dat)
    """
    if ontap_path.startswith('/vol_onpre/'):
        return ontap_path[len('/vol_onpre/'):]
    elif ontap_path.startswith('vol_onpre/'):
        return ontap_path[len('vol_onpre/'):]
    else:
        return ontap_path.lstrip('/')


# For local testing
if __name__ == "__main__":
    # Test event
    test_event = {
        'body': json.dumps({
            'start_time': '2026-02-18 00:00:00',
            'end_time': '2026-02-18 23:59:59',
            'operations': ['create'],
            'path_filter': '/vol_onpre/',
            'limit': 100
        })
    }
    
    result = lambda_handler(test_event, None)
    print(json.dumps(json.loads(result['body']), indent=2))
