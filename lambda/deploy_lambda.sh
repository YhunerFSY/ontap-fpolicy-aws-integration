#!/bin/bash
# Lambda Function Deployment Script
# Deploys FPolicy Log Processor Lambda function

set -e

# Configuration
FUNCTION_NAME="fpolicy-log-processor"
ROLE_NAME="lambda-fpolicy-processor-role"
REGION="ap-northeast-1"
ACCOUNT_ID="178625946981"

# S3 Access Point and SQS configuration
S3_ACCESS_POINT_ARN="arn:aws:s3:${REGION}:${ACCOUNT_ID}:accesspoint/fsxn-fpolicy-log-bucket"
SQS_QUEUE_URL="https://sqs.${REGION}.amazonaws.com/${ACCOUNT_ID}/FPolicy_Q"
LOG_FILE_PREFIX="fpolicy_"

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}FPolicy Log Processor Deployment${NC}"
echo -e "${GREEN}========================================${NC}"

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI is not installed${NC}"
    exit 1
fi

# Check AWS credentials
echo -e "${YELLOW}Checking AWS credentials...${NC}"
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    exit 1
fi
echo -e "${GREEN}✓ AWS credentials OK${NC}"

# Create IAM role if it doesn't exist
echo -e "\n${YELLOW}Checking IAM role...${NC}"
if ! aws iam get-role --role-name ${ROLE_NAME} &> /dev/null; then
    echo "Creating IAM role: ${ROLE_NAME}"
    
    # Create trust policy
    cat > /tmp/trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Service": "lambda.amazonaws.com"
    },
    "Action": "sts:AssumeRole"
  }]
}
EOF

    # Create role
    aws iam create-role \
      --role-name ${ROLE_NAME} \
      --assume-role-policy-document file:///tmp/trust-policy.json \
      --region ${REGION}
    
    # Create and attach policy
    cat > /tmp/role-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:/aws/lambda/${FUNCTION_NAME}:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "${S3_ACCESS_POINT_ARN}",
        "${S3_ACCESS_POINT_ARN}/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage"
      ],
      "Resource": "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:FPolicy_Q"
    }
  ]
}
EOF

    aws iam put-role-policy \
      --role-name ${ROLE_NAME} \
      --policy-name fpolicy-processor-policy \
      --policy-document file:///tmp/role-policy.json
    
    echo -e "${GREEN}✓ IAM role created${NC}"
    
    # Wait for role to propagate
    echo "Waiting for IAM role to propagate (10 seconds)..."
    sleep 10
else
    echo -e "${GREEN}✓ IAM role exists${NC}"
fi

# Package Lambda function
echo -e "\n${YELLOW}Packaging Lambda function...${NC}"
cd /mnt/user-data/outputs
zip -q lambda_fpolicy_processor.zip lambda_fpolicy_processor.py
echo -e "${GREEN}✓ Lambda package created${NC}"

# Create or update Lambda function
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo -e "\n${YELLOW}Deploying Lambda function...${NC}"
if aws lambda get-function --function-name ${FUNCTION_NAME} --region ${REGION} &> /dev/null; then
    echo "Updating existing Lambda function..."
    aws lambda update-function-code \
      --function-name ${FUNCTION_NAME} \
      --zip-file fileb://lambda_fpolicy_processor.zip \
      --region ${REGION} > /dev/null
    
    aws lambda update-function-configuration \
      --function-name ${FUNCTION_NAME} \
      --timeout 300 \
      --memory-size 512 \
      --environment Variables="{S3_ACCESS_POINT_ARN=${S3_ACCESS_POINT_ARN},SQS_QUEUE_URL=${SQS_QUEUE_URL},LOG_FILE_PREFIX=${LOG_FILE_PREFIX}}" \
      --region ${REGION} > /dev/null
    
    echo -e "${GREEN}✓ Lambda function updated${NC}"
else
    echo "Creating new Lambda function..."
    aws lambda create-function \
      --function-name ${FUNCTION_NAME} \
      --runtime python3.11 \
      --role ${ROLE_ARN} \
      --handler lambda_fpolicy_processor.lambda_handler \
      --zip-file fileb://lambda_fpolicy_processor.zip \
      --timeout 300 \
      --memory-size 512 \
      --environment Variables="{S3_ACCESS_POINT_ARN=${S3_ACCESS_POINT_ARN},SQS_QUEUE_URL=${SQS_QUEUE_URL},LOG_FILE_PREFIX=${LOG_FILE_PREFIX}}" \
      --region ${REGION} > /dev/null
    
    echo -e "${GREEN}✓ Lambda function created${NC}"
fi

# Create EventBridge schedule
RULE_NAME="fpolicy-log-processor-schedule"
echo -e "\n${YELLOW}Configuring EventBridge schedule...${NC}"

if ! aws events describe-rule --name ${RULE_NAME} --region ${REGION} &> /dev/null; then
    echo "Creating EventBridge rule (every 5 minutes)..."
    
    aws events put-rule \
      --name ${RULE_NAME} \
      --schedule-expression "rate(5 minutes)" \
      --state ENABLED \
      --region ${REGION} > /dev/null
    
    # Add Lambda as target
    LAMBDA_ARN="arn:aws:lambda:${REGION}:${ACCOUNT_ID}:function:${FUNCTION_NAME}"
    
    aws events put-targets \
      --rule ${RULE_NAME} \
      --targets "Id"="1","Arn"="${LAMBDA_ARN}" \
      --region ${REGION} > /dev/null
    
    # Grant EventBridge permission to invoke Lambda
    aws lambda add-permission \
      --function-name ${FUNCTION_NAME} \
      --statement-id EventBridgeInvoke \
      --action lambda:InvokeFunction \
      --principal events.amazonaws.com \
      --source-arn arn:aws:events:${REGION}:${ACCOUNT_ID}:rule/${RULE_NAME} \
      --region ${REGION} &> /dev/null || true
    
    echo -e "${GREEN}✓ EventBridge schedule created${NC}"
else
    echo -e "${GREEN}✓ EventBridge schedule exists${NC}"
fi

# Test Lambda function
echo -e "\n${YELLOW}Testing Lambda function...${NC}"
aws lambda invoke \
  --function-name ${FUNCTION_NAME} \
  --region ${REGION} \
  /tmp/lambda-response.json > /dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Lambda function test successful${NC}"
    echo -e "\nResponse:"
    cat /tmp/lambda-response.json | jq .
else
    echo -e "${RED}✗ Lambda function test failed${NC}"
fi

# Cleanup
rm -f /tmp/trust-policy.json /tmp/role-policy.json /tmp/lambda-response.json

echo -e "\n${GREEN}========================================${NC}"
echo -e "${GREEN}Deployment Complete!${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "\nLambda Function: ${FUNCTION_NAME}"
echo -e "Region: ${REGION}"
echo -e "Schedule: Every 5 minutes"
echo -e "\nView logs:"
echo -e "  aws logs tail /aws/lambda/${FUNCTION_NAME} --follow --region ${REGION}"
echo -e "\nManual invoke:"
echo -e "  aws lambda invoke --function-name ${FUNCTION_NAME} --region ${REGION} response.json"
echo ""
