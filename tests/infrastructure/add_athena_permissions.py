#!/usr/bin/env python3
"""
Add Athena permissions to the anime-mvp user for querying S3 data.
"""

import json
import boto3
from dotenv import load_dotenv
import os
import logging

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def add_athena_permissions():
    """Add Athena permissions to the current user."""
    
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-2'
    )
    
    iam_client = session.client('iam')
    sts_client = session.client('sts')
    
    # Get current user identity
    identity = sts_client.get_caller_identity()
    user_arn = identity['Arn']
    user_name = user_arn.split('/')[-1]
    
    logger.info(f"Current user: {user_name}")
    logger.info(f"User ARN: {user_arn}")
    
    # Define Athena policy
    athena_policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": [
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                    "athena:StopQueryExecution",
                    "athena:GetWorkGroup",
                    "athena:ListQueryExecutions",
                    "athena:CreateDatabase",
                    "athena:CreateTable",
                    "athena:GetDatabase",
                    "athena:GetTable",
                    "athena:ListDatabases",
                    "athena:ListTableMetadata"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow", 
                "Action": [
                    "glue:CreateDatabase",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:CreateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "glue:UpdateTable",
                    "glue:DeleteTable",
                    "glue:GetPartition",
                    "glue:GetPartitions",
                    "glue:CreatePartition",
                    "glue:UpdatePartition",
                    "glue:DeletePartition"
                ],
                "Resource": "*"
            },
            {
                "Effect": "Allow",
                "Action": [
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                    "s3:PutObject",
                    "s3:DeleteObject"
                ],
                "Resource": [
                    "arn:aws:s3:::anime-mvp-data",
                    "arn:aws:s3:::anime-mvp-data/*"
                ]
            }
        ]
    }
    
    policy_name = "AnimeAthenaPolicy"
    
    try:
        # Check if policy already exists
        try:
            response = iam_client.get_user_policy(
                UserName=user_name,
                PolicyName=policy_name
            )
            logger.info("✓ Athena policy already exists")
        except iam_client.exceptions.NoSuchEntityException:
            # Create the policy
            iam_client.put_user_policy(
                UserName=user_name,
                PolicyName=policy_name,
                PolicyDocument=json.dumps(athena_policy)
            )
            logger.info("✅ Athena policy created successfully")
        
        # List current policies to verify
        policies = iam_client.list_user_policies(UserName=user_name)
        logger.info(f"User policies: {policies['PolicyNames']}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to add Athena permissions: {e}")
        return False

def main():
    """Main entry point."""
    logger.info("🚀 Adding Athena permissions...")
    
    success = add_athena_permissions()
    
    if success:
        logger.info("✅ Athena permissions added successfully!")
        logger.info("You can now run: poetry run python test_athena_queries.py")
    else:
        logger.error("❌ Failed to add Athena permissions")
        
    return success

if __name__ == "__main__":
    main()