#!/usr/bin/env python3
"""
Quick script to create the necessary IAM role for Glue and update the job configuration.
"""

import json
import time
import boto3
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

def create_glue_role():
    """Create IAM role for Glue job execution."""
    
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-2'
    )
    
    iam_client = session.client('iam')
    glue_client = session.client('glue')
    sts_client = session.client('sts')
    
    # Get account ID
    account_id = sts_client.get_caller_identity()['Account']
    role_name = 'anime-glue-execution-role'
    role_arn = f'arn:aws:iam::{account_id}:role/{role_name}'
    
    print(f"Setting up IAM role: {role_name}")
    
    try:
        # Check if role already exists
        role_exists = False
        try:
            response = iam_client.get_role(RoleName=role_name)
            print(f"✓ IAM role already exists: {role_arn}")
            role_exists = True
        except iam_client.exceptions.NoSuchEntityException:
            pass  # Role doesn't exist, we'll create it
        
        if not role_exists:
            # Trust policy for Glue service
            trust_policy = {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "glue.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            }
            
            # Create the role
            print(f"Creating IAM role: {role_name}")
            iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="IAM role for anime ETL Glue job execution"
            )
            
            # Attach AWS managed Glue service role policy
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn='arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole'
            )
        
        # Create and attach custom policy for S3 access
        s3_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Action": [
                        "s3:GetObject",
                        "s3:GetObjectVersion",
                        "s3:PutObject",
                        "s3:DeleteObject",
                        "s3:ListBucket"
                    ],
                    "Resource": [
                        "arn:aws:s3:::anime-mvp-data",
                        "arn:aws:s3:::anime-mvp-data/*"
                    ]
                }
            ]
        }
        
        policy_name = f'{role_name}-s3-policy'
        print(f"Adding S3 policy: {policy_name}")
        iam_client.put_role_policy(
            RoleName=role_name,
            PolicyName=policy_name,
            PolicyDocument=json.dumps(s3_policy)
        )
        
        print(f"✓ IAM role configured: {role_arn}")
        if not role_exists:
            print("  - Created new role")
            print("  - Attached AWSGlueServiceRole policy")
        print("  - Updated S3 access policy")
        
        # Wait for role to be available
        if not role_exists:
            print("Waiting for role to be available...")
            time.sleep(10)
        
        return role_arn
        
    except Exception as e:
        print(f"Error creating IAM role: {e}")
        raise

def update_glue_job_role(role_arn):
    """Update the Glue job to use the proper IAM role."""
    
    session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
        region_name='us-east-2'
    )
    
    glue_client = session.client('glue')
    job_name = 'anime-etl-pyspark'
    
    try:
        # Get current job definition
        response = glue_client.get_job(JobName=job_name)
        job_definition = response['Job']
        
        # Update the role
        job_update = {
            'Role': role_arn,
            'Command': job_definition['Command'],
            'DefaultArguments': job_definition.get('DefaultArguments', {}),
            'Description': job_definition.get('Description', ''),
            'GlueVersion': job_definition.get('GlueVersion', '4.0'),
            'WorkerType': job_definition.get('WorkerType', 'G.1X'),
            'NumberOfWorkers': job_definition.get('NumberOfWorkers', 2),
            'MaxRetries': job_definition.get('MaxRetries', 1),
            'Timeout': job_definition.get('Timeout', 60)
        }
        
        print(f"Updating Glue job {job_name} with new role...")
        glue_client.update_job(
            JobName=job_name,
            JobUpdate=job_update
        )
        
        print(f"✓ Successfully updated job {job_name} to use role: {role_arn}")
        
    except Exception as e:
        print(f"Error updating Glue job: {e}")
        raise

def main():
    """Main execution flow."""
    print("=" * 60)
    print("FIXING GLUE JOB IAM ROLE")
    print("=" * 60)
    
    try:
        # Step 1: Create/check IAM role
        role_arn = create_glue_role()
        
        # Step 2: Update Glue job to use the role
        update_glue_job_role(role_arn)
        
        print("=" * 60)
        print("✓ SETUP COMPLETED SUCCESSFULLY")
        print("=" * 60)
        print("The Glue job is now ready to run with the proper IAM role.")
        print(f"Role ARN: {role_arn}")
        
    except Exception as e:
        print("=" * 60)
        print("✗ SETUP FAILED")
        print("=" * 60)
        print(f"Error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())