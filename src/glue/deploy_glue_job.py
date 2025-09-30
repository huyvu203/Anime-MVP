#!/usr/bin/env python3
"""
AWS Glue Job Deployment Script

This script handles the deployment and management of the Anime ETL job in AWS Glue.
It creates, updates, and manages the Glue job configuration while keeping the
ETL logic separate in anime_etl.py.

Features:
- Create new Glue jobs
- Update existing job configurations
- Upload ETL scripts to S3
- Manage job parameters and settings
- Monitor job execution
"""

import boto3
import json
import os
import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GlueJobDeployer:
    """
    Handles deployment and management of AWS Glue jobs for anime data processing.
    Authenticates with the caller's credentials while targeting a dedicated Glue IAM role.
    """
    
    def __init__(self, aws_region: str = 'us-east-2', profile: str = None):
        """
        Initialize the Glue job deployer.
        
        Args:
            aws_region: AWS region for Glue job
            profile: AWS profile to use (optional)
        """
        self.region = aws_region
        
        # Load AWS credentials from environment
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not aws_access_key or not aws_secret_key:
            logger.warning("AWS credentials not found in .env file, using default AWS profile")
        
        # Initialize AWS clients with credentials from .env or profile
        if aws_access_key and aws_secret_key:
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=aws_region
            )
        else:
            session = boto3.Session(profile_name=profile) if profile else boto3.Session()
        
        self.glue_client = session.client('glue', region_name=aws_region)
        self.s3_client = session.client('s3', region_name=aws_region)
        self.sts_client = session.client('sts', region_name=aws_region)
        
        # Get current user ARN for Glue role
        try:
            caller_identity = self.sts_client.get_caller_identity()
            self.account_id = caller_identity['Account']
            self.user_arn = caller_identity['Arn']
            logger.info(f"Using AWS user: {self.user_arn}")
        except Exception as e:
            logger.error(f"Failed to get caller identity: {e}")
            raise
        
        # Determine which IAM role to use for Glue execution
        default_role_arn = f"arn:aws:iam::{self.account_id}:role/anime-glue-execution-role"
        configured_role_arn = os.getenv('GLUE_EXECUTION_ROLE_ARN', '').strip()

        if configured_role_arn:
            self.execution_role_arn = configured_role_arn
        else:
            self.execution_role_arn = default_role_arn

        # Guard against accidentally using a user ARN
        if ':user/' in self.execution_role_arn:
            logger.warning(
                "Execution role ARN resolved to an IAM user. Falling back to Glue execution role "
                f"{default_role_arn}."
            )
            self.execution_role_arn = default_role_arn

        logger.info(f"Using Glue execution role: {self.execution_role_arn}")
        
        # Default configuration - ensures Glue job always references the execution role
        self.default_config = {
            'job_name': 'anime-etl-pyspark',
            'execution_role_arn': self.execution_role_arn,
            'script_bucket': 'anime-mvp-data',
            'script_key': 'scripts/anime_etl.py',
            'temp_bucket': 'anime-mvp-data',
            'temp_prefix': 'temp/',
            'glue_version': '4.0',
            'worker_type': 'G.1X',
            'number_of_workers': 2,
            'timeout': 60,
            'max_retries': 1
        }
        
        logger.info(f"GlueJobDeployer initialized for region:  {aws_region}")
        logger.info(f"Account ID: {self.account_id}")
    
    def check_user_permissions(self) -> bool:
        """
        Check if the current user has necessary permissions for Glue operations.
        
        Returns:
            True if user has required permissions
        """
        logger.info("Checking user permissions for Glue operations...")
        
        try:
            # Test Glue permissions by listing jobs
            self.glue_client.get_jobs(MaxResults=1)
            logger.info("✓ User has Glue read permissions")
            
            # Test S3 permissions by listing buckets
            self.s3_client.list_buckets()
            logger.info("✓ User has S3 permissions")
            
            return True
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code in ['AccessDenied', 'UnauthorizedOperation']:
                logger.error(f"✗ Insufficient permissions: {e}")
                logger.error("User needs the following permissions:")
                logger.error("  - glue:CreateJob, glue:UpdateJob, glue:GetJob, glue:StartJobRun")
                logger.error("  - s3:GetObject, s3:PutObject, s3:ListBucket")
                return False
            else:
                logger.warning(f"Permission check inconclusive: {e}")
                return True  # Assume permissions are OK
    
    def upload_etl_script(self, local_script_path: str, bucket: str, key: str) -> str:
        """
        Upload the ETL script to S3.
        
        Args:
            local_script_path: Path to local ETL script
            bucket: S3 bucket name
            key: S3 object key
            
        Returns:
            S3 URI of uploaded script
        """
        logger.info(f"Uploading ETL script to s3://{bucket}/{key}")
        
        try:
            # Check if script exists locally
            if not os.path.exists(local_script_path):
                raise FileNotFoundError(f"ETL script not found: {local_script_path}")
            
            # Upload to S3
            self.s3_client.upload_file(local_script_path, bucket, key)
            s3_uri = f"s3://{bucket}/{key}"
            
            logger.info(f"✓ ETL script uploaded to: {s3_uri}")
            return s3_uri
            
        except Exception as e:
            logger.error(f"Failed to upload ETL script: {e}")
            raise
    
    def create_glue_job(self, config: Dict) -> str:
        """
        Create a new Glue job with the specified configuration.
        
        Args:
            config: Job configuration dictionary
            
        Returns:
            Name of the created job
        """
        job_name = config['job_name']
        logger.info(f"Creating Glue job: {job_name}")
        
        try:
            # Construct job parameters
            job_definition = {
                'Name': job_name,
                'Role': config['execution_role_arn'],  # Glue execution IAM role
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f"s3://{config['script_bucket']}/{config['script_key']}",
                    'PythonVersion': '3.9'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--enable-metrics': '',
                    '--enable-spark-ui': 'true',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--TempDir': f"s3://{config['temp_bucket']}/{config['temp_prefix']}",
                    '--input_path': config.get('input_path', 's3://anime-mvp-data/raw'),
                    '--output_path': config.get('output_path', 's3://anime-mvp-data/processed'),
                    '--write_mode': config.get('write_mode', 'overwrite'),
                    '--output_format': config.get('output_format', 'parquet')
                },
                'MaxRetries': config['max_retries'],
                'Timeout': config['timeout'],
                'GlueVersion': config['glue_version'],
                'WorkerType': config['worker_type'],
                'NumberOfWorkers': config['number_of_workers'],
                'Description': 'PySpark ETL job for processing anime data from Jikan API'
            }
            
            # Add tags
            if 'tags' in config:
                job_definition['Tags'] = config['tags']
            else:
                job_definition['Tags'] = {
                    'Project': 'anime-mvp',
                    'Environment': 'production',
                    'ETL': 'anime-data'
                }
            
            # Create the job
            self.glue_client.create_job(**job_definition)
            logger.info(f"✓ Glue job '{job_name}' created successfully")
            
            return job_name
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'AlreadyExistsException':
                logger.warning(f"Job '{job_name}' already exists")
                return job_name
            else:
                logger.error(f"Failed to create Glue job: {e}")
                raise
    
    def update_glue_job(self, config: Dict) -> str:
        """
        Update an existing Glue job configuration.
        
        Args:
            config: Updated job configuration
            
        Returns:
            Name of the updated job
        """
        job_name = config['job_name']
        logger.info(f"Updating Glue job: {job_name}")
        
        try:
            # Get current job definition
            current_job = self.glue_client.get_job(JobName=job_name)['Job']
            
            # Update job definition
            job_update = {
                'Role': config['execution_role_arn'],  # Glue execution IAM role
                'Command': {
                    'Name': 'glueetl',
                    'ScriptLocation': f"s3://{config['script_bucket']}/{config['script_key']}",
                    'PythonVersion': '3.9'
                },
                'DefaultArguments': {
                    '--job-language': 'python',
                    '--enable-metrics': '',
                    '--enable-spark-ui': 'true',
                    '--enable-continuous-cloudwatch-log': 'true',
                    '--TempDir': f"s3://{config['temp_bucket']}/{config['temp_prefix']}",
                    '--input_path': config.get('input_path', 's3://anime-mvp-data/raw'),
                    '--output_path': config.get('output_path', 's3://anime-mvp-data/processed'),
                    '--write_mode': config.get('write_mode', 'overwrite'),
                    '--output_format': config.get('output_format', 'parquet')
                },
                'MaxRetries': config['max_retries'],
                'Timeout': config['timeout'],
                'GlueVersion': config['glue_version'],
                'WorkerType': config['worker_type'],
                'NumberOfWorkers': config['number_of_workers'],
                'Description': 'PySpark ETL job for processing anime data from Jikan API'
            }
            
            # Update the job
            self.glue_client.update_job(JobName=job_name, JobUpdate=job_update)
            logger.info(f"✓ Glue job '{job_name}' updated successfully")
            
            return job_name
            
        except ClientError as e:
            logger.error(f"Failed to update Glue job: {e}")
            raise
    
    def start_job_run(self, job_name: str, parameters: Dict = None) -> str:
        """
        Start a job run for the specified Glue job.
        
        Args:
            job_name: Name of the Glue job
            parameters: Optional job run parameters
            
        Returns:
            Job run ID
        """
        logger.info(f"Starting job run for: {job_name}")
        
        try:
            run_args = {'JobName': job_name}
            
            if parameters:
                run_args['Arguments'] = parameters
            
            response = self.glue_client.start_job_run(**run_args)
            job_run_id = response['JobRunId']
            
            logger.info(f"✓ Started job run: {job_run_id}")
            return job_run_id
            
        except ClientError as e:
            logger.error(f"Failed to start job run: {e}")
            raise
    
    def get_job_run_status(self, job_name: str, job_run_id: str) -> Dict:
        """
        Get the status of a job run.
        
        Args:
            job_name: Name of the Glue job
            job_run_id: Job run ID
            
        Returns:
            Job run details
        """
        try:
            response = self.glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            return response['JobRun']
        except ClientError as e:
            logger.error(f"Failed to get job run status: {e}")
            raise
    
    def delete_job(self, job_name: str) -> bool:
        """
        Delete a Glue job.
        
        Args:
            job_name: Name of the job to delete
            
        Returns:
            True if successful
        """
        logger.info(f"Deleting Glue job: {job_name}")
        
        try:
            self.glue_client.delete_job(JobName=job_name)
            logger.info(f"✓ Deleted job: {job_name}")
            return True
        except ClientError as e:
            logger.error(f"Failed to delete job: {e}")
            raise
    
    def deploy_full_stack(self, config: Dict = None, force_update: bool = False) -> Dict:
        """
        Deploy the complete Glue job stack (permission check, script upload, job creation).
        
        Args:
            config: Optional configuration overrides
            force_update: Whether to update existing resources
            
        Returns:
            Deployment results
        """
        logger.info("=" * 60)
        logger.info("DEPLOYING ANIME ETL GLUE JOB")
        logger.info("=" * 60)
        
        # Merge with default configuration
        deployment_config = self.default_config.copy()
        if config:
            deployment_config.update(config)
        
        results = {'status': 'in_progress', 'components': {}}
        
        try:
            # 1. Check User Permissions
            if self.check_user_permissions():
                results['components']['permissions'] = {
                    'status': 'verified', 
                    'user_arn': self.user_arn
                }
            else:
                raise Exception("Insufficient permissions for Glue operations")
            
            # 2. Upload ETL Script
            script_path = os.path.join(os.path.dirname(__file__), 'anime_etl.py')
            if not os.path.exists(script_path):
                # Try alternative path
                script_path = 'src/glue/anime_etl.py'
            
            script_uri = self.upload_etl_script(
                script_path,
                deployment_config['script_bucket'],
                deployment_config['script_key']
            )
            results['components']['script_upload'] = {'status': 'uploaded', 'uri': script_uri}
            
            # 3. Create/Update Glue Job
            job_name = deployment_config['job_name']
            try:
                # Check if job exists
                self.glue_client.get_job(JobName=job_name)
                
                if force_update:
                    job_name = self.update_glue_job(deployment_config)
                    results['components']['glue_job'] = {'status': 'updated', 'name': job_name}
                else:
                    logger.info(f"Job '{job_name}' already exists (use --force-update to update)")
                    results['components']['glue_job'] = {'status': 'exists', 'name': job_name}
                    
            except ClientError as e:
                if e.response['Error']['Code'] == 'EntityNotFoundException':
                    # Job doesn't exist, create it
                    job_name = self.create_glue_job(deployment_config)
                    results['components']['glue_job'] = {'status': 'created', 'name': job_name}
                else:
                    raise
            
            results['status'] = 'completed'
            logger.info("=" * 60)
            logger.info("✓ DEPLOYMENT COMPLETED SUCCESSFULLY")
            logger.info("=" * 60)
            
            # Print summary
            logger.info("Deployment Summary:")
            for component, details in results['components'].items():
                logger.info(f"  {component}: {details['status']}")
            
            return results
            
        except Exception as e:
            results['status'] = 'failed'
            results['error'] = str(e)
            logger.error("=" * 60)
            logger.error("✗ DEPLOYMENT FAILED")
            logger.error(f"Error: {e}")
            logger.error("=" * 60)
            raise


def main():
    """Main CLI interface for the deployment script."""
    parser = argparse.ArgumentParser(description='Deploy AWS Glue job for anime data processing')
    
    parser.add_argument('action', choices=['deploy', 'update', 'run', 'status', 'delete'],
                       help='Action to perform')
    parser.add_argument('--job-name', default='anime-etl-pyspark',
                       help='Name of the Glue job')
    parser.add_argument('--region', default='us-east-2',
                       help='AWS region')
    parser.add_argument('--profile', help='AWS profile to use')
    parser.add_argument('--force-update', action='store_true',
                       help='Force update existing resources')
    parser.add_argument('--config', help='Path to configuration JSON file')
    parser.add_argument('--run-id', help='Job run ID for status check')
    
    args = parser.parse_args()
    
    # Initialize deployer
    deployer = GlueJobDeployer(aws_region=args.region, profile=args.profile)
    
    # Load additional configuration if provided
    config_overrides = {}
    if args.config and os.path.exists(args.config):
        with open(args.config, 'r') as f:
            config_overrides = json.load(f)
    
    if args.job_name != 'anime-etl-pyspark':
        config_overrides['job_name'] = args.job_name
    
    try:
        if args.action == 'deploy':
            results = deployer.deploy_full_stack(config_overrides, args.force_update)
            
        elif args.action == 'update':
            job_name = deployer.update_glue_job({**deployer.default_config, **config_overrides})
            logger.info(f"Updated job: {job_name}")
            
        elif args.action == 'run':
            job_run_id = deployer.start_job_run(args.job_name)
            logger.info(f"Started job run: {job_run_id}")
            
        elif args.action == 'status':
            if not args.run_id:
                logger.error("--run-id required for status check")
                sys.exit(1)
            status = deployer.get_job_run_status(args.job_name, args.run_id)
            logger.info(f"Job run status: {status['JobRunState']}")
            
        elif args.action == 'delete':
            deployer.delete_job(args.job_name)
            
    except Exception as e:
        logger.error(f"Operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()