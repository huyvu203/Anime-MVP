#!/usr/bin/env python3
"""
End-to-End Glue Job Deployment Test

This script tests the complete pipeline from deployment to execution:
1. Check S3 data availability
2. Create subset of test data
3. Deploy Glue job
4. Execute the job with test data
5. Validate outputs
6. Clean up

Usage:
    poetry run python test_glue_deployment.py
    poetry run python test_glue_deployment.py --region us-west-2
    poetry run python test_glue_deployment.py --test-size 50 --timeout 20

WARNING: This will incur AWS Glue execution costs!
"""

import os
import sys
import time
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Add src directory for imports
current_dir = Path(__file__).parent
src_dir = current_dir / "src"
sys.path.insert(0, str(src_dir))

from glue.deploy_glue_job import GlueJobDeployer

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GlueJobTester:
    """
    Comprehensive tester for Glue job deployment and execution.
    """
    
    def __init__(self, aws_region: str = 'us-east-2'):
        """
        Initialize the Glue job tester.
        
        Args:
            aws_region: AWS region for resources
        """
        self.region = aws_region
        
        # Load AWS credentials
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        
        if not aws_access_key or not aws_secret_key:
            raise ValueError("AWS credentials not found in .env file")
        
        # Initialize AWS clients
        self.session = boto3.Session(
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
        self.s3_client = self.session.client('s3')
        self.glue_client = self.session.client('glue')
        
        # Initialize deployer
        self.deployer = GlueJobDeployer(aws_region)
        
        # Test configuration
        self.test_config = {
            'job_name': 'anime-etl-test',
            'script_bucket': 'anime-mvp-data',
            'script_key': 'scripts/anime_etl.py',
            'input_bucket': 'anime-mvp-data',
            'input_prefix': 'raw',
            'output_bucket': 'anime-mvp-data',
            'output_prefix': 'processed-test',
            'temp_bucket': 'anime-mvp-data',
            'temp_prefix': 'temp/',
            'glue_version': '4.0',
            'worker_type': 'G.1X',
            'number_of_workers': 2,
            'timeout': 30,  # Shorter timeout for testing
            'max_retries': 1
        }
        
        logger.info(f"GlueJobTester initialized for region: {aws_region}")
    
    def check_s3_data_availability(self) -> Dict[str, List[str]]:
        """
        Check what data is available in S3 for testing.
        
        Returns:
            Dictionary mapping data types to available files
        """
        logger.info("Checking S3 data availability...")
        
        bucket = self.test_config['input_bucket']
        prefix = self.test_config['input_prefix']
        
        data_types = [
            'anime_details',
            'anime_statistics',
            'anime_recommendations', 
            'genres',
            'top_anime',
            'seasonal_anime'
        ]
        
        available_data = {}
        total_files = 0
        
        for data_type in data_types:
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=f"{prefix}/{data_type}/",
                    MaxKeys=10  # Limit for testing
                )
                
                files = []
                if 'Contents' in response:
                    files = [obj['Key'] for obj in response['Contents'] 
                           if obj['Key'].endswith('.json')]
                
                available_data[data_type] = files[:5]  # Limit to 5 files per type
                total_files += len(available_data[data_type])
                
                logger.info(f"  {data_type}: {len(available_data[data_type])} files")
                
            except ClientError as e:
                logger.warning(f"  {data_type}: Error accessing - {e}")
                available_data[data_type] = []
        
        logger.info(f"Total files available for testing: {total_files}")
        
        if total_files == 0:
            raise Exception("No data files found in S3. Run data ingestion first.")
        
        return available_data
    
    def create_test_subset(self, available_data: Dict[str, List[str]]) -> str:
        """
        Create a subset of data for testing by copying files to a test prefix.
        
        Args:
            available_data: Dictionary of available data files
            
        Returns:
            Test input prefix
        """
        logger.info("Creating test data subset...")
        
        source_bucket = self.test_config['input_bucket']
        test_prefix = f"{self.test_config['input_prefix']}-test"
        
        total_copied = 0
        
        for data_type, files in available_data.items():
            if not files:
                continue
                
            # Take up to 3 files per data type for testing
            test_files = files[:3]
            
            for source_key in test_files:
                # Create test copy
                target_key = source_key.replace(
                    self.test_config['input_prefix'],
                    test_prefix
                )
                
                try:
                    self.s3_client.copy_object(
                        CopySource={'Bucket': source_bucket, 'Key': source_key},
                        Bucket=source_bucket,
                        Key=target_key
                    )
                    total_copied += 1
                    
                except ClientError as e:
                    logger.warning(f"Failed to copy {source_key}: {e}")
        
        logger.info(f"Created test subset with {total_copied} files at s3://{source_bucket}/{test_prefix}")
        return test_prefix
    
    def deploy_test_job(self) -> str:
        """
        Deploy the Glue job for testing.
        
        Returns:
            Job name
        """
        logger.info("Deploying test Glue job...")
        
        # Update configuration for test job
        deploy_config = self.deployer.default_config.copy()
        deploy_config.update({
            'job_name': self.test_config['job_name'],
            'timeout': self.test_config['timeout'],
            'worker_type': self.test_config['worker_type'],
            'number_of_workers': self.test_config['number_of_workers']
        })
        
        try:
            results = self.deployer.deploy_full_stack(deploy_config, force_update=True)
            
            if results['status'] == 'completed':
                logger.info("✓ Test Glue job deployed successfully")
                return self.test_config['job_name']
            else:
                raise Exception(f"Deployment failed: {results}")
                
        except Exception as e:
            logger.error(f"Failed to deploy test job: {e}")
            raise
    
    def run_test_job(self, test_input_prefix: str) -> str:
        """
        Execute the test Glue job with test data.
        
        Args:
            test_input_prefix: S3 prefix containing test data
            
        Returns:
            Job run ID
        """
        logger.info("Starting test Glue job execution...")
        
        job_name = self.test_config['job_name']
        
        # Job parameters for test run
        job_params = {
            '--input_path': f"s3://{self.test_config['input_bucket']}/{test_input_prefix}",
            '--output_path': f"s3://{self.test_config['output_bucket']}/{self.test_config['output_prefix']}",
            '--write_mode': 'overwrite',
            '--output_format': 'parquet'
        }
        
        try:
            job_run_id = self.deployer.start_job_run(job_name, job_params)
            logger.info(f"✓ Test job started with run ID: {job_run_id}")
            return job_run_id
            
        except Exception as e:
            logger.error(f"Failed to start test job: {e}")
            raise
    
    def monitor_job_execution(self, job_name: str, job_run_id: str, timeout_minutes: int = 20) -> Dict:
        """
        Monitor job execution until completion or timeout.
        
        Args:
            job_name: Name of the Glue job
            job_run_id: Job run ID to monitor
            timeout_minutes: Maximum time to wait
            
        Returns:
            Final job run status
        """
        logger.info(f"Monitoring job execution (timeout: {timeout_minutes} minutes)...")
        
        start_time = datetime.now()
        timeout = timedelta(minutes=timeout_minutes)
        
        while datetime.now() - start_time < timeout:
            try:
                job_run = self.deployer.get_job_run_status(job_name, job_run_id)
                state = job_run['JobRunState']
                
                logger.info(f"Job state: {state}")
                
                # Check for completion states
                if state in ['SUCCEEDED', 'FAILED', 'STOPPED', 'TIMEOUT']:
                    if state == 'SUCCEEDED':
                        duration = (datetime.now() - start_time).total_seconds()
                        logger.info(f"✓ Job completed successfully in {duration:.1f} seconds")
                    else:
                        logger.error(f"✗ Job ended with state: {state}")
                        if 'ErrorMessage' in job_run:
                            logger.error(f"Error: {job_run['ErrorMessage']}")
                    
                    return job_run
                
                # Wait before next check
                time.sleep(30)
                
            except Exception as e:
                logger.error(f"Error monitoring job: {e}")
                break
        
        # Timeout reached
        logger.error(f"✗ Job monitoring timed out after {timeout_minutes} minutes")
        try:
            return self.deployer.get_job_run_status(job_name, job_run_id)
        except:
            return {'JobRunState': 'TIMEOUT'}
    
    def validate_output_data(self) -> Dict[str, Dict]:
        """
        Validate the output data produced by the job.
        
        Returns:
            Validation results
        """
        logger.info("Validating output data...")
        
        bucket = self.test_config['output_bucket']
        prefix = self.test_config['output_prefix']
        
        # Expected output tables
        expected_tables = [
            'anime',
            'anime_genres',
            'anime_studios',
            'anime_producers',
            'anime_themes',
            'anime_demographics',
            'anime_relations',
            'genres_master',
            'top_anime',
            'seasonal_anime'
        ]
        
        validation_results = {}
        
        for table in expected_tables:
            table_prefix = f"{prefix}/{table}/"
            
            try:
                # Check if table exists
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=table_prefix,
                    MaxKeys=10
                )
                
                if 'Contents' in response:
                    files = [obj for obj in response['Contents'] 
                           if obj['Key'].endswith('.parquet')]
                    
                    if files:
                        total_size = sum(obj['Size'] for obj in files)
                        validation_results[table] = {
                            'exists': True,
                            'file_count': len(files),
                            'total_size_bytes': total_size,
                            'files': [obj['Key'] for obj in files]
                        }
                        logger.info(f"✓ {table}: {len(files)} files, {total_size:,} bytes")
                    else:
                        validation_results[table] = {'exists': False, 'reason': 'no_parquet_files'}
                        logger.warning(f"✗ {table}: No parquet files found")
                else:
                    validation_results[table] = {'exists': False, 'reason': 'no_objects'}
                    logger.warning(f"✗ {table}: No objects found")
                    
            except ClientError as e:
                validation_results[table] = {'exists': False, 'error': str(e)}
                logger.error(f"✗ {table}: Error checking - {e}")
        
        # Summary
        successful_tables = sum(1 for r in validation_results.values() if r.get('exists', False))
        total_size = sum(r.get('total_size_bytes', 0) for r in validation_results.values())
        
        logger.info(f"Validation Summary:")
        logger.info(f"  Successful tables: {successful_tables}/{len(expected_tables)}")
        logger.info(f"  Total output size: {total_size:,} bytes")
        
        return validation_results
    
    def cleanup_test_resources(self, test_input_prefix: str, keep_job: bool = False):
        """
        Clean up test resources.
        
        Args:
            test_input_prefix: Test input prefix to clean up
            keep_job: Whether to keep the test job (for debugging)
        """
        logger.info("Cleaning up test resources...")
        
        try:
            # Clean up test input data
            bucket = self.test_config['input_bucket']
            
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=test_input_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    logger.info(f"✓ Deleted {len(objects_to_delete)} test input files")
            
            # Clean up test output data
            output_bucket = self.test_config['output_bucket']
            output_prefix = self.test_config['output_prefix']
            
            response = self.s3_client.list_objects_v2(
                Bucket=output_bucket,
                Prefix=output_prefix
            )
            
            if 'Contents' in response:
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                if objects_to_delete:
                    self.s3_client.delete_objects(
                        Bucket=output_bucket,
                        Delete={'Objects': objects_to_delete}
                    )
                    logger.info(f"✓ Deleted {len(objects_to_delete)} test output files")
            
            # Delete test job
            if not keep_job:
                try:
                    self.deployer.delete_job(self.test_config['job_name'])
                    logger.info("✓ Deleted test Glue job")
                except Exception as e:
                    logger.warning(f"Could not delete test job: {e}")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")
    
    def run_full_test(self, cleanup: bool = True, keep_job: bool = False) -> Dict:
        """
        Run the complete end-to-end test.
        
        Args:
            cleanup: Whether to clean up resources after test
            keep_job: Whether to keep the Glue job for debugging
            
        Returns:
            Test results
        """
        logger.info("=" * 80)
        logger.info("GLUE JOB END-TO-END TEST")
        logger.info("=" * 80)
        
        test_results = {
            'start_time': datetime.now(),
            'status': 'running',
            'phases': {}
        }
        
        test_input_prefix = None
        
        try:
            # Phase 1: Check S3 data availability
            logger.info("Phase 1: Checking S3 data availability")
            available_data = self.check_s3_data_availability()
            test_results['phases']['data_check'] = {
                'status': 'completed',
                'available_data': available_data
            }
            
            # Phase 2: Create test data subset
            logger.info("Phase 2: Creating test data subset")
            test_input_prefix = self.create_test_subset(available_data)
            test_results['phases']['data_subset'] = {
                'status': 'completed',
                'test_prefix': test_input_prefix
            }
            
            # Phase 3: Deploy Glue job
            logger.info("Phase 3: Deploying Glue job")
            job_name = self.deploy_test_job()
            test_results['phases']['job_deployment'] = {
                'status': 'completed',
                'job_name': job_name
            }
            
            # Phase 4: Execute job
            logger.info("Phase 4: Executing Glue job")
            job_run_id = self.run_test_job(test_input_prefix)
            test_results['phases']['job_execution'] = {
                'status': 'started',
                'job_run_id': job_run_id
            }
            
            # Phase 5: Monitor execution
            logger.info("Phase 5: Monitoring job execution")
            job_run_status = self.monitor_job_execution(job_name, job_run_id)
            test_results['phases']['job_monitoring'] = {
                'status': 'completed',
                'final_state': job_run_status['JobRunState'],
                'job_run_details': job_run_status
            }
            
            # Phase 6: Validate output (only if job succeeded)
            if job_run_status['JobRunState'] == 'SUCCEEDED':
                logger.info("Phase 6: Validating output data")
                validation_results = self.validate_output_data()
                test_results['phases']['output_validation'] = {
                    'status': 'completed',
                    'results': validation_results
                }
                
                # Determine overall success
                successful_tables = sum(1 for r in validation_results.values() 
                                      if r.get('exists', False))
                if successful_tables > 0:
                    test_results['status'] = 'success'
                else:
                    test_results['status'] = 'partial_success'
            else:
                test_results['status'] = 'failed'
                test_results['failure_reason'] = f"Job failed with state: {job_run_status['JobRunState']}"
            
            # Calculate duration
            test_results['end_time'] = datetime.now()
            test_results['duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
            
            # Print results
            self._print_test_summary(test_results)
            
            return test_results
            
        except Exception as e:
            test_results['end_time'] = datetime.now()
            test_results['duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
            test_results['status'] = 'error'
            test_results['error'] = str(e)
            
            logger.error("=" * 80)
            logger.error("✗ TEST FAILED")
            logger.error(f"Error: {e}")
            logger.error("=" * 80)
            
            return test_results
            
        finally:
            # Cleanup
            if cleanup and test_input_prefix:
                self.cleanup_test_resources(test_input_prefix, keep_job)
    
    def _print_test_summary(self, results: Dict):
        """Print a summary of test results."""
        logger.info("=" * 80)
        
        if results['status'] == 'success':
            logger.info("✓ END-TO-END TEST COMPLETED SUCCESSFULLY")
        elif results['status'] == 'partial_success':
            logger.info("⚠ END-TO-END TEST COMPLETED WITH WARNINGS")
        else:
            logger.info("✗ END-TO-END TEST FAILED")
        
        logger.info("=" * 80)
        
        logger.info(f"Test Duration: {results['duration']:.1f} seconds")
        
        # Phase summary
        logger.info("Phase Results:")
        for phase, details in results['phases'].items():
            status_icon = "✓" if details['status'] == 'completed' else "✗"
            logger.info(f"  {status_icon} {phase}: {details['status']}")
        
        # Output validation summary
        if 'output_validation' in results['phases']:
            validation = results['phases']['output_validation']['results']
            successful = sum(1 for r in validation.values() if r.get('exists', False))
            total = len(validation)
            logger.info(f"Output Tables: {successful}/{total} successful")


def main():
    """Main entry point for the test script."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Glue job deployment and execution')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--no-cleanup', action='store_true', 
                       help='Skip cleanup of test resources')
    parser.add_argument('--keep-job', action='store_true',
                       help='Keep the test Glue job for debugging')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Run the test
        tester = GlueJobTester(args.region)
        results = tester.run_full_test(
            cleanup=not args.no_cleanup,
            keep_job=args.keep_job
        )
        
        # Exit with appropriate code
        if results['status'] == 'success':
            sys.exit(0)
        elif results['status'] == 'partial_success':
            sys.exit(1)
        else:
            sys.exit(2)
            
    except Exception as e:
        logger.error(f"Test execution failed: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()