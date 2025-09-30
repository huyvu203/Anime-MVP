#!/usr/bin/env python3
"""
Quick Glue Job Deployment Test

This script tests just the deployment part without running the actual job:
1. Check S3 data availability
2. Deploy the Glue job
3. Validate job configuration
4. Clean up (optional)

Usage:
    poetry run python test_quick_deployment.py
    poetry run python test_quick_deployment.py --region us-west-2
    poetry run python test_quick_deployment.py --no-cleanup --verbose

This is useful for testing deployment without incurring Glue execution costs.
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List

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


class QuickDeploymentTester:
    """
    Quick tester for Glue job deployment (no execution).
    """
    
    def __init__(self, aws_region: str = 'us-east-2'):
        """Initialize the deployment tester."""
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
        
        self.test_job_name = 'anime-etl-deployment-test'
        
        logger.info(f"QuickDeploymentTester initialized for region: {aws_region}")
    
    def check_s3_data_exists(self) -> Dict[str, int]:
        """
        Quick check to see if we have data in S3.
        
        Returns:
            Dictionary with file counts per data type
        """
        logger.info("Checking S3 data availability...")
        
        bucket = 'anime-mvp-data'
        prefix = 'raw'
        
        data_types = [
            'anime_details',
            'anime_statistics', 
            'anime_recommendations',
            'genres',
            'top_anime',
            'seasonal_anime'
        ]
        
        data_summary = {}
        total_files = 0
        
        for data_type in data_types:
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=bucket,
                    Prefix=f"{prefix}/{data_type}/",
                    MaxKeys=50
                )
                
                file_count = 0
                if 'Contents' in response:
                    file_count = len([obj for obj in response['Contents'] 
                                    if obj['Key'].endswith('.json')])
                
                data_summary[data_type] = file_count
                total_files += file_count
                
                status = "✓" if file_count > 0 else "✗"
                logger.info(f"  {status} {data_type}: {file_count} files")
                
            except ClientError as e:
                logger.warning(f"  ✗ {data_type}: Error - {e}")
                data_summary[data_type] = 0
        
        logger.info(f"Total data files available: {total_files}")
        
        if total_files == 0:
            logger.warning("No data files found - you may need to run data ingestion first")
        
        return data_summary
    
    def test_deployment(self) -> Dict:
        """
        Test deploying the Glue job.
        
        Returns:
            Deployment results
        """
        logger.info("Testing Glue job deployment...")
        
        # Configure test job
        test_config = self.deployer.default_config.copy()
        test_config.update({
            'job_name': self.test_job_name,
            'timeout': 30,  # Shorter timeout for test
            'worker_type': 'G.1X',
            'number_of_workers': 2
        })
        
        try:
            # Deploy the job
            results = self.deployer.deploy_full_stack(test_config, force_update=True)
            
            if results['status'] == 'completed':
                logger.info("✓ Deployment test successful")
                return results
            else:
                logger.error("✗ Deployment test failed")
                return results
                
        except Exception as e:
            logger.error(f"✗ Deployment test failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    def validate_job_configuration(self) -> Dict:
        """
        Validate the deployed job configuration.
        
        Returns:
            Validation results
        """
        logger.info("Validating job configuration...")
        
        try:
            # Get job details
            response = self.glue_client.get_job(JobName=self.test_job_name)
            job = response['Job']
            
            validation_results = {
                'job_exists': True,
                'job_name': job['Name'],
                'role': job['Role'],
                'worker_type': job.get('WorkerType'),
                'number_of_workers': job.get('NumberOfWorkers'),
                'glue_version': job.get('GlueVersion'),
                'script_location': job['Command']['ScriptLocation'],
                'python_version': job['Command'].get('PythonVersion'),
                'default_arguments': job.get('DefaultArguments', {}),
                'timeout': job.get('Timeout'),
                'max_retries': job.get('MaxRetries')
            }
            
            # Check key configurations
            checks = {
                'has_script_location': bool(validation_results['script_location']),
                'has_user_role': 'user/' in validation_results['role'],
                'correct_worker_type': validation_results['worker_type'] == 'G.1X',
                'correct_workers': validation_results['number_of_workers'] == 2,
                'has_spark_ui': validation_results['default_arguments'].get('--enable-spark-ui') == 'true',
                'has_temp_dir': '--TempDir' in validation_results['default_arguments']
            }
            
            validation_results['checks'] = checks
            
            # Log results
            logger.info("Job configuration validation:")
            for check, passed in checks.items():
                status = "✓" if passed else "✗"
                logger.info(f"  {status} {check}")
            
            all_passed = all(checks.values())
            validation_results['all_checks_passed'] = all_passed
            
            if all_passed:
                logger.info("✓ All configuration checks passed")
            else:
                logger.warning("⚠ Some configuration checks failed")
            
            return validation_results
            
        except ClientError as e:
            if e.response['Error']['Code'] == 'EntityNotFoundException':
                logger.error("✗ Job not found - deployment may have failed")
                return {'job_exists': False, 'error': 'Job not found'}
            else:
                logger.error(f"✗ Error validating job: {e}")
                return {'job_exists': False, 'error': str(e)}
    
    def cleanup_test_job(self):
        """Clean up the test job."""
        logger.info("Cleaning up test job...")
        
        try:
            self.deployer.delete_job(self.test_job_name)
            logger.info("✓ Test job deleted successfully")
        except Exception as e:
            logger.warning(f"Could not delete test job: {e}")
    
    def run_quick_test(self, cleanup: bool = True) -> Dict:
        """
        Run the quick deployment test.
        
        Args:
            cleanup: Whether to clean up the test job
            
        Returns:
            Test results
        """
        logger.info("=" * 60)
        logger.info("QUICK GLUE DEPLOYMENT TEST")
        logger.info("=" * 60)
        
        test_results = {
            'start_time': datetime.now(),
            'status': 'running',
            'phases': {}
        }
        
        try:
            # Phase 1: Check S3 data
            logger.info("Phase 1: Checking S3 data availability")
            data_summary = self.check_s3_data_exists()
            test_results['phases']['data_check'] = {
                'status': 'completed',
                'data_summary': data_summary,
                'total_files': sum(data_summary.values())
            }
            
            # Phase 2: Test deployment
            logger.info("Phase 2: Testing deployment")
            deployment_results = self.test_deployment()
            test_results['phases']['deployment'] = deployment_results
            
            if deployment_results['status'] != 'completed':
                test_results['status'] = 'failed'
                test_results['failure_reason'] = 'Deployment failed'
                return test_results
            
            # Phase 3: Validate configuration
            logger.info("Phase 3: Validating job configuration")
            validation_results = self.validate_job_configuration()
            test_results['phases']['validation'] = validation_results
            
            # Determine overall status
            if validation_results.get('all_checks_passed', False):
                test_results['status'] = 'success'
            else:
                test_results['status'] = 'partial_success'
                test_results['warning'] = 'Some configuration checks failed'
            
            # Calculate duration
            test_results['end_time'] = datetime.now()
            test_results['duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
            
            # Print summary
            self._print_summary(test_results)
            
            return test_results
            
        except Exception as e:
            test_results['end_time'] = datetime.now()
            test_results['duration'] = (test_results['end_time'] - test_results['start_time']).total_seconds()
            test_results['status'] = 'error'
            test_results['error'] = str(e)
            
            logger.error("=" * 60)
            logger.error("✗ QUICK TEST FAILED")
            logger.error(f"Error: {e}")
            logger.error("=" * 60)
            
            return test_results
            
        finally:
            # Cleanup
            if cleanup:
                self.cleanup_test_job()
    
    def _print_summary(self, results: Dict):
        """Print test summary."""
        logger.info("=" * 60)
        
        if results['status'] == 'success':
            logger.info("✓ QUICK DEPLOYMENT TEST PASSED")
        elif results['status'] == 'partial_success':
            logger.info("⚠ QUICK DEPLOYMENT TEST PASSED WITH WARNINGS")
        else:
            logger.info("✗ QUICK DEPLOYMENT TEST FAILED")
        
        logger.info("=" * 60)
        
        logger.info(f"Test Duration: {results['duration']:.1f} seconds")
        
        # Phase summary
        for phase, details in results['phases'].items():
            if phase == 'data_check':
                total_files = details.get('total_files', 0)
                logger.info(f"✓ S3 Data Check: {total_files} files available")
            elif phase == 'deployment':
                status = "✓" if details['status'] == 'completed' else "✗"
                logger.info(f"{status} Deployment: {details['status']}")
            elif phase == 'validation':
                if details.get('job_exists'):
                    passed = details.get('all_checks_passed', False)
                    status = "✓" if passed else "⚠"
                    logger.info(f"{status} Configuration Validation: {'passed' if passed else 'partial'}")
                else:
                    logger.info("✗ Configuration Validation: job not found")
        
        if results['status'] == 'success':
            logger.info("\n🎉 Deployment is working correctly!")
            logger.info("   You can now run the full end-to-end test or execute the job manually.")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Quick test of Glue job deployment')
    parser.add_argument('--region', default='us-east-2', help='AWS region')
    parser.add_argument('--no-cleanup', action='store_true', 
                       help='Keep the test job for manual inspection')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        tester = QuickDeploymentTester(args.region)
        results = tester.run_quick_test(cleanup=not args.no_cleanup)
        
        # Exit codes
        if results['status'] == 'success':
            sys.exit(0)
        elif results['status'] == 'partial_success':
            sys.exit(1)
        else:
            sys.exit(2)
            
    except Exception as e:
        logger.error(f"Test failed: {e}")
        sys.exit(3)


if __name__ == "__main__":
    main()