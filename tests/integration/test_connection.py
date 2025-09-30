#!/usr/bin/env python3
"""
Simple test script to verify Jikan API + S3 integration.

This script fetches a small amount of anime data from Jikan API
and uploads it to S3 to test the basic pipeline.
"""

import os
import sys
import json
import logging
from datetime import datetime
from pathlib import Path

# Add src to path so we can import our modules
sys.path.append(str(Path(__file__).parent / "src"))

from ingestion.fetch_jikan import JikanAPIClient, S3Uploader
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def test_jikan_api():
    """Test basic Jikan API connectivity."""
    logger.info("🔍 Testing Jikan API connection...")
    
    try:
        client = JikanAPIClient()
        
        # Test getting a single popular anime (Cowboy Bebop - ID 1)
        anime_data = client.get_anime(1)
        
        if anime_data and 'data' in anime_data:
            title = anime_data['data'].get('title', 'Unknown')
            score = anime_data['data'].get('score', 'N/A')
            logger.info(f"✅ Successfully fetched: {title} (Score: {score})")
            return anime_data
        else:
            logger.error("❌ Failed to fetch anime data")
            return None
            
    except Exception as e:
        logger.error(f"❌ Jikan API test failed: {e}")
        return None


def test_s3_connection():
    """Test S3 bucket connectivity."""
    logger.info("☁️ Testing S3 connection...")
    
    try:
        uploader = S3Uploader()
        
        # Test with a simple JSON object
        test_data = {
            "test": "data",
            "timestamp": datetime.now().isoformat(),
            "bucket": uploader.bucket_name,
            "region": uploader.region
        }
        
        test_key = f"test/connection_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        success = uploader.upload_json(test_data, test_key)
        
        if success:
            logger.info(f"✅ Successfully uploaded test file to: s3://{uploader.bucket_name}/{test_key}")
            return True
        else:
            logger.error("❌ Failed to upload test file")
            return False
            
    except Exception as e:
        logger.error(f"❌ S3 connection test failed: {e}")
        return False


def test_full_pipeline():
    """Test the full pipeline: Jikan API → S3."""
    logger.info("🚀 Testing full pipeline: Jikan API → S3...")
    
    try:
        # Initialize clients
        api_client = JikanAPIClient()
        s3_uploader = S3Uploader()
        
        # Fetch a few anime (small dataset)
        test_anime_ids = [1, 5, 20]  # Cowboy Bebop, FMA: Brotherhood, Naruto
        date_str = datetime.now().strftime("%Y-%m-%d")
        
        successful_uploads = 0
        total_attempts = len(test_anime_ids)
        
        for anime_id in test_anime_ids:
            logger.info(f"📥 Fetching anime ID: {anime_id}")
            
            # Fetch anime data
            anime_data = api_client.get_anime(anime_id)
            
            if anime_data and 'data' in anime_data:
                title = anime_data['data'].get('title', f'anime_{anime_id}')
                logger.info(f"✅ Fetched: {title}")
                
                # Upload to S3
                s3_key = f"raw/{date_str}/test_anime_{anime_id}.json"
                if s3_uploader.upload_json(anime_data, s3_key):
                    logger.info(f"✅ Uploaded to: s3://{s3_uploader.bucket_name}/{s3_key}")
                    successful_uploads += 1
                else:
                    logger.error(f"❌ Failed to upload anime {anime_id}")
            else:
                logger.error(f"❌ Failed to fetch anime {anime_id}")
            
            # Small delay to respect rate limits
            import time
            time.sleep(1)
        
        # Summary
        logger.info("=" * 50)
        logger.info("📊 TEST SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total attempts: {total_attempts}")
        logger.info(f"Successful uploads: {successful_uploads}")
        logger.info(f"Success rate: {successful_uploads/total_attempts*100:.1f}%")
        
        if successful_uploads == total_attempts:
            logger.info("🎉 All tests passed! Pipeline is working correctly.")
            return True
        else:
            logger.warning("⚠️ Some tests failed. Check the logs above.")
            return False
            
    except Exception as e:
        logger.error(f"❌ Full pipeline test failed: {e}")
        return False


def verify_uploads():
    """Verify that files were uploaded to S3."""
    logger.info("🔍 Verifying uploaded files...")
    
    try:
        import boto3
        
        s3_client = boto3.client('s3', region_name=os.getenv('AWS_REGION'))
        bucket_name = os.getenv('S3_BUCKET')
        
        # List objects in the bucket
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        
        if 'Contents' in response:
            logger.info(f"📁 Found {len(response['Contents'])} files in bucket:")
            for obj in response['Contents']:
                size_kb = obj['Size'] / 1024
                logger.info(f"  📄 {obj['Key']} ({size_kb:.1f} KB)")
        else:
            logger.info("📁 No files found in bucket")
            
    except Exception as e:
        logger.error(f"❌ Failed to verify uploads: {e}")


def main():
    """Main test function."""
    logger.info("🧪 Starting Jikan API + S3 Integration Test")
    logger.info("=" * 60)
    
    # Check environment variables
    required_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'S3_BUCKET', 'AWS_REGION']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing required environment variables: {missing_vars}")
        logger.error("Please check your .env file")
        return 1
    
    logger.info(f"🪣 S3 Bucket: {os.getenv('S3_BUCKET')}")
    logger.info(f"🌍 AWS Region: {os.getenv('AWS_REGION')}")
    logger.info("")
    
    # Run tests
    all_passed = True
    
    # Test 1: Jikan API
    if not test_jikan_api():
        all_passed = False
    logger.info("")
    
    # Test 2: S3 Connection
    if not test_s3_connection():
        all_passed = False
    logger.info("")
    
    # Test 3: Full Pipeline
    if not test_full_pipeline():
        all_passed = False
    logger.info("")
    
    # Verify uploads
    verify_uploads()
    logger.info("")
    
    # Final result
    if all_passed:
        logger.info("🎉 ALL TESTS PASSED! Your setup is working correctly.")
        logger.info("You can now run the full anime data fetcher with:")
        logger.info("  poetry run python src/ingestion/fetch_jikan.py --mvp")
        return 0
    else:
        logger.error("❌ Some tests failed. Please check the error messages above.")
        return 1


if __name__ == "__main__":
    exit(main())