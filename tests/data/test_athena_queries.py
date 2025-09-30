#!/usr/bin/env python3
"""
Simple Athena Query Test Script

Tests querying processed anime CSV data on S3 using AWS Athena.
This script will:
1. Create Athena database and tables
2. Test basic queries on anime data
3. Validate Athena integration for agents
"""

import boto3
import time
from datetime import datetime
from typing import Dict, List, Optional
import logging
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AthenaQueryTester:
    """Test Athena queries against anime data in S3."""
    
    def __init__(self, region: str = 'us-east-2'):
        """Initialize Athena client and configuration."""
        self.region = region
        self.athena_client = boto3.client('athena', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        
        # Configuration
        self.database_name = 'anime_data'
        self.bucket_name = 'anime-mvp-data'
        self.data_location = f's3://{self.bucket_name}/processed/'
        self.results_location = f's3://{self.bucket_name}/athena-results/'
        
        logger.info(f"AthenaQueryTester initialized for region: {region}")
        logger.info(f"Database: {self.database_name}")
        logger.info(f"Data location: {self.data_location}")
        logger.info(f"Results location: {self.results_location}")
    
    def execute_query(self, query: str, timeout: int = 60) -> Dict:
        """
        Execute an Athena query and return results.
        
        Args:
            query: SQL query string
            timeout: Maximum wait time in seconds
            
        Returns:
            Dictionary with query results
        """
        try:
            logger.info(f"Executing query: {query[:100]}...")
            
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=query,
                ResultConfiguration={
                    'OutputLocation': self.results_location
                }
            )
            
            query_id = response['QueryExecutionId']
            logger.info(f"Query started with ID: {query_id}")
            
            # Wait for completion
            start_time = time.time()
            while time.time() - start_time < timeout:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status == 'SUCCEEDED':
                    logger.info("Query completed successfully")
                    break
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = result['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Query failed: {error_msg}")
                    return {'status': 'failed', 'error': error_msg}
                
                time.sleep(2)
            else:
                logger.error("Query timed out")
                return {'status': 'timeout'}
            
            # Get results
            results = self.athena_client.get_query_results(QueryExecutionId=query_id)
            
            # Parse results
            columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]
            rows = []
            
            for row in results['ResultSet']['Rows'][1:]:  # Skip header row
                row_data = [field.get('VarCharValue', '') for field in row['Data']]
                rows.append(row_data)
            
            return {
                'status': 'success',
                'columns': columns,
                'rows': rows,
                'row_count': len(rows),
                'query_id': query_id
            }
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return {'status': 'error', 'error': str(e)}
    
    def create_database(self) -> bool:
        """Create Athena database for anime data."""
        query = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"
        result = self.execute_query(query)
        
        if result['status'] == 'success':
            logger.info(f"✅ Database '{self.database_name}' created/exists")
            return True
        else:
            logger.error(f"❌ Failed to create database: {result}")
            return False
    
    def create_anime_table(self) -> bool:
        """Create Athena table for main anime data."""
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.anime (
            anime_id bigint,
            title string,
            title_english string,
            title_japanese string,
            title_synonyms_json string,
            type string,
            source string,
            episodes bigint,
            status string,
            airing boolean,
            aired_from string,
            aired_to string,
            duration string,
            rating string,
            score double,
            scored_by bigint,
            rank bigint,
            popularity bigint,
            members bigint,
            favorites bigint,
            synopsis string,
            background string,
            season string,
            year bigint,
            broadcast_day string,
            broadcast_time string,
            approved boolean,
            processed_at timestamp
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
            'serialization.format' = ',',
            'field.delim' = ','
        )
        LOCATION '{self.data_location}anime/'
        TBLPROPERTIES (
            'has_encrypted_data'='false',
            'skip.header.line.count'='1'
        )
        """
        
        result = self.execute_query(query)
        
        if result['status'] == 'success':
            logger.info("✅ Anime table created/exists")
            return True
        else:
            logger.error(f"❌ Failed to create anime table: {result}")
            return False
    
    def create_genres_table(self) -> bool:
        """Create Athena table for anime genres."""
        query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.anime_genres (
            anime_id bigint,
            genre_id bigint,
            genre_name string,
            genre_type string,
            processed_at timestamp
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES (
            'serialization.format' = ',',
            'field.delim' = ','
        )
        LOCATION '{self.data_location}anime_genres/'
        TBLPROPERTIES (
            'has_encrypted_data'='false',
            'skip.header.line.count'='1'
        )
        """
        
        result = self.execute_query(query)
        
        if result['status'] == 'success':
            logger.info("✅ Anime genres table created/exists")
            return True
        else:
            logger.error(f"❌ Failed to create anime genres table: {result}")
            return False
    
    def test_basic_queries(self) -> Dict[str, Dict]:
        """Run a series of test queries to validate Athena setup."""
        logger.info("🧪 Running basic query tests...")
        
        test_results = {}
        
        # Test 1: Count total anime
        logger.info("Test 1: Count total anime")
        query1 = f"SELECT COUNT(*) as total_anime FROM {self.database_name}.anime"
        test_results['count_anime'] = self.execute_query(query1)
        
        # Test 2: Top 5 highest scored anime
        logger.info("Test 2: Top 5 highest scored anime")
        query2 = f"""
        SELECT title, score, year, type 
        FROM {self.database_name}.anime 
        WHERE score IS NOT NULL 
        ORDER BY score DESC 
        LIMIT 5
        """
        test_results['top_scored'] = self.execute_query(query2)
        
        # Test 3: Count anime by year
        logger.info("Test 3: Count anime by year")
        query3 = f"""
        SELECT year, COUNT(*) as count 
        FROM {self.database_name}.anime 
        WHERE year IS NOT NULL 
        GROUP BY year 
        ORDER BY year DESC 
        LIMIT 10
        """
        test_results['count_by_year'] = self.execute_query(query3)
        
        # Test 4: Genre distribution (if genres table exists)
        logger.info("Test 4: Genre distribution")
        query4 = f"""
        SELECT genre_name, COUNT(*) as anime_count 
        FROM {self.database_name}.anime_genres 
        GROUP BY genre_name 
        ORDER BY anime_count DESC 
        LIMIT 10
        """
        test_results['genre_distribution'] = self.execute_query(query4)
        
        return test_results
    
    def print_test_results(self, test_results: Dict[str, Dict]):
        """Print formatted test results."""
        logger.info("📊 Test Results Summary")
        logger.info("=" * 50)
        
        for test_name, result in test_results.items():
            logger.info(f"\n{test_name.upper()}:")
            
            if result['status'] == 'success':
                logger.info(f"✅ Success - {result['row_count']} rows returned")
                
                # Print first few rows
                if result['rows']:
                    logger.info(f"Columns: {result['columns']}")
                    for i, row in enumerate(result['rows'][:3]):
                        logger.info(f"Row {i+1}: {row}")
                    if len(result['rows']) > 3:
                        logger.info(f"... and {len(result['rows']) - 3} more rows")
            else:
                logger.error(f"❌ Failed: {result.get('error', 'Unknown error')}")
    
    def run_full_test(self) -> bool:
        """Run complete Athena test suite."""
        logger.info("🚀 Starting Athena Query Test")
        logger.info("=" * 50)
        
        try:
            # Step 1: Create database
            if not self.create_database():
                return False
            
            # Step 2: Create tables
            if not self.create_anime_table():
                return False
            
            if not self.create_genres_table():
                logger.warning("Genres table creation failed, continuing with anime table only")
            
            # Step 3: Run test queries
            test_results = self.test_basic_queries()
            
            # Step 4: Print results
            self.print_test_results(test_results)
            
            # Step 5: Overall success check
            success_count = sum(1 for result in test_results.values() if result['status'] == 'success')
            total_tests = len(test_results)
            
            logger.info(f"\n🎯 Test Summary: {success_count}/{total_tests} tests passed")
            
            if success_count >= total_tests // 2:  # At least half successful
                logger.info("✅ Athena integration test PASSED")
                return True
            else:
                logger.error("❌ Athena integration test FAILED")
                return False
                
        except Exception as e:
            logger.error(f"❌ Test suite failed: {e}")
            return False


def main():
    """Main entry point for Athena testing."""
    tester = AthenaQueryTester()
    success = tester.run_full_test()
    
    if success:
        logger.info("\n🎉 Athena is ready for agent integration!")
        logger.info("Next steps:")
        logger.info("1. Update Data Retrieval Agent to use Athena queries")
        logger.info("2. Add SQL query generation from natural language")
        logger.info("3. Test agent workflows with Athena backend")
    else:
        logger.error("\n❌ Athena setup needs attention before agent integration")
    
    return success


if __name__ == "__main__":
    main()