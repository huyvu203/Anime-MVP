#!/usr/bin/env python3
"""
Athena Query Client for Data Retrieval Agent

This module provides SQL query capabilities for the Data Retrieval Agent
using AWS Athena to query processed anime data in S3.
"""

import boto3
import time
import logging
import os
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class AthenaQueryClient:
    """
    Athena client for executing SQL queries against anime data.
    """
    
    def __init__(self, region: str = 'us-east-2'):
        """Initialize Athena client."""
        self.region = region
        self.database = 'anime_data'
        self.results_location = 's3://anime-mvp-data/athena-results/'
        
        # Initialize AWS clients
        self.athena_client = boto3.client('athena', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        
        logger.info(f"AthenaQueryClient initialized for region: {region}")
        logger.info(f"Database: {self.database}")
    
    def execute_query(self, sql: str, timeout: int = 60) -> Dict[str, Any]:
        """
        Execute SQL query and return results.
        
        Args:
            sql: SQL query to execute
            timeout: Maximum time to wait for query completion (seconds)
            
        Returns:
            Dictionary with query results and metadata
        """
        try:
            logger.info(f"Executing query: {sql[:100]}...")
            
            # Start query execution
            response = self.athena_client.start_query_execution(
                QueryString=sql,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': self.results_location}
            )
            
            query_execution_id = response['QueryExecutionId']
            logger.info(f"Query started with ID: {query_execution_id}")
            
            # Wait for completion
            start_time = time.time()
            while time.time() - start_time < timeout:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED']:
                    # Get results
                    results = self.athena_client.get_query_results(
                        QueryExecutionId=query_execution_id
                    )
                    
                    return self._format_results(results, query_execution_id, sql)
                    
                elif status in ['FAILED', 'CANCELLED']:
                    error_msg = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                    logger.error(f"Query failed: {error_msg}")
                    return {
                        'status': 'error',
                        'error': error_msg,
                        'query_id': query_execution_id,
                        'sql': sql
                    }
                    
                # Still running, wait and check again
                time.sleep(2)
            
            # Timeout
            logger.error(f"Query timeout after {timeout} seconds")
            return {
                'status': 'timeout',
                'error': f'Query timeout after {timeout} seconds',
                'query_id': query_execution_id,
                'sql': sql
            }
            
        except Exception as e:
            logger.error(f"Query execution error: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'sql': sql
            }
    
    def _format_results(self, results: Dict, query_id: str, sql: str) -> Dict[str, Any]:
        """Format Athena query results into structured data."""
        try:
            result_set = results['ResultSet']
            
            # Extract column names
            columns = []
            if 'ResultSetMetadata' in result_set and 'ColumnInfo' in result_set['ResultSetMetadata']:
                columns = [col['Name'] for col in result_set['ResultSetMetadata']['ColumnInfo']]
            
            # Extract rows
            rows = []
            if 'Rows' in result_set:
                data_rows = result_set['Rows']
                
                # Skip header row if it exists
                start_idx = 1 if len(data_rows) > 0 and data_rows[0].get('Data') else 0
                
                for row in data_rows[start_idx:]:
                    if 'Data' in row:
                        row_data = []
                        for cell in row['Data']:
                            value = cell.get('VarCharValue', '')
                            row_data.append(value)
                        rows.append(row_data)
            
            logger.info(f"Query completed: {len(rows)} rows returned")
            
            return {
                'status': 'success',
                'columns': columns,
                'rows': rows,
                'row_count': len(rows),
                'query_id': query_id,
                'sql': sql
            }
            
        except Exception as e:
            logger.error(f"Error formatting results: {e}")
            return {
                'status': 'error',
                'error': f'Error formatting results: {e}',
                'query_id': query_id,
                'sql': sql
            }
    
    # Query methods for common anime data operations
    
    def get_top_rated_anime(self, limit: int = 10, min_score: float = 7.0) -> Dict[str, Any]:
        """Get top rated anime."""
        sql = f"""
        SELECT title, score, year, type, episodes, status
        FROM anime_data.anime 
        WHERE score IS NOT NULL AND score >= {min_score}
        ORDER BY score DESC 
        LIMIT {limit}
        """
        return self.execute_query(sql)
    
    def search_anime_by_title(self, title_query: str, limit: int = 20) -> Dict[str, Any]:
        """Search anime by title (case insensitive)."""
        sql = f"""
        SELECT title, score, year, type, episodes, status
        FROM anime_data.anime 
        WHERE LOWER(title) LIKE LOWER('%{title_query}%')
        ORDER BY score DESC NULLS LAST
        LIMIT {limit}
        """
        return self.execute_query(sql)
    
    def get_anime_by_genre(self, genre: str, limit: int = 20) -> Dict[str, Any]:
        """Get anime by genre."""
        sql = f"""
        SELECT a.title, a.score, a.year, a.type, a.episodes
        FROM anime_data.anime a
        JOIN anime_data.anime_genres g ON a.anime_id = g.anime_id
        WHERE LOWER(g.genre_name) LIKE LOWER('%{genre}%')
        ORDER BY a.score DESC NULLS LAST
        LIMIT {limit}
        """
        return self.execute_query(sql)
    
    def get_anime_by_year(self, year: int, limit: int = 20) -> Dict[str, Any]:
        """Get anime from specific year."""
        sql = f"""
        SELECT title, score, type, episodes, status
        FROM anime_data.anime 
        WHERE year = '{year}'
        ORDER BY score DESC NULLS LAST
        LIMIT {limit}
        """
        return self.execute_query(sql)
    
    def get_currently_airing(self, limit: int = 20) -> Dict[str, Any]:
        """Get currently airing anime."""
        sql = f"""
        SELECT title, score, year, type, episodes, status
        FROM anime_data.anime 
        WHERE LOWER(status) = 'currently airing'
        ORDER BY score DESC NULLS LAST
        LIMIT {limit}
        """
        return self.execute_query(sql)
    
    def get_anime_stats(self) -> Dict[str, Any]:
        """Get overall anime statistics."""
        sql = """
        SELECT 
            COUNT(*) as total_anime,
            AVG(score) as avg_score,
            COUNT(CASE WHEN score IS NOT NULL THEN 1 END) as scored_anime,
            MIN(year) as earliest_year,
            MAX(year) as latest_year
        FROM anime_data.anime
        WHERE year IS NOT NULL AND year != ''
        """
        return self.execute_query(sql)
    
    def get_genre_distribution(self, limit: int = 15) -> Dict[str, Any]:
        """Get genre distribution."""
        sql = f"""
        SELECT genre_name, COUNT(*) as anime_count
        FROM anime_data.anime_genres 
        GROUP BY genre_name
        ORDER BY anime_count DESC
        LIMIT {limit}
        """
        return self.execute_query(sql)


def test_athena_client():
    """Test the Athena client functionality."""
    logger.info("🧪 Testing AthenaQueryClient...")
    
    client = AthenaQueryClient()
    
    # Test basic query
    print("\\n1. Testing top rated anime...")
    result = client.get_top_rated_anime(limit=5)
    if result['status'] == 'success':
        print(f"✅ Found {result['row_count']} top anime")
        for i, row in enumerate(result['rows'][:3]):
            print(f"  {i+1}. {row[0]} - Score: {row[1]}")
    
    # Test search
    print("\\n2. Testing search...")
    result = client.search_anime_by_title('attack')
    if result['status'] == 'success':
        print(f"✅ Found {result['row_count']} anime matching 'attack'")
    
    # Test genre query
    print("\\n3. Testing genre query...")
    result = client.get_anime_by_genre('action', limit=5)
    if result['status'] == 'success':
        print(f"✅ Found {result['row_count']} action anime")
    
    print("\\n✅ AthenaQueryClient test completed!")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_athena_client()