"""
Data Retrieval Agent for Anime Assistant Sequential Workflow.

This agent acts as the backend executor that:
1. Receives structured data requests from the User Interface Agent
2. Executes SQL queries against processed anime data using AWS Athena
3. Queries user watch history from SQLite database
4. Returns structured results back to the User Interface Agent

Supported query types:
- search_title: Search for anime by title
- genre_filter: Filter anime by genre
- currently_airing: Get currently airing anime
- top_rated: Get top-rated anime (with optional year filter)
- watch_history: Get user's personal watch history
- recommendations: Get personalized recommendations
"""

import os
import json
import logging
import sqlite3
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

# Import pandas with error handling for NumPy compatibility issues
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except Exception as e:
    logger.warning(f"Pandas import failed, will use fallback methods: {e}")
    PANDAS_AVAILABLE = False
    pd = None

# Import AthenaQueryClient for SQL-based queries
try:
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from data.athena_client import AthenaQueryClient
    ATHENA_AVAILABLE = True
except Exception as e:
    logger.warning(f"AthenaQueryClient import failed: {e}")
    AthenaQueryClient = None
    ATHENA_AVAILABLE = False

# Import boto3 for direct S3 access if pandas fails
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except Exception as e:
    logger.warning(f"Boto3 import failed: {e}")
    BOTO3_AVAILABLE = False

logger = logging.getLogger(__name__)


class DataRetrievalAgent:
    """
    Backend agent that executes queries against anime datasets.
    
    Responsibilities:
    - Process structured data requests from UI Agent
    - Execute SQL queries against anime data using AWS Athena
    - Query personal watch history SQLite database
    - Return structured, consistent results
    - Handle errors gracefully
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Data Retrieval Agent with configuration."""
        
        self.config = config or self._load_default_config()
        self.name = "DataRetrievalAgent"
        
        # Initialize Athena query client
        if ATHENA_AVAILABLE and AthenaQueryClient:
            try:
                self.athena_client = AthenaQueryClient(
                    region=self.config.get('aws_region')
                )
                logger.info("✅ AthenaQueryClient initialized")
            except Exception as e:
                logger.warning(f"⚠️ AthenaQueryClient initialization failed: {e}")
                self.athena_client = None
        else:
            logger.warning("⚠️ AthenaQueryClient not available")
            self.athena_client = None
        
        # Initialize SQLite database path
        self.db_path = Path(self.config.get('watch_data_path', 'data/user_history.db'))
        if not self.db_path.exists():
            logger.warning(f"⚠️ Watch history database not found: {self.db_path}")
        
        logger.info(f"Data Retrieval Agent initialized")

    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration from environment."""
        return {
            's3_bucket': os.getenv('S3_BUCKET', 'anime-mvp-data'),
            'aws_region': os.getenv('AWS_REGION', 'us-east-2'),
            'watch_data_path': 'data/user_history.db'
        }

    def process_data_request(self, data_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a structured data request from the UI Agent.
        
        Args:
            data_request: Dictionary with 'query_type', 'parameters', 'user_query'
            
        Returns:
            Structured response dictionary with results
        """
        try:
            query_type = data_request.get('query_type')
            parameters = data_request.get('parameters', {})
            original_query = data_request.get('user_query', '')
            
            logger.info(f"Processing data request: {query_type} with params: {parameters}")
            
            # Route to appropriate handler based on query type
            if query_type == "search_title":
                return self._search_anime_by_title(parameters)
            elif query_type == "genre_filter":
                return self._get_anime_by_genre(parameters)
            elif query_type == "currently_airing":
                return self._get_currently_airing(parameters)
            elif query_type == "top_rated":
                return self._get_top_rated_anime(parameters)
            elif query_type == "watch_history":
                return self._get_user_watch_history(parameters)
            elif query_type == "recommendations":
                return self._get_user_recommendations(parameters)
            else:
                return {
                    "status": "error",
                    "message": f"Unknown query type: {query_type}",
                    "query_type": query_type
                }
                
        except Exception as e:
            logger.error(f"Error processing data request: {e}")
            return {
                "status": "error",
                "message": f"Data retrieval failed: {str(e)}",
                "query_type": data_request.get('query_type', 'unknown')
            }

    def _search_anime_by_title(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Search for anime by title using Athena SQL queries."""
        try:
            title = parameters.get('title', '')
            limit = parameters.get('limit', 10)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "search_title")
            
            # Execute Athena query
            result = self.athena_client.search_anime_by_title(title, limit)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 6:  # title, score, year, type, episodes, status
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None,
                            'status': row[5]
                        })
                
                return {
                    "status": "success",
                    "query_type": "search_title",
                    "results": results,
                    "count": len(results),
                    "search_term": title,
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "status": "error",
                    "query_type": "search_title",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}",
                    "search_term": title
                }
            
        except Exception as e:
            logger.error(f"Title search error: {e}")
            return self._create_error_response(str(e), "search_title")

    def _get_anime_by_genre(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get anime filtered by genre using Athena SQL queries."""
        try:
            genre = parameters.get('genre', '')
            limit = parameters.get('limit', 20)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "genre_filter")
            
            # Execute Athena query
            result = self.athena_client.get_anime_by_genre(genre, limit)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 5:  # title, score, year, type, episodes
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None
                        })
                
                return {
                    "status": "success",
                    "query_type": "genre_filter",
                    "results": results,
                    "count": len(results),
                    "genre": genre,
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "status": "error",
                    "query_type": "genre_filter", 
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}",
                    "genre": genre
                }
            
        except Exception as e:
            logger.error(f"Genre filter error: {e}")
            return self._create_error_response(str(e), "genre_filter")

    def _get_currently_airing(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get currently airing anime using Athena SQL queries."""
        try:
            limit = parameters.get('limit', 20)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "currently_airing")
            
            # Execute Athena query
            result = self.athena_client.get_currently_airing(limit)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 6:  # title, score, year, type, episodes, status
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None,
                            'status': row[5]
                        })
                
                return {
                    "status": "success",
                    "query_type": "currently_airing",
                    "results": results,
                    "count": len(results),
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "status": "error",
                    "query_type": "currently_airing",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}"
                }
            
        except Exception as e:
            logger.error(f"Currently airing error: {e}")
            return self._create_error_response(str(e), "currently_airing")

    def _get_top_rated_anime(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get top-rated anime, optionally filtered by year using Athena."""
        try:
            limit = parameters.get('limit', 20)
            year = parameters.get('year')
            min_score = parameters.get('min_score', 7.0)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "top_rated")
            
            # Use specific year query if year is provided
            if year:
                result = self.athena_client.get_anime_by_year(int(year), limit)
            else:
                result = self.athena_client.get_top_rated_anime(limit, min_score)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 5:  # Varies by query, but at least title, score
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if len(row) > 2 and row[2] and row[2] != '' else None,
                            'type': row[3] if len(row) > 3 else None,
                            'episodes': int(row[4]) if len(row) > 4 and row[4] and row[4] != '' else None,
                            'status': row[5] if len(row) > 5 else None
                        })
                
                response = {
                    "status": "success",
                    "query_type": "top_rated", 
                    "results": results,
                    "count": len(results),
                    "query_id": result.get('query_id')
                }
                
                if year:
                    response["year"] = year
                
                return response
            else:
                return {
                    "status": "error",
                    "query_type": "top_rated",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}"
                }
            
        except Exception as e:
            logger.error(f"Top rated error: {e}")
            return self._create_error_response(str(e), "top_rated")

    def _get_user_watch_history(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get user's personal watch history from SQLite database."""
        try:
            user_id = parameters.get('user_id', 'personal_user')
            status = parameters.get('status')  # Optional status filter
            limit = parameters.get('limit', 50)
            
            if not self.db_path.exists():
                return self._create_error_response("Watch history database not found", "watch_history")
            
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            
            # Build query
            query = "SELECT * FROM user_watch_history WHERE user_id = ?"
            params = [user_id]
            
            if status:
                query += " AND watch_status = ?"
                params.append(status)
            
            query += " ORDER BY created_at DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = [dict(row) for row in rows]
            conn.close()
            
            response = {
                "status": "success",
                "query_type": "watch_history",
                "results": results,
                "count": len(results),
                "user_id": user_id
            }
            
            if status:
                response["status_filter"] = status
            
            return response
            
        except Exception as e:
            logger.error(f"Watch history error: {e}")
            return self._create_error_response(str(e), "watch_history")

    def _get_user_recommendations(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate personalized recommendations based on watch history."""
        try:
            user_id = parameters.get('user_id', 'personal_user')
            count = parameters.get('limit', 10)
            
            # Get user's watch history to understand preferences
            history_response = self._get_user_watch_history({
                'user_id': user_id,
                'limit': 100  # Get more history for better recommendations
            })
            
            if history_response['status'] != 'success' or not history_response['results']:
                return {
                    "status": "success",
                    "query_type": "recommendations",
                    "results": [],
                    "count": 0,
                    "message": "No watch history available for recommendations"
                }
            
            # Analyze user preferences from history
            history = history_response['results']
            
            # Get genres from completed/high-rated anime
            preferred_genres = set()
            for entry in history:
                if entry.get('watch_status') == 'completed' and entry.get('rating', 0) >= 7:
                    if entry.get('genre'):
                        preferred_genres.add(entry.get('genre'))
            
            if not preferred_genres:
                # Fallback to any genres from watch history
                for entry in history[:10]:  # Look at recent entries
                    if entry.get('genre'):
                        preferred_genres.add(entry.get('genre'))
            
            # Get recommendations from S3 data based on preferences
            if not self.s3_reader or not preferred_genres:
                return {
                    "status": "success",
                    "query_type": "recommendations",
                    "results": [],
                    "count": 0,
                    "message": "Unable to generate recommendations"
                }
            
            df = self.s3_reader.read_anime_data()
            if df is None or df.empty:
                return self._create_error_response("No anime data available", "recommendations")
            
            # Filter out anime user has already seen
            watched_ids = {entry.get('anime_id') for entry in history}
            
            if 'mal_id' in df.columns:
                df = df[~df['mal_id'].isin(watched_ids)]
            elif 'id' in df.columns:
                df = df[~df['id'].isin(watched_ids)]
            
            # Score anime based on genre matches and ratings
            df['rec_score'] = 0
            
            for genre in preferred_genres:
                if 'genres' in df.columns:
                    genre_match = df['genres'].str.contains(genre, case=False, na=False)
                elif 'genre' in df.columns:
                    genre_match = df['genre'].str.contains(genre, case=False, na=False)
                else:
                    continue
                    
                df.loc[genre_match, 'rec_score'] += 1
            
            # Boost score for highly rated anime
            if 'score' in df.columns:
                df['rec_score'] += df['score'] / 10  # Normalize score contribution
            
            # Sort by recommendation score
            recommended_df = df[df['rec_score'] > 0].sort_values('rec_score', ascending=False)
            
            if PANDAS_AVAILABLE:
                results = recommended_df.head(count).to_dict('records')
            else:
                results = []
            
            return {
                "status": "success",
                "query_type": "recommendations",
                "results": results,
                "count": len(results),
                "user_id": user_id,
                "based_on_genres": list(preferred_genres)
            }
            
        except Exception as e:
            logger.error(f"Recommendations error: {e}")
            return self._create_error_response(str(e), "recommendations")

    def _search_s3_direct(self, title: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Direct S3 search without pandas (fallback method)."""
        try:
            if not self.s3_client:
                return []
            
            bucket = self.config.get('s3_bucket')
            prefix = 'processed/'  # Assuming processed data is in this prefix
            
            # List objects in the processed folder
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=100  # Limit for performance
            )
            
            results = []
            title_lower = title.lower()
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    # Skip non-data files
                    if not (key.endswith('.json') or key.endswith('.parquet')):
                        continue
                    
                    try:
                        # For demo, create mock results based on filename
                        # In production, you'd download and parse the files
                        if title_lower in key.lower():
                            mock_result = {
                                'title': key.replace(prefix, '').replace('.json', '').replace('_', ' ').title(),
                                'source': 's3_fallback',
                                'key': key,
                                'score': 7.5,  # Mock score
                                'type': 'TV',
                                'episodes': 12,
                                'status': 'Finished Airing'
                            }
                            results.append(mock_result)
                            
                            if len(results) >= limit:
                                break
                    except Exception as e:
                        logger.warning(f"Error processing S3 object {key}: {e}")
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Direct S3 search error: {e}")
            return []

    def _get_s3_fallback_data(self, data_type: str = 'anime', limit: int = 50) -> List[Dict[str, Any]]:
        """Get mock data when S3 access is limited."""
        try:
            # Return mock data structure for testing
            mock_data = []
            
            if data_type == 'top_rated':
                # Mock top rated anime
                top_anime = [
                    {'title': 'Fullmetal Alchemist: Brotherhood', 'score': 9.1, 'rank': 1, 'type': 'TV', 'episodes': 64},
                    {'title': 'Steins;Gate', 'score': 9.0, 'rank': 2, 'type': 'TV', 'episodes': 24},
                    {'title': 'Attack on Titan', 'score': 8.9, 'rank': 3, 'type': 'TV', 'episodes': 25},
                    {'title': 'Spirited Away', 'score': 8.8, 'rank': 4, 'type': 'Movie', 'episodes': 1},
                    {'title': 'Your Name', 'score': 8.4, 'rank': 5, 'type': 'Movie', 'episodes': 1}
                ]
                mock_data = top_anime[:limit]
                
            elif data_type == 'airing':
                # Mock currently airing
                airing_anime = [
                    {'title': 'One Piece', 'score': 8.9, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 1000},
                    {'title': 'Boruto', 'score': 7.5, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 200},
                    {'title': 'Demon Slayer Season 4', 'score': 8.7, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 12}
                ]
                mock_data = airing_anime[:limit]
                
            return mock_data
            
        except Exception as e:
            logger.error(f"Fallback data error: {e}")
            return []

    def _create_error_response(self, message: str, query_type: str) -> Dict[str, Any]:
        """Create a standardized error response."""
        return {
            "status": "error",
            "message": message,
            "query_type": query_type,
            "results": [],
            "count": 0
        }

    def get_capabilities(self) -> Dict[str, Any]:
        """Return information about the agent's capabilities."""
        return {
            "name": self.name,
            "type": "data_retrieval",
            "data_sources": {
                "s3_available": self.s3_reader is not None,
                "watch_db_available": self.db_path.exists()
            },
            "supported_queries": [
                "search_title",
                "genre_filter",
                "currently_airing", 
                "top_rated",
                "watch_history",
                "recommendations"
            ],
            "config": {
                "s3_bucket": self.config.get('s3_bucket'),
                "watch_db_path": str(self.db_path)
            }
        }


# Convenience function for testing
def create_data_retrieval_agent(config: Optional[Dict[str, Any]] = None) -> DataRetrievalAgent:
    """Create a Data Retrieval Agent with optional configuration."""
    return DataRetrievalAgent(config)


if __name__ == "__main__":
    """Simple test of the Data Retrieval Agent."""
    
    logging.basicConfig(level=logging.INFO)
    
    # Test data requests
    test_requests = [
        {
            "query_type": "search_title",
            "parameters": {"title": "Attack on Titan", "limit": 3},
            "user_query": "Tell me about Attack on Titan"
        },
        {
            "query_type": "watch_history", 
            "parameters": {"user_id": "personal_user", "limit": 5},
            "user_query": "What am I currently watching?"
        },
        {
            "query_type": "top_rated",
            "parameters": {"limit": 5, "year": "2023"},
            "user_query": "Best anime from 2023"
        }
    ]
    
    try:
        # Create agent
        agent = create_data_retrieval_agent()
        print(f"✅ Data Retrieval Agent created successfully")
        
        capabilities = agent.get_capabilities()
        print(f"📋 S3 Available: {capabilities['data_sources']['s3_available']}")
        print(f"📋 Watch DB Available: {capabilities['data_sources']['watch_db_available']}")
        
        # Test requests
        print(f"\n🧪 Testing {len(test_requests)} data requests...")
        
        for i, request in enumerate(test_requests, 1):
            print(f"\n{'='*50}")
            print(f"Test {i}: {request['query_type']}")
            print(f"Query: {request['user_query']}")
            print('='*50)
            
            result = agent.process_data_request(request)
            
            print(f"Status: {result['status']}")
            print(f"Count: {result['count']}")
            
            if result['status'] == 'success' and result['results']:
                print("Sample results:")
                for j, item in enumerate(result['results'][:2], 1):
                    title = item.get('title') or item.get('anime_title', 'Unknown')
                    print(f"  {j}. {title}")
            elif result['status'] == 'error':
                print(f"Error: {result['message']}")
                
    except Exception as e:
        print(f"❌ Test failed: {e}")