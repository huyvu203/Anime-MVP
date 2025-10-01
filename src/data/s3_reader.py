"""
S3 Data Reader for Custom Agents

This module provides utilities for agents to read processed anime data directly from S3.
"""

import logging
import os
from typing import Dict, List, Optional, Union
from pathlib import Path

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv
import s3fs

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class S3DataReader:
    """Read processed anime data from S3 for use by custom agents."""
    
    def __init__(self, bucket_name: str = None, region: str = None):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET", "anime-data")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        self.processed_prefix = os.getenv("S3_PROCESSED_PREFIX", "processed")
        
        try:
            self.s3_client = boto3.client("s3", region_name=self.region)
            self.fs = s3fs.S3FileSystem(anon=False)
            
            # Test connection
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"Connected to S3 bucket: {self.bucket_name}")
        except NoCredentialsError:
            logger.error("AWS credentials not found")
            raise
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404":
                logger.error(f"S3 bucket {self.bucket_name} not found")
            else:
                logger.error(f"S3 connection error: {e}")
            raise
    
    def list_processed_files(self, date: str = None) -> List[str]:
        """List all processed files in S3."""
        if date:
            prefix = f"{self.processed_prefix}/{date}/"
        else:
            prefix = f"{self.processed_prefix}/"
        
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    files.append(obj['Key'])
            
            logger.info(f"Found {len(files)} processed files")
            return files
            
        except ClientError as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    def read_anime_data(self, date: str = None) -> Optional[pd.DataFrame]:
        """Read anime metadata from processed CSV."""
        try:
            # Read from the simple CSV file
            s3_path = f"s3://{self.bucket_name}/{self.processed_prefix}/anime.csv"
            df = pd.read_csv(s3_path)
            logger.info(f"Loaded {len(df)} anime records from CSV")
            return df
            
        except Exception as e:
            logger.error(f"Failed to read anime data: {s3_path}: {e}")
            return None
    
    def read_statistics_data(self, date: str = None) -> Optional[pd.DataFrame]:
        """Read anime statistics from processed CSV."""
        try:
            # Try to read from statistics CSV file
            s3_path = f"s3://{self.bucket_name}/{self.processed_prefix}/statistics.csv"
            df = pd.read_csv(s3_path)
            logger.info(f"Loaded {len(df)} statistics records from CSV")
            return df
            
        except Exception as e:
            logger.warning(f"Statistics data not available: {e}")
            # Return anime data as fallback since it may contain statistical fields
            return self.read_anime_data()
    
    def read_recommendations_data(self, date: str = None) -> Optional[pd.DataFrame]:
        """Read anime recommendations from processed CSV."""
        try:
            # Try to read from recommendations CSV file
            s3_path = f"s3://{self.bucket_name}/{self.processed_prefix}/recommendations.csv"
            df = pd.read_csv(s3_path)
            logger.info(f"Loaded {len(df)} recommendation records from CSV")
            return df
            
        except Exception as e:
            logger.warning(f"Recommendations data not available: {e}")
            return None
    
    def read_genres_data(self, date: str = None) -> Optional[pd.DataFrame]:
        """Read genres data from processed CSV."""
        try:
            # Try to read from genres CSV file
            s3_path = f"s3://{self.bucket_name}/{self.processed_prefix}/genres.csv"
            df = pd.read_csv(s3_path)
            logger.info(f"Loaded {len(df)} genre records from CSV")
            return df
            
        except Exception as e:
            logger.warning(f"Genres data not available: {e}")
            return None    def read_all_data(self, date: str = None) -> Dict[str, pd.DataFrame]:
        """Read all processed data types into a dictionary."""
        logger.info("Loading all processed data from S3...")
        
        data = {}
        
        # Load each data type
        data['anime'] = self.read_anime_data(date)
        data['statistics'] = self.read_statistics_data(date)
        data['recommendations'] = self.read_recommendations_data(date)
        data['genres'] = self.read_genres_data(date)
        
        # Filter out None values
        data = {k: v for k, v in data.items() if v is not None}
        
        logger.info(f"Loaded data types: {list(data.keys())}")
        return data
    
    def get_anime_by_id(self, anime_id: int, date: str = None) -> Optional[Dict]:
        """Get specific anime data by ID."""
        anime_df = self.read_anime_data(date)
        if anime_df is None:
            return None
        
        anime_row = anime_df[anime_df['anime_id'] == anime_id]
        if anime_row.empty:
            return None
        
        return anime_row.iloc[0].to_dict()
    
    def search_anime(self, query: str, date: str = None, limit: int = 10) -> Optional[pd.DataFrame]:
        """Search anime by title."""
        anime_df = self.read_anime_data(date)
        if anime_df is None:
            return None
        
        # Search in title fields
        mask = (
            anime_df['title'].str.contains(query, case=False, na=False) |
            anime_df['title_english'].str.contains(query, case=False, na=False) |
            anime_df['title_japanese'].str.contains(query, case=False, na=False)
        )
        
        results = anime_df[mask].head(limit)
        logger.info(f"Found {len(results)} anime matching '{query}'")
        return results
    
    def get_top_anime(self, limit: int = 50, date: str = None) -> Optional[pd.DataFrame]:
        """Get top anime by score."""
        anime_df = self.read_anime_data(date)
        if anime_df is None:
            return None
        
        top_anime = anime_df.nlargest(limit, 'score')
        logger.info(f"Retrieved top {len(top_anime)} anime")
        return top_anime
    
    def get_popular_anime(self, limit: int = 50, date: str = None) -> Optional[pd.DataFrame]:
        """Get most popular anime by popularity rank."""
        anime_df = self.read_anime_data(date)
        if anime_df is None:
            return None
        
        popular_anime = anime_df.nsmallest(limit, 'popularity')
        logger.info(f"Retrieved {len(popular_anime)} most popular anime")
        return popular_anime


def test_s3_connection():
    """Test S3 connection and data reading."""
    try:
        reader = S3DataReader()
        
        # Test listing files
        files = reader.list_processed_files()
        print(f"Found {len(files)} processed files")
        
        # Test reading data (will fail if no data exists yet)
        data = reader.read_all_data()
        for data_type, df in data.items():
            print(f"{data_type}: {len(df)} records")
            
        return True
        
    except Exception as e:
        logger.error(f"S3 connection test failed: {e}")
        return False


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_s3_connection()