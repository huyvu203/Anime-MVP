#!/usr/bin/env python3
"""
Test suite for anime data pipeline components.

This module contains unit and integration tests for the Jikan API fetcher,
AWS Glue job processing, and S3 data reading components.
"""

import json
import os
import tempfile
from datetime import datetime
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
import pandas as pd
from moto import mock_s3
import boto3

# Import modules to test
import sys
sys.path.append(str(Path(__file__).parent.parent / "src"))

from ingestion.fetch_jikan import JikanAPIClient, S3Uploader, AnimeDataFetcher
from data.s3_reader import S3DataReader


# Test data fixtures
@pytest.fixture
def sample_anime_data():
    """Sample anime data from Jikan API."""
    return {
        "data": {
            "mal_id": 1,
            "title": "Cowboy Bebop",
            "title_english": "Cowboy Bebop",
            "title_japanese": "カウボーイビバップ",
            "synopsis": "In the year 2071, humanity has colonized several of the planets...",
            "type": "TV",
            "source": "Original",
            "episodes": 26,
            "status": "Finished Airing",
            "duration": "24 min per ep",
            "rating": "R - 17+ (violence & profanity)",
            "score": 8.76,
            "scored_by": 742115,
            "rank": 28,
            "popularity": 43,
            "members": 1325837,
            "favorites": 54287,
            "year": 1998,
            "season": "spring",
            "broadcast": {
                "day": "Saturday",
                "time": "01:00"
            },
            "aired": {
                "from": "1998-04-03T00:00:00+00:00",
                "to": "1999-04-24T00:00:00+00:00"
            },
            "producers": [
                {"mal_id": 23, "name": "Bandai Visual"}
            ],
            "studios": [
                {"mal_id": 14, "name": "Sunrise"}
            ],
            "genres": [
                {"mal_id": 1, "name": "Action"},
                {"mal_id": 8, "name": "Drama"}
            ],
            "statistics": {
                "watching": 12345,
                "completed": 67890,
                "on_hold": 1234,
                "dropped": 567,
                "plan_to_watch": 23456
            }
        }
    }


@pytest.fixture
def sample_top_anime_data():
    """Sample top anime data from Jikan API."""
    return {
        "data": [
            {
                "mal_id": 1,
                "title": "Cowboy Bebop",
                "score": 8.76,
                "rank": 28
            },
            {
                "mal_id": 5,
                "title": "Fullmetal Alchemist: Brotherhood",
                "score": 9.1,
                "rank": 1
            }
        ]
    }


@pytest.fixture
def sample_processed_csv():
    """Sample processed CSV data."""
    return """anime_id,title,title_english,score,episodes,year,season
1,Cowboy Bebop,Cowboy Bebop,8.76,26,1998,spring
5,Fullmetal Alchemist: Brotherhood,Fullmetal Alchemist: Brotherhood,9.1,64,2009,spring"""


class TestJikanAPIClient:
    """Test cases for JikanAPIClient."""
    
    def test_client_initialization(self):
        """Test API client initialization."""
        client = JikanAPIClient()
        assert client.base_url == "https://api.jikan.moe/v4"
        assert client.rate_limit_delay == 1.0
        assert client.max_retries == 3
    
    @patch('requests.Session.get')
    def test_successful_api_request(self, mock_get, sample_anime_data):
        """Test successful API request."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = sample_anime_data
        mock_get.return_value = mock_response
        
        client = JikanAPIClient()
        result = client.get_anime(1)
        
        assert result == sample_anime_data
        mock_get.assert_called_once()
    
    @patch('requests.Session.get')
    def test_api_rate_limiting(self, mock_get):
        """Test API rate limiting handling."""
        # First call returns 429, second call succeeds
        mock_response_429 = Mock()
        mock_response_429.status_code = 429
        mock_response_429.headers = {"Retry-After": "1"}
        
        mock_response_200 = Mock()
        mock_response_200.status_code = 200
        mock_response_200.json.return_value = {"data": {"mal_id": 1}}
        
        mock_get.side_effect = [mock_response_429, mock_response_200]
        
        client = JikanAPIClient()
        with patch('time.sleep'):  # Mock sleep to speed up test
            result = client.get_anime(1)
        
        assert result == {"data": {"mal_id": 1}}
        assert mock_get.call_count == 2
    
    @patch('requests.Session.get')
    def test_api_client_error(self, mock_get):
        """Test API client error handling."""
        mock_response = Mock()
        mock_response.status_code = 404
        mock_response.text = "Not Found"
        mock_get.return_value = mock_response
        
        client = JikanAPIClient()
        result = client.get_anime(99999)
        
        assert result is None


class TestS3Uploader:
    """Test cases for S3Uploader."""
    
    @mock_s3
    def test_s3_uploader_initialization(self):
        """Test S3 uploader initialization."""
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        uploader = S3Uploader(bucket_name=bucket_name, region="us-east-1")
        assert uploader.bucket_name == bucket_name
        assert uploader.region == "us-east-1"
    
    @mock_s3
    def test_json_upload(self, sample_anime_data):
        """Test JSON upload to S3."""
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        uploader = S3Uploader(bucket_name=bucket_name, region="us-east-1")
        success = uploader.upload_json(sample_anime_data, "test/anime_1.json")
        
        assert success is True
        
        # Verify upload
        response = s3_client.get_object(Bucket=bucket_name, Key="test/anime_1.json")
        uploaded_data = json.loads(response["Body"].read().decode())
        assert uploaded_data == sample_anime_data


class TestS3DataReader:
    """Test cases for S3DataReader."""
    
    @mock_s3
    def test_s3_reader_initialization(self):
        """Test S3 data reader initialization."""
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        reader = S3DataReader(bucket_name=bucket_name, region="us-east-1")
        assert reader.bucket_name == bucket_name
        assert reader.region == "us-east-1"
    
    @mock_s3
    def test_list_processed_files(self):
        """Test listing processed files."""
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Upload test files
        test_files = ["processed/anime.csv", "processed/statistics.csv"]
        for file_key in test_files:
            s3_client.put_object(Bucket=bucket_name, Key=file_key, Body="test,data\n1,test")
        
        reader = S3DataReader(bucket_name=bucket_name, region="us-east-1")
        files = reader.list_processed_files()
        
        assert len(files) == 2
        assert "processed/anime.csv" in files
        assert "processed/statistics.csv" in files
    
    @mock_s3
    def test_read_anime_data(self, sample_processed_csv):
        """Test reading anime data from S3."""
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Upload test CSV
        s3_client.put_object(
            Bucket=bucket_name,
            Key="processed/anime.csv",
            Body=sample_processed_csv.encode()
        )
        
        reader = S3DataReader(bucket_name=bucket_name, region="us-east-1")
        df = reader.read_anime_data()
        
        assert df is not None
        assert len(df) == 2
        assert df.iloc[0]["anime_id"] == 1
        assert df.iloc[0]["title"] == "Cowboy Bebop"
        assert df.iloc[1]["anime_id"] == 5


class TestAnimeDataFetcher:
    """Test cases for AnimeDataFetcher."""
    
    @mock_s3
    @patch('src.ingestion.fetch_jikan.JikanAPIClient')
    def test_fetch_single_anime(self, mock_client_class, sample_anime_data):
        """Test fetching single anime with mocked API and S3."""
        # Setup mock S3
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Mock API client
        mock_client = Mock()
        mock_client.get_anime_full.return_value = sample_anime_data
        mock_client.rate_limit_delay = 0.1
        mock_client_class.return_value = mock_client
        
        # Mock environment variables
        with patch.dict(os.environ, {
            "S3_BUCKET": bucket_name,
            "AWS_REGION": "us-east-1"
        }):
            fetcher = AnimeDataFetcher()
            result = fetcher.fetch_single_anime(1)
        
        assert result is True
        assert fetcher.stats["successful_fetches"] == 1
        assert fetcher.stats["successful_uploads"] == 1
        mock_client.get_anime_full.assert_called_once_with(1)
    
    @mock_s3
    @patch('src.ingestion.fetch_jikan.JikanAPIClient')
    def test_fetch_genres(self, mock_client_class):
        """Test fetching genres."""
        # Setup mock S3
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Mock API client
        genres_data = {"data": [{"mal_id": 1, "name": "Action"}]}
        mock_client = Mock()
        mock_client.get_anime_genres.return_value = genres_data
        mock_client.rate_limit_delay = 0.1
        mock_client_class.return_value = mock_client
        
        # Mock environment variables
        with patch.dict(os.environ, {
            "S3_BUCKET": bucket_name,
            "AWS_REGION": "us-east-1"
        }):
            fetcher = AnimeDataFetcher()
            result = fetcher.fetch_genres()
        
        assert result is True
        assert fetcher.stats["successful_fetches"] == 1
        assert fetcher.stats["successful_uploads"] == 1
    
    @mock_s3
    @patch('src.ingestion.fetch_jikan.JikanAPIClient')
    def test_fetch_top_anime(self, mock_client_class, sample_top_anime_data):
        """Test fetching top anime."""
        # Setup mock S3
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Mock API client
        mock_client = Mock()
        mock_client.get_top_anime.return_value = sample_top_anime_data
        mock_client.rate_limit_delay = 0.1
        mock_client_class.return_value = mock_client
        
        # Mock environment variables
        with patch.dict(os.environ, {
            "S3_BUCKET": bucket_name,
            "AWS_REGION": "us-east-1"
        }):
            fetcher = AnimeDataFetcher()
            anime_ids = fetcher.fetch_top_anime(max_pages=1)
        
        assert len(anime_ids) == 2
        assert 1 in anime_ids
        assert 5 in anime_ids
        assert fetcher.stats["successful_fetches"] == 1
        assert fetcher.stats["successful_uploads"] == 1


class TestIntegration:
    """Integration tests for the complete pipeline."""
    
    @mock_s3
    @patch('src.ingestion.fetch_jikan.JikanAPIClient')
    def test_end_to_end_pipeline(self, mock_client_class, sample_anime_data, sample_processed_csv):
        """Test end-to-end pipeline from API to S3."""
        # Setup mock S3
        bucket_name = "test-anime-bucket"
        s3_client = boto3.client("s3", region_name="us-east-1")
        s3_client.create_bucket(Bucket=bucket_name)
        
        # Mock API client
        mock_client = Mock()
        mock_client.get_anime_full.return_value = sample_anime_data
        mock_client.rate_limit_delay = 0.1
        mock_client_class.return_value = mock_client
        
        # Mock environment variables
        with patch.dict(os.environ, {
            "S3_BUCKET": bucket_name,
            "AWS_REGION": "us-east-1"
        }):
            # Step 1: Fetch and upload to S3
            fetcher = AnimeDataFetcher()
            fetch_result = fetcher.fetch_single_anime(1)
            assert fetch_result is True
            
            # Verify S3 upload
            s3_objects = s3_client.list_objects_v2(Bucket=bucket_name)
            assert s3_objects["KeyCount"] == 1
            
            # Step 2: Simulate processed data in S3
            s3_client.put_object(
                Bucket=bucket_name,
                Key="processed/anime.csv",
                Body=sample_processed_csv.encode()
            )
            
            # Step 3: Read processed data
            reader = S3DataReader(bucket_name=bucket_name, region="us-east-1")
            df = reader.read_anime_data()
            
            assert df is not None
            assert len(df) == 2
            assert df.iloc[0]["anime_id"] == 1


def test_environment_variables():
    """Test that required environment variables are documented."""
    env_example_path = Path(__file__).parent.parent / ".env.example"
    assert env_example_path.exists(), ".env.example file should exist"
    
    env_content = env_example_path.read_text()
    required_vars = [
        "JIKAN_BASE_URL",
        "S3_BUCKET",
        "AWS_REGION"
    ]
    
    for var in required_vars:
        assert var in env_content, f"{var} should be documented in .env.example"


# Pytest configuration and fixtures
@pytest.fixture(autouse=True)
def setup_test_environment():
    """Setup test environment before each test."""
    # Ensure test isolation
    with patch.dict(os.environ, {}, clear=True):
        yield


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v"])