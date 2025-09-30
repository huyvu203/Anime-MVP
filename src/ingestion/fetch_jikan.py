#!/usr/bin/env python3
"""
Jikan API fetcher for anime data ingestion.

This module fetches anime data from the Jikan API and uploads raw JSON files to S3.
Supports retries, rate limiting, and robust error handling.
"""

import json
import logging
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import boto3
import requests
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class JikanAPIClient:
    """Client for interacting with the Jikan API."""
    
    def __init__(
        self,
        base_url: str = None,
        rate_limit_delay: float = None,
        max_retries: int = 3,
        timeout: int = 30
    ):
        self.base_url = base_url or os.getenv("JIKAN_BASE_URL", "https://api.jikan.moe/v4")
        self.rate_limit_delay = rate_limit_delay or float(os.getenv("JIKAN_RATE_LIMIT_DELAY", "1.0"))
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "anime-mvp-pipeline/1.0"
        })
        
    def _make_request(self, endpoint: str) -> Optional[Dict]:
        """Make a request to the Jikan API with retries and rate limiting."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        
        for attempt in range(self.max_retries):
            try:
                logger.debug(f"Making request to {url} (attempt {attempt + 1})")
                
                response = self.session.get(url, timeout=self.timeout)
                
                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", 60))
                    logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                
                # Handle successful responses
                if response.status_code == 200:
                    return response.json()
                
                # Handle client errors (4xx)
                elif 400 <= response.status_code < 500:
                    logger.warning(f"Client error {response.status_code} for {url}: {response.text}")
                    return None
                
                # Handle server errors (5xx) - retry
                else:
                    logger.warning(f"Server error {response.status_code} for {url}. Retrying...")
                    
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed for {url}: {e}")
                
            # Wait before retry (exponential backoff)
            if attempt < self.max_retries - 1:
                wait_time = self.rate_limit_delay * (2 ** attempt)
                logger.info(f"Waiting {wait_time:.1f} seconds before retry...")
                time.sleep(wait_time)
        
        logger.error(f"Failed to fetch data from {url} after {self.max_retries} attempts")
        return None
    
    def get_anime(self, anime_id: int) -> Optional[Dict]:
        """Fetch anime data by ID."""
        return self._make_request(f"anime/{anime_id}")
    
    def get_anime_full(self, anime_id: int) -> Optional[Dict]:
        """Fetch full anime data by ID (includes all related data)."""
        return self._make_request(f"anime/{anime_id}/full")
    
    def get_anime_statistics(self, anime_id: int) -> Optional[Dict]:
        """Fetch anime statistics by ID."""
        return self._make_request(f"anime/{anime_id}/statistics")
    
    def get_top_anime(self, page: int = 1, limit: int = 25) -> Optional[Dict]:
        """Fetch top anime list."""
        return self._make_request(f"top/anime?page={page}&limit={limit}")
    
    def get_anime_recommendations(self, anime_id: int) -> Optional[Dict]:
        """Fetch anime recommendations by ID."""
        return self._make_request(f"anime/{anime_id}/recommendations")
    
    def get_anime_genres(self) -> Optional[Dict]:
        """Fetch all anime genres."""
        return self._make_request("genres/anime")
    
    def get_seasonal_anime(self, year: int, season: str, page: int = 1) -> Optional[Dict]:
        """Fetch seasonal anime."""
        return self._make_request(f"seasons/{year}/{season}?page={page}")


class S3Uploader:
    """Handle S3 uploads for raw JSON data."""
    
    def __init__(self, bucket_name: str = None, region: str = None):
        self.bucket_name = bucket_name or os.getenv("S3_BUCKET", "anime-data")
        self.region = region or os.getenv("AWS_REGION", "us-east-1")
        
        try:
            self.s3_client = boto3.client("s3", region_name=self.region)
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
    
    def upload_json(self, data: Dict, s3_key: str, local_backup_path: str = None) -> bool:
        """Upload JSON data to S3 with optional local backup."""
        try:
            # Convert data to JSON string
            json_data = json.dumps(data, indent=2, ensure_ascii=False)
            
            # Save local backup if path provided
            if local_backup_path:
                local_path = Path(local_backup_path)
                local_path.parent.mkdir(parents=True, exist_ok=True)
                with open(local_path, 'w', encoding='utf-8') as f:
                    f.write(json_data)
                logger.debug(f"Saved local backup: {local_path}")
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"Uploaded to S3: s3://{self.bucket_name}/{s3_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {s3_key}: {e}")
            return False


class AnimeDataFetcher:
    """Main class for fetching anime data from multiple endpoints and uploading to S3."""
    
    def __init__(self):
        self.api_client = JikanAPIClient()
        self.s3_uploader = S3Uploader()
        self.date = os.getenv("DATE") or datetime.now().strftime("%Y-%m-%d")
        self.raw_prefix = os.getenv("S3_RAW_PREFIX", "raw")
        self.local_backup_dir = Path("data/raw") / self.date
        
        # Statistics
        self.stats = {
            "total_requested": 0,
            "successful_fetches": 0,
            "successful_uploads": 0,
            "failures": [],
            "anime_ids": set()  # Track collected anime IDs
        }
    
    def _upload_data(self, data: Dict, filename: str) -> bool:
        """Helper method to upload data to S3 and save local backup."""
        if data is None:
            return False
            
        s3_key = f"{self.raw_prefix}/{self.date}/{filename}"
        local_path = self.local_backup_dir / filename
        
        return self.s3_uploader.upload_json(data, s3_key, str(local_path))
    
    def fetch_genres(self) -> bool:
        """Fetch anime genres (static list, pull once)."""
        logger.info("Fetching anime genres...")
        self.stats["total_requested"] += 1
        
        genres_data = self.api_client.get_anime_genres()
        if genres_data:
            self.stats["successful_fetches"] += 1
            if self._upload_data(genres_data, "genres.json"):
                self.stats["successful_uploads"] += 1
                return True
            else:
                self.stats["failures"].append("upload_genres")
        else:
            self.stats["failures"].append("genres")
        
        time.sleep(self.api_client.rate_limit_delay)
        return False
    
    def fetch_top_anime(self, max_pages: int = 5) -> List[int]:
        """Fetch top anime (2-5 pages, ~100-250 entries) and return anime IDs."""
        logger.info(f"Fetching top anime ({max_pages} pages)...")
        anime_ids = []
        
        for page in range(1, max_pages + 1):
            self.stats["total_requested"] += 1
            
            top_data = self.api_client.get_top_anime(page=page)
            if top_data:
                self.stats["successful_fetches"] += 1
                
                # Extract anime IDs
                if 'data' in top_data:
                    for anime in top_data['data']:
                        if 'mal_id' in anime:
                            anime_ids.append(anime['mal_id'])
                
                # Upload raw data
                filename = f"top_anime_page_{page}.json"
                if self._upload_data(top_data, filename):
                    self.stats["successful_uploads"] += 1
                else:
                    self.stats["failures"].append(f"upload_top_page_{page}")
            else:
                self.stats["failures"].append(f"top_anime_page_{page}")
            
            time.sleep(self.api_client.rate_limit_delay)
        
        self.stats["anime_ids"].update(anime_ids)
        logger.info(f"Collected {len(anime_ids)} anime IDs from top anime")
        return anime_ids
    
    def fetch_seasonal_anime(self, seasons_config: List[Dict]) -> List[int]:
        """
        Fetch seasonal anime for specified seasons.
        seasons_config: List of dicts with 'year', 'season', 'max_pages'
        """
        logger.info(f"Fetching seasonal anime for {len(seasons_config)} seasons...")
        anime_ids = []
        
        for config in seasons_config:
            year = config['year']
            season = config['season']
            max_pages = config.get('max_pages', 2)
            
            logger.info(f"Fetching {season} {year} (up to {max_pages} pages)...")
            
            for page in range(1, max_pages + 1):
                self.stats["total_requested"] += 1
                
                seasonal_data = self.api_client.get_seasonal_anime(year, season, page)
                if seasonal_data:
                    self.stats["successful_fetches"] += 1
                    
                    # Extract anime IDs
                    if 'data' in seasonal_data:
                        for anime in seasonal_data['data']:
                            if 'mal_id' in anime:
                                anime_ids.append(anime['mal_id'])
                    
                    # Upload raw data
                    filename = f"seasonal_{year}_{season}_page_{page}.json"
                    if self._upload_data(seasonal_data, filename):
                        self.stats["successful_uploads"] += 1
                    else:
                        self.stats["failures"].append(f"upload_seasonal_{year}_{season}_page_{page}")
                else:
                    self.stats["failures"].append(f"seasonal_{year}_{season}_page_{page}")
                
                time.sleep(self.api_client.rate_limit_delay)
        
        self.stats["anime_ids"].update(anime_ids)
        logger.info(f"Collected {len(anime_ids)} anime IDs from seasonal anime")
        return anime_ids
    
    def fetch_anime_details(self, anime_ids: List[int]) -> bool:
        """Fetch detailed metadata for each anime ID."""
        logger.info(f"Fetching detailed metadata for {len(anime_ids)} anime...")
        
        for i, anime_id in enumerate(anime_ids, 1):
            if i % 50 == 0:
                logger.info(f"Processing anime {i}/{len(anime_ids)}: ID {anime_id}")
            
            self.stats["total_requested"] += 1
            
            # Fetch full anime data
            anime_data = self.api_client.get_anime_full(anime_id)
            if anime_data:
                self.stats["successful_fetches"] += 1
                
                filename = f"anime_{anime_id}.json"
                if self._upload_data(anime_data, filename):
                    self.stats["successful_uploads"] += 1
                else:
                    self.stats["failures"].append(f"upload_anime_{anime_id}")
            else:
                self.stats["failures"].append(f"anime_{anime_id}")
            
            time.sleep(self.api_client.rate_limit_delay)
        
        return True
    
    def fetch_anime_statistics(self, anime_ids: List[int]) -> bool:
        """Fetch statistics for each anime ID."""
        logger.info(f"Fetching statistics for {len(anime_ids)} anime...")
        
        for i, anime_id in enumerate(anime_ids, 1):
            if i % 50 == 0:
                logger.info(f"Processing statistics {i}/{len(anime_ids)}: ID {anime_id}")
            
            self.stats["total_requested"] += 1
            
            # Fetch anime statistics
            stats_data = self.api_client.get_anime_statistics(anime_id)
            if stats_data:
                self.stats["successful_fetches"] += 1
                
                filename = f"statistics_{anime_id}.json"
                if self._upload_data(stats_data, filename):
                    self.stats["successful_uploads"] += 1
                else:
                    self.stats["failures"].append(f"upload_statistics_{anime_id}")
            else:
                self.stats["failures"].append(f"statistics_{anime_id}")
            
            time.sleep(self.api_client.rate_limit_delay)
        
        return True
    
    def fetch_anime_recommendations(self, anime_ids: List[int], max_recs_per_anime: int = 10) -> bool:
        """Fetch recommendations for each anime ID (limit to top 5-10 per anime)."""
        logger.info(f"Fetching recommendations for {len(anime_ids)} anime (max {max_recs_per_anime} each)...")
        
        for i, anime_id in enumerate(anime_ids, 1):
            if i % 50 == 0:
                logger.info(f"Processing recommendations {i}/{len(anime_ids)}: ID {anime_id}")
            
            self.stats["total_requested"] += 1
            
            # Fetch anime recommendations
            recs_data = self.api_client.get_anime_recommendations(anime_id)
            if recs_data:
                self.stats["successful_fetches"] += 1
                
                # Limit recommendations if needed
                if 'data' in recs_data and len(recs_data['data']) > max_recs_per_anime:
                    recs_data['data'] = recs_data['data'][:max_recs_per_anime]
                
                filename = f"recommendations_{anime_id}.json"
                if self._upload_data(recs_data, filename):
                    self.stats["successful_uploads"] += 1
                else:
                    self.stats["failures"].append(f"upload_recommendations_{anime_id}")
            else:
                self.stats["failures"].append(f"recommendations_{anime_id}")
            
            time.sleep(self.api_client.rate_limit_delay)
        
        return True
    
    def fetch_mvp_dataset(self) -> Dict:
        """Fetch the complete MVP dataset from all endpoints."""
        logger.info("Starting MVP dataset collection...")
        
        # 1. Fetch genres (static list)
        self.fetch_genres()
        
        # 2. Fetch top anime (2-5 pages)
        top_anime_ids = self.fetch_top_anime(max_pages=5)
        
        # 3. Fetch seasonal anime (current + last 1-2 seasons)
        current_year = datetime.now().year
        current_month = datetime.now().month
        
        # Determine current season
        if current_month in [12, 1, 2]:
            current_season = "winter"
        elif current_month in [3, 4, 5]:
            current_season = "spring"
        elif current_month in [6, 7, 8]:
            current_season = "summer"
        else:
            current_season = "fall"
        
        # Define seasons to fetch
        seasons_config = [
            {"year": current_year, "season": current_season, "max_pages": 3},
        ]
        
        # Add previous seasons
        season_order = ["winter", "spring", "summer", "fall"]
        current_idx = season_order.index(current_season)
        
        for i in range(1, 3):  # Last 2 seasons
            prev_idx = (current_idx - i) % 4
            prev_season = season_order[prev_idx]
            prev_year = current_year if current_idx >= i else current_year - 1
            
            seasons_config.append({
                "year": prev_year,
                "season": prev_season,
                "max_pages": 2
            })
        
        seasonal_anime_ids = self.fetch_seasonal_anime(seasons_config)
        
        # 4. Combine and deduplicate anime IDs
        all_anime_ids = list(set(top_anime_ids + seasonal_anime_ids))
        logger.info(f"Total unique anime IDs to process: {len(all_anime_ids)}")
        
        # 5. Fetch detailed data for each anime
        self.fetch_anime_details(all_anime_ids)
        
        # 6. Fetch statistics for each anime
        self.fetch_anime_statistics(all_anime_ids)
        
        # 7. Fetch recommendations for each anime (limit to top 10)
        self.fetch_anime_recommendations(all_anime_ids, max_recs_per_anime=10)
        
        return self.stats
    
    def fetch_anime_range(self, start_id: int, end_id: int) -> Dict:
        """Fetch anime data for a range of IDs (legacy method for compatibility)."""
        logger.info(f"Fetching anime data from ID {start_id} to {end_id}")
        anime_ids = list(range(start_id, end_id + 1))
        self.fetch_anime_details(anime_ids)
        return self.stats
    
    def fetch_single_anime(self, anime_id: int) -> bool:
        """Fetch data for a single anime ID (legacy method for compatibility)."""
        logger.info(f"Fetching anime ID: {anime_id}")
        return len(self.fetch_anime_details([anime_id])) > 0
    
    def print_summary(self):
        """Print execution summary."""
        logger.info("=" * 50)
        logger.info("EXECUTION SUMMARY")
        logger.info("=" * 50)
        logger.info(f"Total requests: {self.stats['total_requested']}")
        logger.info(f"Successful fetches: {self.stats['successful_fetches']}")
        logger.info(f"Successful uploads: {self.stats['successful_uploads']}")
        logger.info(f"Unique anime IDs collected: {len(self.stats['anime_ids'])}")
        logger.info(f"Failures: {len(self.stats['failures'])}")
        
        if self.stats["failures"]:
            logger.info("Failed operations:")
            for failure in self.stats["failures"][:10]:  # Show first 10 failures
                logger.info(f"  - {failure}")
            if len(self.stats["failures"]) > 10:
                logger.info(f"  ... and {len(self.stats['failures']) - 10} more")


def main():
    """Main execution function."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch anime data from Jikan API")
    parser.add_argument("--mvp", action="store_true", help="Fetch complete MVP dataset (all endpoints)")
    parser.add_argument("--anime-id", type=int, help="Fetch single anime by ID")
    parser.add_argument("--start-id", type=int, default=1, help="Start anime ID for range")
    parser.add_argument("--end-id", type=int, default=100, help="End anime ID for range")
    parser.add_argument("--max-id", type=int, help="Maximum anime ID (overrides end-id)")
    parser.add_argument("--genres-only", action="store_true", help="Fetch only genres")
    parser.add_argument("--top-only", action="store_true", help="Fetch only top anime")
    parser.add_argument("--seasonal-only", action="store_true", help="Fetch only seasonal anime")
    
    args = parser.parse_args()
    
    # Override end_id if max_id is provided from env or args
    max_id_env = os.getenv("MAX_ANIME_ID")
    if args.max_id:
        args.end_id = args.max_id
    elif max_id_env:
        args.end_id = min(args.end_id, int(max_id_env))
    
    try:
        fetcher = AnimeDataFetcher()
        
        if args.mvp:
            # Fetch complete MVP dataset
            logger.info("Fetching complete MVP dataset from all endpoints...")
            fetcher.fetch_mvp_dataset()
        elif args.genres_only:
            # Fetch only genres
            fetcher.fetch_genres()
        elif args.top_only:
            # Fetch only top anime
            fetcher.fetch_top_anime(max_pages=5)
        elif args.seasonal_only:
            # Fetch only seasonal anime
            current_year = datetime.now().year
            seasons_config = [
                {"year": current_year, "season": "fall", "max_pages": 3},
                {"year": current_year, "season": "summer", "max_pages": 2},
            ]
            fetcher.fetch_seasonal_anime(seasons_config)
        elif args.anime_id:
            # Fetch single anime
            success = fetcher.fetch_single_anime(args.anime_id)
            if success:
                logger.info(f"Successfully fetched anime {args.anime_id}")
            else:
                logger.error(f"Failed to fetch anime {args.anime_id}")
        else:
            # Fetch range of anime (legacy mode)
            fetcher.fetch_anime_range(args.start_id, args.end_id)
        
        fetcher.print_summary()
        
    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())