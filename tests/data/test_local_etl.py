#!/usr/bin/env python3
"""
Test local ETL processing with our sample data.
"""

import sys
from pathlib import Path
import tempfile
import os

# Add src to path
sys.path.append('src')

def test_local_etl():
    """Test ETL processing locally with sample data."""
    try:
        # Try to import pyspark
        from pyspark.sql import SparkSession
        from src.glue.anime_etl import AnimeETL
        
        print("✓ Successfully imported PySpark and AnimeETL")
        
        # Create local Spark session for testing
        spark = SparkSession.builder \
            .appName("LocalAnimeETLTest") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()
        
        print("✓ Created local Spark session")
        
        # Configure ETL with local paths
        config = {
            'input_path': str(Path('data/raw').absolute()),
            'output_path': str(Path('data/processed').absolute()),
            'input_date': '2025-09-22',  # Use our known data date
            'write_mode': 'overwrite',
            'output_format': 'parquet'
        }
        
        # Initialize ETL
        etl = AnimeETL(spark, config)
        print("✓ Initialized AnimeETL")
        
        # Test reading anime data
        anime_paths = etl._build_input_paths('anime_*.json')
        print(f"✓ Generated anime paths: {anime_paths}")
        
        # Try to read a small sample
        anime_df = etl.read_json_data(anime_paths)
        print(f"✓ Successfully read anime data: {anime_df.count()} records")
        
        # Show schema
        print("✓ Anime data schema:")
        anime_df.printSchema()
        
        # Test reading statistics data
        stats_paths = etl._build_input_paths('statistics_*.json')
        stats_df = etl.read_json_data(stats_paths)
        print(f"✓ Successfully read statistics data: {stats_df.count()} records")
        
        # Clean up
        spark.stop()
        print("✓ Local ETL test completed successfully!")
        
    except ImportError as e:
        print(f"✗ Missing dependency: {e}")
        print("This is expected since we're not in a Spark environment")
        return False
    except Exception as e:
        print(f"✗ ETL test failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_local_etl()