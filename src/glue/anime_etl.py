#!/usr/bin/env python3
"""
Anime Data ETL Pipeline using PySpark

This script contains the core ETL logic for processing raw anime JSON data
into normalized, queryable datasets. It's designed to run in AWS Glue
but can also be executed locally for testing.

Architecture:
- Pure data processing logic (no AWS Glue job configuration)
- Configurable input/output paths
- Comprehensive data normalization
- Error handling and data quality validation
"""

import sys
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union

# PySpark imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, lit, when, coalesce, explode, explode_outer, 
    from_json, to_json, current_timestamp, 
    regexp_replace, trim, lower, split, collect_list,
    row_number, rank, size, array_contains
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DoubleType, BooleanType, ArrayType, MapType
)
from pyspark.sql.window import Window

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AnimeETL:
    """
    Core ETL pipeline for anime data processing.
    
    This class handles the transformation of raw JSON anime data
    into normalized, structured datasets optimized for analytics.
    """
    
    def __init__(self, spark: SparkSession, config: Dict):
        """
        Initialize the ETL pipeline.
        
        Args:
            spark: SparkSession instance
            config: Configuration dictionary with paths and settings
        """
        self.spark = spark
        self.config = config
        self.processing_timestamp = current_timestamp()
        
        # Validate required configuration
        required_keys = ['input_path', 'output_path']
        for key in required_keys:
            if key not in config:
                raise ValueError(f"Missing required configuration: {key}")
        
        logger.info(f"AnimeETL initialized")
        logger.info(f"Input path: {config['input_path']}")
        logger.info(f"Output path: {config['output_path']}")
    
    def read_json_data(self, path_pattern: Union[str, List[str]]) -> DataFrame:
        """
        Read JSON files with comprehensive error handling.
        
        Args:
            path_pattern: S3 or local path pattern for JSON files
            
        Returns:
            Spark DataFrame with raw JSON data
        """
        try:
            if isinstance(path_pattern, (list, tuple, set)):
                paths = [path for path in path_pattern if path]
            else:
                paths = [path_pattern]

            if not paths:
                raise ValueError("No input paths provided for JSON read")

            logger.info(f"Reading JSON files from: {', '.join(paths)}")
            
            df = self.spark.read \
                .option("multiline", "true") \
                .option("mode", "PERMISSIVE") \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .json(paths)
            
            record_count = df.count()
            joined_paths = ', '.join(paths)
            logger.info(f"Read {record_count} records from {joined_paths}")
            
            # Log corrupt records if any
            if "_corrupt_record" in df.columns:
                corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
                if corrupt_count > 0:
                    logger.warning(f"Found {corrupt_count} corrupt records")
                    # Show sample for debugging
                    df.filter(col("_corrupt_record").isNotNull()) \
                      .select("_corrupt_record") \
                      .show(3, truncate=False)
            
            return df
            
        except Exception as e:
            logger.error(f"Error reading JSON from {path_pattern}: {e}")
            raise

    def _build_input_paths(self, filename_pattern: str) -> List[str]:
        """Generate candidate S3 path patterns for a given file prefix."""
        base_path = self.config['input_path'].rstrip('/')
        
        # Default to the known data date since we're using the same dataset
        input_date = self.config.get('input_date', '2025-09-22')

        candidate_paths: List[str] = []

        # Priority 1: Use the specific date (our actual data structure)
        candidate_paths.append(f"{base_path}/{input_date}/{filename_pattern}")
        
        # Priority 2: Date-partitioned structure fallback
        candidate_paths.append(f"{base_path}/*/{filename_pattern}")
        
        # Priority 3: Direct under base path (fallback)
        candidate_paths.append(f"{base_path}/{filename_pattern}")

        # Deduplicate while preserving order
        seen = set()
        deduped_paths: List[str] = []
        for path in candidate_paths:
            if path not in seen:
                deduped_paths.append(path)
                seen.add(path)

        logger.info(f"Generated input paths for {filename_pattern}: {deduped_paths}")
        return deduped_paths
    
    def process_anime_details(self) -> Dict[str, DataFrame]:
        """
        Process anime details into normalized tables.
        
        Returns:
            Dictionary of DataFrames for different entities
        """
        logger.info("Processing anime details...")
        
        # Read only anime detail files (not recommendations/statistics which have different structure)  
        details_path = "s3://anime-mvp-data/raw/2025-09-22/anime_*.json"
        raw_df = self.read_json_data(details_path)
        
        if raw_df.count() == 0:
            logger.warning("No anime details found")
            return {}
        
        # Extract from nested 'data' field
        anime_df = raw_df.select("data.*").filter(col("mal_id").isNotNull())
        
        # Main anime table
        main_anime = anime_df.select(
            col("mal_id").alias("anime_id"),
            col("title"),
            col("title_english"),
            col("title_japanese"),
            to_json(col("title_synonyms")).alias("title_synonyms_json"),
            col("type"),
            col("source"),
            col("episodes"),
            col("status"),
            col("airing"),
            col("aired.from").alias("aired_from"),
            col("aired.to").alias("aired_to"),
            col("duration"),
            col("rating"),
            col("score"),
            col("scored_by"),
            col("rank"),
            col("popularity"),
            col("members"),
            col("favorites"),
            col("synopsis"),
            col("background"),
            col("season"),
            col("year"),
            col("broadcast.day").alias("broadcast_day"),
            col("broadcast.time").alias("broadcast_time"),
            col("approved"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Genre relationships
        genres_df = anime_df.select(
            col("mal_id").alias("anime_id"),
            explode_outer(col("genres")).alias("genre")
        ).filter(col("genre").isNotNull()).select(
            col("anime_id"),
            col("genre.mal_id").alias("genre_id"),
            col("genre.name").alias("genre_name"),
            col("genre.type").alias("genre_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Studio relationships
        studios_df = anime_df.select(
            col("mal_id").alias("anime_id"),
            explode_outer(col("studios")).alias("studio")
        ).filter(col("studio").isNotNull()).select(
            col("anime_id"),
            col("studio.mal_id").alias("studio_id"),
            col("studio.name").alias("studio_name"),
            col("studio.type").alias("studio_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Producer relationships
        producers_df = anime_df.select(
            col("mal_id").alias("anime_id"),
            explode_outer(col("producers")).alias("producer")
        ).filter(col("producer").isNotNull()).select(
            col("anime_id"),
            col("producer.mal_id").alias("producer_id"),
            col("producer.name").alias("producer_name"),
            col("producer.type").alias("producer_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Theme relationships
        themes_df = anime_df.select(
            col("mal_id").alias("anime_id"),
            explode_outer(col("themes")).alias("theme")
        ).filter(col("theme").isNotNull()).select(
            col("anime_id"),
            col("theme.mal_id").alias("theme_id"),
            col("theme.name").alias("theme_name"),
            col("theme.type").alias("theme_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Demographic relationships
        demographics_df = anime_df.select(
            col("mal_id").alias("anime_id"),
            explode_outer(col("demographics")).alias("demographic")
        ).filter(col("demographic").isNotNull()).select(
            col("anime_id"),
            col("demographic.mal_id").alias("demographic_id"),
            col("demographic.name").alias("demographic_name"),
            col("demographic.type").alias("demographic_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        # Relations - handle nested structure
        relations_df = anime_df.select(
            col("mal_id").alias("source_anime_id"),
            explode_outer(col("relations")).alias("relation_group")
        ).filter(col("relation_group").isNotNull()).select(
            col("source_anime_id"),
            col("relation_group.relation").alias("relation_type"),
            explode_outer(col("relation_group.entry")).alias("entry")
        ).filter(col("entry").isNotNull()).select(
            col("source_anime_id"),
            col("entry.mal_id").alias("target_anime_id"),
            col("entry.name").alias("target_title"),
            col("entry.type").alias("target_type"),
            col("relation_type"),
            self.processing_timestamp.alias("processed_at")
        )
        
        tables = {
            'anime': main_anime,
            'anime_genres': genres_df,
            'anime_studios': studios_df,
            'anime_producers': producers_df,
            'anime_themes': themes_df,
            'anime_demographics': demographics_df,
            'anime_relations': relations_df
        }
        
        # Log record counts
        for name, df in tables.items():
            count = df.count()
            logger.info(f"Generated {name}: {count} records")
        
        return tables
    
    def process_anime_statistics(self) -> Optional[DataFrame]:
        """Process anime statistics data."""
        logger.info("Processing anime statistics...")
        
        stats_path = "s3://anime-mvp-data/raw/2025-09-22/statistics_*.json"
        raw_df = self.read_json_data(stats_path)
        
        if raw_df.count() == 0:
            logger.warning("No anime statistics found")
            return None
        
        # Extract statistics with filename-based anime_id derivation
        # Note: In production, anime_id should be embedded in the data
        stats_df = raw_df.select("data.*").select(
            lit(None).cast(IntegerType()).alias("anime_id"),  # Needs enhancement
            col("watching"),
            col("completed"),
            col("on_hold"),
            col("dropped"),
            col("plan_to_watch"),
            col("total"),
            to_json(col("scores")).alias("scores_json"),
            self.processing_timestamp.alias("processed_at")
        )
        
        logger.info(f"Generated anime_statistics: {stats_df.count()} records")
        return stats_df
    
    def process_genres_master(self) -> Optional[DataFrame]:
        """Process master genres list."""
        logger.info("Processing genres master list...")
        
        genres_path = "s3://anime-mvp-data/raw/2025-09-22/genres_*.json"
        raw_df = self.read_json_data(genres_path)
        
        if raw_df.count() == 0:
            logger.warning("No genres master data found")
            return None
        
        genres_df = raw_df.select(
            explode_outer(col("data")).alias("genre")
        ).filter(col("genre").isNotNull()).select(
            col("genre.mal_id").alias("genre_id"),
            col("genre.name"),
            col("genre.url"),
            col("genre.count"),
            self.processing_timestamp.alias("processed_at")
        )
        
        logger.info(f"Generated genres_master: {genres_df.count()} records")
        return genres_df
    
    def process_top_anime(self) -> Optional[DataFrame]:
        """Process top anime rankings."""
        logger.info("Processing top anime rankings...")
        
        top_path = "s3://anime-mvp-data/raw/2025-09-22/top_*.json"
        raw_df = self.read_json_data(top_path)
        
        if raw_df.count() == 0:
            logger.warning("No top anime data found")
            return None
        
        top_df = raw_df.select(
            explode_outer(col("data")).alias("anime")
        ).filter(col("anime").isNotNull()).select(
            col("anime.mal_id").alias("anime_id"),
            col("anime.title"),
            col("anime.score"),
            col("anime.rank"),
            col("anime.popularity"),
            col("anime.members"),
            col("anime.type"),
            col("anime.episodes"),
            col("anime.status"),
            self.processing_timestamp.alias("processed_at")
        )
        
        logger.info(f"Generated top_anime: {top_df.count()} records")
        return top_df
    
    def process_seasonal_anime(self) -> Optional[DataFrame]:
        """Process seasonal anime data."""
        logger.info("Processing seasonal anime...")
        
        seasonal_path = "s3://anime-mvp-data/raw/2025-09-22/seasonal_*.json"
        raw_df = self.read_json_data(seasonal_path)
        
        if raw_df.count() == 0:
            logger.warning("No seasonal anime data found")
            return None
        
        seasonal_df = raw_df.select(
            col("season_name"),
            col("season_year"),
            explode_outer(col("data")).alias("anime")
        ).filter(col("anime").isNotNull()).select(
            col("anime.mal_id").alias("anime_id"),
            col("anime.title"),
            col("season_name"),
            col("season_year"),
            col("anime.type"),
            col("anime.episodes"),
            col("anime.status"),
            col("anime.score"),
            self.processing_timestamp.alias("processed_at")
        )
        
        logger.info(f"Generated seasonal_anime: {seasonal_df.count()} records")
        return seasonal_df
    
    def write_table(self, df: DataFrame, table_name: str, partition_cols: List[str] = None):
        """
        Write DataFrame to output location with optimal settings.
        
        Args:
            df: DataFrame to write
            table_name: Name of the table
            partition_cols: Optional list of partition columns
        """
        if df is None or df.count() == 0:
            logger.warning(f"Skipping empty table: {table_name}")
            return
        
        output_path = f"{self.config['output_path']}/{table_name}/"
        write_mode = self.config.get('write_mode', 'overwrite')
        output_format = self.config.get('output_format', 'parquet')
        
        logger.info(f"Writing {table_name} to {output_path}")
        logger.info(f"Records: {df.count()}, Columns: {len(df.columns)}")
        
        try:
            # Optimize for small to medium datasets
            writer = df.coalesce(1).write.mode(write_mode)
            
            if partition_cols:
                writer = writer.partitionBy(*partition_cols)
            
            if output_format == 'parquet':
                writer.option("compression", "snappy").parquet(output_path)
            elif output_format == 'delta':
                writer.format("delta").save(output_path)
            else:
                writer.csv(output_path, header=True)
            
            logger.info(f"Successfully wrote {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to write {table_name}: {e}")
            raise
    
    def validate_data_quality(self, tables: Dict[str, DataFrame]) -> Dict[str, Dict]:
        """
        Perform data quality validation on processed tables.
        
        Args:
            tables: Dictionary of table name to DataFrame
            
        Returns:
            Dictionary of validation results
        """
        logger.info("Validating data quality...")
        
        validation_results = {}
        
        for table_name, df in tables.items():
            if df is None:
                validation_results[table_name] = {'status': 'skipped', 'reason': 'empty_dataframe'}
                continue
            
            try:
                # Basic metrics
                record_count = df.count()
                column_count = len(df.columns)
                
                # Check for nulls in key columns
                null_checks = {}
                if 'anime_id' in df.columns:
                    null_count = df.filter(col('anime_id').isNull()).count()
                    null_checks['anime_id_nulls'] = null_count
                
                validation_results[table_name] = {
                    'status': 'validated',
                    'record_count': record_count,
                    'column_count': column_count,
                    'null_checks': null_checks,
                    'columns': df.columns
                }
                
                logger.info(f"✓ {table_name}: {record_count} records, {column_count} columns")
                
            except Exception as e:
                validation_results[table_name] = {
                    'status': 'failed',
                    'error': str(e)
                }
                logger.error(f"✗ {table_name}: Validation failed - {e}")
        
        return validation_results
    
    def run_full_pipeline(self) -> Dict[str, Dict]:
        """
        Execute the complete ETL pipeline.
        
        Returns:
            Dictionary with processing results and validation metrics
        """
        logger.info("Starting Anime ETL Pipeline...")
        start_time = datetime.now()
        
        try:
            # Process all data types
            all_tables = {}
            
            # 1. Process anime details (multiple related tables)
            anime_tables = self.process_anime_details()
            all_tables.update(anime_tables)
            
            # 2. Process other data types (temporarily disabled to fix data structure issues)
            processors = [
                # ('anime_statistics', self.process_anime_statistics),
                # ('genres_master', self.process_genres_master), 
                # ('top_anime', self.process_top_anime),
                # ('seasonal_anime', self.process_seasonal_anime)
            ]
            
            for table_name, processor_func in processors:
                try:
                    df = processor_func()
                    if df is not None:
                        all_tables[table_name] = df
                except Exception as e:
                    logger.error(f"Error processing {table_name}: {e}")
                    continue
            
            # 3. Validate data quality
            validation_results = self.validate_data_quality(all_tables)
            
            # 4. Write all tables
            for table_name, df in all_tables.items():
                try:
                    # Apply table-specific partitioning
                    partition_cols = None
                    if table_name == 'anime' and 'year' in df.columns:
                        partition_cols = ['year']
                    elif table_name == 'seasonal_anime' and 'season_year' in df.columns:
                        partition_cols = ['season_year']
                    
                    self.write_table(df, table_name, partition_cols)
                    
                except Exception as e:
                    logger.error(f"Error writing {table_name}: {e}")
                    continue
            
            # 5. Generate summary
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            total_records = sum(r.get('record_count', 0) for r in validation_results.values())
            
            summary = {
                'status': 'completed',
                'duration_seconds': duration,
                'tables_processed': len(all_tables),
                'total_records': total_records,
                'validation_results': validation_results
            }
            
            logger.info(f"ETL Pipeline completed successfully!")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Tables processed: {len(all_tables)}")
            logger.info(f"Total records: {total_records}")
            
            return summary
            
        except Exception as e:
            logger.error(f"ETL Pipeline failed: {e}")
            raise


def create_spark_session(app_name: str = "AnimeETL") -> SparkSession:
    """
    Create optimized Spark session for anime data processing.
    
    Args:
        app_name: Spark application name
        
    Returns:
        Configured SparkSession
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.files.maxPartitionBytes", "128MB") \
        .config("spark.sql.files.openCostInBytes", "4MB") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    # Set appropriate log level
    spark.sparkContext.setLogLevel("WARN")
    
    return spark


def main():
    """
    Main entry point for the ETL script.
    Can be called from AWS Glue or run locally for testing.
    """
    # Default configuration
    default_config = {
        'input_path': 's3://anime-mvp-data/raw',
        'output_path': 's3://anime-mvp-data/processed',
        'write_mode': 'overwrite',
        'output_format': 'csv'
    }
    
    # Override with environment variables or job parameters if available
    try:
        # Try to get AWS Glue job parameters
        from awsglue.utils import getResolvedOptions
        args = getResolvedOptions(sys.argv, [
            'input_path', 'output_path', 'write_mode', 'output_format', 'input_date'
        ])
        default_config.update(args)
    except:
        # Running locally - use defaults or environment variables
        import os
        for key in default_config.keys():
            env_key = key.upper()
            if env_key in os.environ:
                default_config[key] = os.environ[env_key]
        
        # Also check for input_date
        if 'INPUT_DATE' in os.environ:
            default_config['input_date'] = os.environ['INPUT_DATE']
    
    logger.info(f"ETL Configuration: {default_config}")
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Initialize and run ETL
        etl = AnimeETL(spark, default_config)
        results = etl.run_full_pipeline()
        
        logger.info("ETL execution completed successfully")
        return results
        
    except Exception as e:
        logger.error(f"ETL execution failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()