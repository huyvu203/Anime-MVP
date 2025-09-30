#!/usr/bin/env python3
"""
Main execution script for the anime data pipeline.

This script provides a simple interface to run different components
of the pipeline with appropriate error handling and logging.
"""

import argparse
import logging
import sys
from pathlib import Path

# Add src directory to path
src_path = Path(__file__).parent / "src"
sys.path.insert(0, str(src_path))

def setup_logging(level: str = "INFO"):
    """Setup logging configuration."""
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

def run_fetcher(args):
    """Run the Jikan API fetcher."""
    from ingestion.fetch_jikan import main as fetch_main
    
    # Build arguments for fetcher
    fetch_args = ["fetch_jikan.py"]
    if args.anime_id:
        fetch_args.extend(["--anime-id", str(args.anime_id)])
    if args.start_id:
        fetch_args.extend(["--start-id", str(args.start_id)])
    if args.end_id:
        fetch_args.extend(["--end-id", str(args.end_id)])
    if args.max_id:
        fetch_args.extend(["--max-id", str(args.max_id)])
    
    # Temporarily replace sys.argv
    original_argv = sys.argv
    sys.argv = fetch_args
    
    try:
        return fetch_main()
    finally:
        sys.argv = original_argv

def run_loader(args):
    """Run the RDS loader."""
    from ingestion.load_rds import main as load_main
    
    # Build arguments for loader
    load_args = ["load_rds.py"]
    if args.create_schema:
        load_args.append("--create-schema")
    if args.table:
        load_args.extend(["--table", args.table])
    
    # Temporarily replace sys.argv
    original_argv = sys.argv
    sys.argv = load_args
    
    try:
        return load_main()
    finally:
        sys.argv = original_argv

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Anime MVP Data Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Fetch single anime
  poetry run python run.py fetch --anime-id 1
  
  # Fetch range of anime
  poetry run python run.py fetch --start-id 1 --end-id 100
  
  # Load data to RDS
  poetry run python run.py load --create-schema
  
  # Load specific table
  poetry run python run.py load --table anime
        """
    )
    
    parser.add_argument(
        "--log-level", 
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch data from Jikan API")
    fetch_parser.add_argument("--anime-id", type=int, help="Fetch single anime by ID")
    fetch_parser.add_argument("--start-id", type=int, default=1, help="Start anime ID for range")
    fetch_parser.add_argument("--end-id", type=int, default=100, help="End anime ID for range")
    fetch_parser.add_argument("--max-id", type=int, help="Maximum anime ID (overrides end-id)")
    
    # Load command
    load_parser = subparsers.add_parser("load", help="Load data to RDS")
    load_parser.add_argument("--create-schema", action="store_true", 
                           help="Create database schema before loading")
    load_parser.add_argument("--table", choices=["anime", "statistics", "genres"],
                           help="Load specific table only")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging(args.log_level)
    
    if not args.command:
        parser.print_help()
        return 1
    
    try:
        if args.command == "fetch":
            return run_fetcher(args)
        elif args.command == "load":
            return run_loader(args)
        else:
            print(f"Unknown command: {args.command}")
            return 1
    
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        return 130
    except Exception as e:
        logging.error(f"Unexpected error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    exit(main())