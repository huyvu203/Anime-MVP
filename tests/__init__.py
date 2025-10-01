"""
Test Configuration and Organization for Anime MVP

This module provides centralized test configuration and utilities
for the anime recommendation system tests.
"""

import os
import sys
from pathlib import Path

# Add src to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

# Test categories
TEST_CATEGORIES = {
    "agents": {
        "description": "Tests for custom agents (UI Agent, Data Retrieval Agent)",
        "files": [
            "test_user_interface_agent.py",
            "test_data_retrieval_agent.py", 
            "test_athena_agent.py"
        ]
    },
    "infrastructure": {
        "description": "Tests for AWS infrastructure setup and deployment",
        "files": [
            "test_glue_deployment.py",
            "test_quick_deployment.py",
            "fix_glue_role.py",
            "add_athena_permissions.py"
        ]
    },
    "integration": {
        "description": "Integration tests for sequential workflow and orchestration",
        "files": [
            "test_orchestration.py",
            "test_connection.py",
            "sequential_workflow.py"
        ]
    },
    "data": {
        "description": "Tests for data processing, ETL, and API endpoints",
        "files": [
            "test_athena_queries.py",
            "test_local_etl.py",
            "test_jikan_api.py",
            "test_pipeline.py",
            "explore_jikan_endpoints.py"
        ]
    }
}

# Environment setup for tests
def setup_test_environment():
    """Set up the test environment with required paths and configurations."""
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Verify required environment variables
    required_vars = ["OPENAI_API_KEY", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"⚠️  Warning: Missing environment variables: {', '.join(missing_vars)}")
        print("Some tests may fail without proper AWS and OpenAI credentials.")
    
    return len(missing_vars) == 0

# Test utilities
class TestLogger:
    """Simple test logger for consistent output formatting."""
    
    @staticmethod
    def info(message: str):
        print(f"ℹ️  {message}")
    
    @staticmethod
    def success(message: str):
        print(f"✅ {message}")
    
    @staticmethod
    def error(message: str):
        print(f"❌ {message}")
    
    @staticmethod
    def warning(message: str):
        print(f"⚠️  {message}")

# Test data helpers
def get_test_data_path() -> Path:
    """Get the path to test data files."""
    return project_root / "data"

def get_sample_anime_data():
    """Get sample anime data for testing."""
    return {
        "title": "Test Anime",
        "score": 8.5,
        "year": 2023,
        "type": "TV",
        "episodes": 24,
        "status": "Completed"
    }