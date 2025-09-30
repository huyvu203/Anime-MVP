#!/usr/bin/env python3
"""
Test the updated Data Retrieval Agent with Athena integration.
"""

import sys
import logging
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from agents.data_retrieval_agent import DataRetrievalAgent

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_data_retrieval_agent():
    """Test the Data Retrieval Agent with Athena."""
    
    print("🧪 Testing Data Retrieval Agent with Athena")
    print("=" * 50)
    
    try:
        # Create agent
        agent = DataRetrievalAgent()
        print("✅ Data Retrieval Agent created successfully")
        
        # Test search by title
        print("\\n📝 Test 1: Search by title")
        request = {
            "query_type": "search_title",
            "parameters": {
                "title": "attack",
                "limit": 5
            }
        }
        
        result = agent.process_data_request(request)
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Found {result['count']} anime matching 'attack'")
            for i, anime in enumerate(result['results'][:3]):
                print(f"  {i+1}. {anime['title']} - Score: {anime.get('score')}")
        else:
            print(f"Error: {result.get('message')}")
        
        # Test genre filter
        print("\\n📝 Test 2: Filter by genre")
        request = {
            "query_type": "genre_filter", 
            "parameters": {
                "genre": "action",
                "limit": 5
            }
        }
        
        result = agent.process_data_request(request)
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Found {result['count']} action anime")
            for i, anime in enumerate(result['results'][:3]):
                print(f"  {i+1}. {anime['title']} - Score: {anime.get('score')}")
        else:
            print(f"Error: {result.get('message')}")
        
        # Test top rated
        print("\\n📝 Test 3: Top rated anime")
        request = {
            "query_type": "top_rated",
            "parameters": {
                "limit": 5,
                "min_score": 8.5
            }
        }
        
        result = agent.process_data_request(request)
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Found {result['count']} top-rated anime")
            for i, anime in enumerate(result['results'][:3]):
                print(f"  {i+1}. {anime['title']} - Score: {anime.get('score')}")
        else:
            print(f"Error: {result.get('message')}")
        
        # Test currently airing
        print("\\n📝 Test 4: Currently airing")
        request = {
            "query_type": "currently_airing",
            "parameters": {
                "limit": 5
            }
        }
        
        result = agent.process_data_request(request)
        print(f"Status: {result['status']}")
        if result['status'] == 'success':
            print(f"Found {result['count']} currently airing anime")
            for i, anime in enumerate(result['results'][:3]):
                print(f"  {i+1}. {anime['title']} - Status: {anime.get('status')}")
        else:
            print(f"Error: {result.get('message')}")
        
        print("\\n✅ Data Retrieval Agent test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.exception("Test error details:")
        return False

if __name__ == "__main__":
    success = test_data_retrieval_agent()
    sys.exit(0 if success else 1)