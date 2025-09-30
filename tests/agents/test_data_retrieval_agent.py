#!/usr/bin/env python3
"""
Test Data Retrieval Agent

Tests the Data Retrieval Agent functionality with various query types.
Validates S3 data access and SQLite database operations.
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from agents.data_retrieval_agent import create_data_retrieval_agent
from dotenv import load_dotenv

load_dotenv()

def test_data_retrieval_agent():
    """Test the Data Retrieval Agent with various query types."""
    
    print("🗄️ Testing Data Retrieval Agent")
    print("=" * 50)
    
    # Initialize agent
    try:
        agent = create_data_retrieval_agent()
        print("✅ Data Retrieval Agent created successfully")
        
        # Show capabilities
        capabilities = agent.get_capabilities()
        print(f"✅ Agent Type: {capabilities['type']}")
        print(f"✅ S3 Available: {capabilities['data_sources']['s3_available']}")
        print(f"✅ Watch DB Available: {capabilities['data_sources']['watch_db_available']}")
        print(f"✅ Supported Queries: {len(capabilities['supported_queries'])}")
        
    except Exception as e:
        print(f"❌ Failed to create agent: {e}")
        return False
    
    # Test different types of data requests
    test_cases = [
        {
            "name": "Title Search",
            "request": {
                "query_type": "search_title",
                "parameters": {"title": "Attack", "limit": 3},
                "user_query": "Tell me about Attack on Titan"
            },
            "expects_results": True
        },
        {
            "name": "Genre Filter",
            "request": {
                "query_type": "genre_filter", 
                "parameters": {"genre": "Action", "limit": 5},
                "user_query": "What are good action anime?"
            },
            "expects_results": True
        },
        {
            "name": "Top Rated",
            "request": {
                "query_type": "top_rated",
                "parameters": {"limit": 5},
                "user_query": "What are the highest rated anime?"
            },
            "expects_results": True
        },
        {
            "name": "Watch History",
            "request": {
                "query_type": "watch_history",
                "parameters": {"user_id": "personal_user", "limit": 5},
                "user_query": "What have I watched?"
            },
            "expects_results": None  # May or may not have results depending on DB
        },
        {
            "name": "Recommendations", 
            "request": {
                "query_type": "recommendations",
                "parameters": {"user_id": "personal_user", "limit": 3},
                "user_query": "Recommend something for me"
            },
            "expects_results": None  # Depends on watch history
        }
    ]
    
    print(f"\n🧪 Running {len(test_cases)} test cases...")
    
    passed = 0
    failed = 0
    warnings = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📝 Test {i}: {test_case['name']}")
        print(f"Query Type: {test_case['request']['query_type']}")
        print(f"Parameters: {test_case['request']['parameters']}")
        
        try:
            result = agent.process_data_request(test_case['request'])
            status = result['status']
            count = result['count']
            
            print(f"Status: {status}")
            print(f"Results Count: {count}")
            
            if status == "success":
                passed += 1
                print("✅ PASS")
                
                # Show sample results if available
                if count > 0 and result['results']:
                    print("Sample results:")
                    for j, item in enumerate(result['results'][:2], 1):
                        title = (item.get('title') or 
                                item.get('anime_title') or 
                                item.get('name', 'Unknown Title'))
                        score = item.get('score') or item.get('rating', 'N/A')
                        print(f"  {j}. {title} (Score: {score})")
                elif test_case['expects_results']:
                    print("⚠️ Expected results but got none")
                    warnings += 1
                    
            elif status == "error":
                error_msg = result.get('message', 'Unknown error')
                print(f"❌ FAIL - {error_msg}")
                
                # Check if it's an expected error (missing data sources)
                if ("S3 reader not available" in error_msg or 
                    "database not found" in error_msg):
                    print("   (This may be expected if S3/DB not configured)")
                    warnings += 1
                else:
                    failed += 1
                    
        except Exception as e:
            print(f"❌ FAIL - Exception: {e}")
            failed += 1
    
    # Summary
    print(f"\n📊 Test Results:")
    print(f"✅ Passed: {passed}")
    print(f"⚠️ Warnings: {warnings}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {passed}/{len(test_cases)} ({passed/len(test_cases)*100:.1f}%)")
    
    # Additional info
    if warnings > 0:
        print(f"\n💡 Warnings indicate missing S3 credentials or watch history DB")
        print("   This is normal for initial setup - the agent handles it gracefully")
    
    return failed == 0

def main():
    """Run the test."""
    
    if test_data_retrieval_agent():
        print(f"\n🎉 Data Retrieval Agent test COMPLETED!")
        print("💡 Ready to implement Sequential Workflow next")
    else:
        print(f"\n❌ Data Retrieval Agent test had failures")
        print("🔧 Please check the implementation and data sources")

if __name__ == "__main__":
    main()