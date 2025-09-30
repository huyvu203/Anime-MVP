#!/usr/bin/env python3
"""
Test User Interface Agent

Simple test to validate the User Interface Agent functionality.
Tests query processing, intent recognition, and data request generation.
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from agents.user_interface_agent import create_user_interface_agent
from dotenv import load_dotenv

load_dotenv()

def test_user_interface_agent():
    """Test the User Interface Agent with various query types."""
    
    print("🤖 Testing User Interface Agent")
    print("=" * 50)
    
    # Check environment
    api_key = os.getenv('OPENAI_API_KEY')
    if not api_key:
        print("❌ OPENAI_API_KEY not found in environment")
        return False
    
    print(f"✅ OpenAI API Key: {api_key[:20]}...")
    
    # Initialize agent
    try:
        agent = create_user_interface_agent()
        print("✅ User Interface Agent created successfully")
        
        # Show capabilities
        capabilities = agent.get_capabilities()
        print(f"✅ Agent Type: {capabilities['type']}")
        print(f"✅ Model: {capabilities['model']}")
        print(f"✅ Supported Query Types: {len(capabilities['supported_query_types'])}")
        
    except Exception as e:
        print(f"❌ Failed to create agent: {e}")
        return False
    
    # Test different types of queries
    test_cases = [
        {
            "query": "Hello there!",
            "expected_type": "direct_response",
            "description": "Simple greeting"
        },
        {
            "query": "What are some good action anime?", 
            "expected_type": "data_request",
            "description": "Genre-based query"
        },
        {
            "query": "Tell me about Attack on Titan",
            "expected_type": "data_request", 
            "description": "Title search query"
        },
        {
            "query": "What's the best anime from 2023?",
            "expected_type": "data_request",
            "description": "Year-filtered top rated query"
        },
        {
            "query": "What can you help me with?",
            "expected_type": "direct_response",
            "description": "Help/capability query"
        }
    ]
    
    print(f"\n🧪 Running {len(test_cases)} test cases...")
    
    passed = 0
    failed = 0
    
    for i, test_case in enumerate(test_cases, 1):
        print(f"\n📝 Test {i}: {test_case['description']}")
        print(f"Query: \"{test_case['query']}\"")
        print(f"Expected: {test_case['expected_type']}")
        
        try:
            result = agent.process_user_query(test_case['query'])
            actual_type = result['type']
            
            if actual_type == test_case['expected_type']:
                print(f"✅ PASS - Got {actual_type}")
                passed += 1
                
                # Show result details
                if actual_type == "direct_response":
                    response = result['response']
                    print(f"   Response: {response[:100]}...")
                    
                elif actual_type == "data_request":
                    request = result['request']
                    print(f"   Query Type: {request.query_type}")
                    print(f"   Parameters: {request.parameters}")
                    
            else:
                print(f"❌ FAIL - Expected {test_case['expected_type']}, got {actual_type}")
                failed += 1
                
        except Exception as e:
            print(f"❌ FAIL - Exception: {e}")
            failed += 1
    
    # Summary
    print(f"\n📊 Test Results:")
    print(f"✅ Passed: {passed}")
    print(f"❌ Failed: {failed}")
    print(f"📈 Success Rate: {passed}/{len(test_cases)} ({passed/len(test_cases)*100:.1f}%)")
    
    return failed == 0

def main():
    """Run the test."""
    
    if test_user_interface_agent():
        print(f"\n🎉 User Interface Agent test PASSED!")
        print("💡 Ready to implement Data Retrieval Agent next")
    else:
        print(f"\n❌ User Interface Agent test FAILED")
        print("🔧 Please check the implementation and try again")

if __name__ == "__main__":
    main()