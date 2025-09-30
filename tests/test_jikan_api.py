#!/usr/bin/env python3
"""
Test script to verify Jikan API endpoints are working.

This script tests all the endpoints we'll be using in the MVP dataset
without uploading to S3 - just to verify API connectivity and responses.
"""

import json
import time
from datetime import datetime
from src.ingestion.fetch_jikan import JikanAPIClient

def test_api_endpoint(client, endpoint_name, api_call, expected_keys=None):
    """Test a single API endpoint and return results."""
    print(f"\n{'='*50}")
    print(f"Testing: {endpoint_name}")
    print(f"{'='*50}")
    
    try:
        start_time = time.time()
        response = api_call()
        end_time = time.time()
        
        if response is None:
            print("❌ FAILED: No response received")
            return False
        
        response_time = end_time - start_time
        print(f"✅ SUCCESS: Response received in {response_time:.2f}s")
        
        # Check if response has expected structure
        if expected_keys:
            for key in expected_keys:
                if key not in response:
                    print(f"⚠️  WARNING: Expected key '{key}' not found in response")
                else:
                    print(f"✓ Key '{key}' found")
        
        # Show sample data structure
        if 'data' in response:
            data = response['data']
            if isinstance(data, list) and len(data) > 0:
                print(f"📊 Data type: List with {len(data)} items")
                print("📋 Sample item keys:", list(data[0].keys())[:10])
                if 'mal_id' in data[0]:
                    print(f"🎯 Sample anime ID: {data[0]['mal_id']}")
                if 'title' in data[0]:
                    print(f"📺 Sample title: {data[0]['title']}")
            elif isinstance(data, dict):
                print("📊 Data type: Dictionary")
                print("📋 Keys:", list(data.keys())[:10])
            else:
                print(f"📊 Data type: {type(data)}")
        
        # Show response size
        response_str = json.dumps(response)
        size_kb = len(response_str.encode('utf-8')) / 1024
        print(f"💾 Response size: {size_kb:.1f} KB")
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        return False

def main():
    """Test all Jikan API endpoints."""
    print("🚀 Starting Jikan API Test Suite")
    print(f"⏰ Test started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Initialize API client
    client = JikanAPIClient(rate_limit_delay=1.0)  # Be respectful with rate limiting
    
    # Track test results
    results = {}
    
    # Test 1: Genres endpoint
    results['genres'] = test_api_endpoint(
        client, 
        "Anime Genres", 
        lambda: client.get_anime_genres(),
        expected_keys=['data']
    )
    
    time.sleep(1)  # Rate limiting
    
    # Test 2: Top anime endpoint
    results['top_anime'] = test_api_endpoint(
        client,
        "Top Anime (Page 1)",
        lambda: client.get_top_anime(page=1, limit=10),
        expected_keys=['data', 'pagination']
    )
    
    time.sleep(1)
    
    # Test 3: Current seasonal anime
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
    
    results['seasonal'] = test_api_endpoint(
        client,
        f"Seasonal Anime ({current_season} {current_year})",
        lambda: client.get_seasonal_anime(current_year, current_season, page=1),
        expected_keys=['data', 'pagination']
    )
    
    time.sleep(1)
    
    # Test 4: Specific anime (using popular anime ID)
    test_anime_id = 1  # Cowboy Bebop - should always exist
    results['anime_details'] = test_api_endpoint(
        client,
        f"Anime Details (ID: {test_anime_id})",
        lambda: client.get_anime_full(test_anime_id),
        expected_keys=['data']
    )
    
    time.sleep(1)
    
    # Test 5: Anime statistics
    results['anime_stats'] = test_api_endpoint(
        client,
        f"Anime Statistics (ID: {test_anime_id})",
        lambda: client.get_anime_statistics(test_anime_id),
        expected_keys=['data']
    )
    
    time.sleep(1)
    
    # Test 6: Anime recommendations
    results['anime_recs'] = test_api_endpoint(
        client,
        f"Anime Recommendations (ID: {test_anime_id})",
        lambda: client.get_anime_recommendations(test_anime_id),
        expected_keys=['data']
    )
    
    # Print summary
    print(f"\n\n{'='*60}")
    print("🏁 TEST SUMMARY")
    print(f"{'='*60}")
    
    passed = sum(1 for result in results.values() if result)
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{test_name:20} : {status}")
    
    print(f"\n📊 Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Jikan API is working correctly.")
        print("\n💡 Next steps:")
        print("   1. Run the MVP dataset fetch: python src/ingestion/fetch_jikan.py --mvp")
        print("   2. Check S3 for uploaded files")
        print("   3. Process data with AWS Glue")
    else:
        print("⚠️  Some tests failed. Check API connectivity and endpoint availability.")
    
    print(f"\n⏰ Test completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    main()