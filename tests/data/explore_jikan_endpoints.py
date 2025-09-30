#!/usr/bin/env python3
"""
Jikan API Endpoints Documentation

This script demonstrates all the Jikan API endpoints we're using in the anime MVP
and shows the data structure returned by each endpoint.
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from ingestion.fetch_jikan import JikanAPIClient
from dotenv import load_dotenv

load_dotenv()


def pretty_print_data(title: str, data: Dict[Any, Any], max_items: int = 3):
    """Pretty print JSON data with truncation for readability."""
    print(f"\n{'='*60}")
    print(f"📊 {title}")
    print(f"{'='*60}")
    
    if not data:
        print("❌ No data returned")
        return
    
    # If data has a 'data' field that's a list, show first few items
    if isinstance(data.get('data'), list) and len(data['data']) > max_items:
        original_data = data['data']
        data['data'] = original_data[:max_items]
        print(f"📝 Showing first {max_items} of {len(original_data)} items")
    
    print(json.dumps(data, indent=2, ensure_ascii=False)[:2000] + "..." if len(str(data)) > 2000 else json.dumps(data, indent=2, ensure_ascii=False))


def demonstrate_jikan_endpoints():
    """Demonstrate all Jikan API endpoints we use."""
    client = JikanAPIClient()
    
    print("🎌 JIKAN API ENDPOINTS DEMONSTRATION")
    print("This shows all endpoints used in the anime MVP pipeline")
    
    # 1. ANIME DETAILS ENDPOINT
    print("\n" + "🔸" * 80)
    print("1️⃣ ANIME DETAILS: /anime/{id}")
    print("🔸" * 80)
    print("Purpose: Get basic anime metadata")
    print("Usage: For each anime we want detailed info about")
    
    anime_data = client.get_anime(1)  # Cowboy Bebop
    if anime_data:
        # Show key fields
        anime = anime_data.get('data', {})
        print(f"""
📋 KEY FIELDS AVAILABLE:
• mal_id: {anime.get('mal_id')}
• title: {anime.get('title')}
• title_english: {anime.get('title_english')}
• title_japanese: {anime.get('title_japanese')}
• type: {anime.get('type')} (TV, Movie, OVA, etc.)
• episodes: {anime.get('episodes')}
• status: {anime.get('status')}
• score: {anime.get('score')}
• rank: {anime.get('rank')}
• popularity: {anime.get('popularity')}
• members: {anime.get('members')}
• synopsis: {anime.get('synopsis', '')[:100]}...
• year: {anime.get('year')}
• season: {anime.get('season')}
        """)
    
    # 2. ANIME FULL ENDPOINT
    print("\n" + "🔸" * 80)
    print("2️⃣ ANIME FULL DETAILS: /anime/{id}/full")
    print("🔸" * 80)
    print("Purpose: Get complete anime data including relations, staff, etc.")
    print("Usage: Primary endpoint for detailed anime data")
    
    anime_full = client.get_anime_full(1)
    if anime_full:
        anime = anime_full.get('data', {})
        print(f"""
📋 ADDITIONAL FIELDS IN FULL VERSION:
• producers: {len(anime.get('producers', []))} producers
• licensors: {len(anime.get('licensors', []))} licensors  
• studios: {len(anime.get('studios', []))} studios
• genres: {len(anime.get('genres', []))} genres
• themes: {len(anime.get('themes', []))} themes
• demographics: {len(anime.get('demographics', []))} demographics
• relations: {len(anime.get('relations', []))} related anime
• external_links: {len(anime.get('external', []))} external links
        """)
        
        # Show sample genres
        if anime.get('genres'):
            print("🏷️ Sample Genres:")
            for genre in anime.get('genres', [])[:3]:
                print(f"  • {genre.get('name')} (ID: {genre.get('mal_id')})")
    
    # 3. ANIME STATISTICS ENDPOINT
    print("\n" + "🔸" * 80)
    print("3️⃣ ANIME STATISTICS: /anime/{id}/statistics")
    print("🔸" * 80)
    print("Purpose: Get viewing statistics (watching, completed, etc.)")
    print("Usage: For popularity analysis and recommendations")
    
    stats_data = client.get_anime_statistics(1)
    if stats_data:
        stats = stats_data.get('data', {})
        print(f"""
📊 VIEWING STATISTICS:
• watching: {stats.get('watching', 0):,} users currently watching
• completed: {stats.get('completed', 0):,} users completed
• on_hold: {stats.get('on_hold', 0):,} users put on hold
• dropped: {stats.get('dropped', 0):,} users dropped
• plan_to_watch: {stats.get('plan_to_watch', 0):,} users planning to watch
• total: {stats.get('total', 0):,} total user interactions
        """)
    
    # 4. TOP ANIME ENDPOINT
    print("\n" + "🔸" * 80)
    print("4️⃣ TOP ANIME: /top/anime")
    print("🔸" * 80)
    print("Purpose: Get ranked list of highest-rated anime")
    print("Usage: Seed data for popular anime (we fetch 5 pages = ~250 anime)")
    
    top_data = client.get_top_anime(page=1, limit=5)
    if top_data:
        print(f"""
📋 TOP ANIME STRUCTURE:
• data: List of anime objects
• pagination: Page info
        """)
        print("🏆 Sample Top Anime:")
        for i, anime in enumerate(top_data.get('data', [])[:3], 1):
            print(f"  {i}. {anime.get('title')} (Score: {anime.get('score')}, Rank: {anime.get('rank')})")
    
    # 5. SEASONAL ANIME ENDPOINT
    print("\n" + "🔸" * 80)
    print("5️⃣ SEASONAL ANIME: /seasons/{year}/{season}")
    print("🔸" * 80)
    print("Purpose: Get anime from specific seasons")
    print("Usage: Current + last 2 seasons (~400 anime total)")
    
    seasonal_data = client.get_seasonal_anime(2024, "fall", page=1)
    if seasonal_data:
        print(f"""
📋 SEASONAL ANIME STRUCTURE:
• data: List of seasonal anime
• season_name: {seasonal_data.get('data', [{}])[0].get('season') if seasonal_data.get('data') else 'N/A'}
• season_year: {seasonal_data.get('data', [{}])[0].get('year') if seasonal_data.get('data') else 'N/A'}
        """)
        print("🍂 Sample Fall 2024 Anime:")
        for anime in seasonal_data.get('data', [])[:3]:
            print(f"  • {anime.get('title')} ({anime.get('type')}, {anime.get('episodes')} eps)")
    
    # 6. ANIME RECOMMENDATIONS ENDPOINT
    print("\n" + "🔸" * 80)
    print("6️⃣ ANIME RECOMMENDATIONS: /anime/{id}/recommendations")
    print("🔸" * 80)
    print("Purpose: Get user-generated recommendations for an anime")
    print("Usage: Build recommendation graph (top 10 per anime)")
    
    recs_data = client.get_anime_recommendations(1)
    if recs_data:
        print(f"""
📋 RECOMMENDATIONS STRUCTURE:
• data: List of recommendation objects
• Each recommendation has:
  - entry: Target anime info
  - votes: Number of user votes
  - url: MAL recommendation page
        """)
        print("💡 Sample Recommendations for Cowboy Bebop:")
        for rec in recs_data.get('data', [])[:3]:
            entry = rec.get('entry', {})
            print(f"  • {entry.get('title')} ({rec.get('votes')} votes)")
    
    # 7. GENRES ENDPOINT
    print("\n" + "🔸" * 80)
    print("7️⃣ ANIME GENRES: /genres/anime")
    print("🔸" * 80)
    print("Purpose: Get master list of all anime genres")
    print("Usage: Static reference data (pulled once)")
    
    genres_data = client.get_anime_genres()
    if genres_data:
        print(f"""
📋 GENRES STRUCTURE:
• data: List of genre objects
• Total genres: {len(genres_data.get('data', []))}
        """)
        print("🏷️ Sample Genres:")
        for genre in genres_data.get('data', [])[:5]:
            print(f"  • {genre.get('name')} (ID: {genre.get('mal_id')})")
    
    # SUMMARY
    print("\n" + "🔸" * 80)
    print("📋 ENDPOINT SUMMARY")
    print("🔸" * 80)
    print("""
MVP COLLECTION STRATEGY:
┌─────────────────────────────────────────────────────────────┐
│ 1. GENRES (/genres/anime)                                   │
│    └── Pull once: ~50 genre definitions                    │
│                                                             │
│ 2. TOP ANIME (/top/anime)                                   │
│    └── 5 pages × 25 = ~250 top-rated anime IDs            │
│                                                             │
│ 3. SEASONAL (/seasons/{year}/{season})                      │
│    └── Current + 2 previous seasons = ~400 anime IDs       │
│                                                             │
│ 4. ANIME DETAILS (/anime/{id}/full)                         │
│    └── For each unique ID: full metadata                   │
│                                                             │
│ 5. STATISTICS (/anime/{id}/statistics)                      │
│    └── For each unique ID: viewing stats                   │
│                                                             │
│ 6. RECOMMENDATIONS (/anime/{id}/recommendations)            │
│    └── For each unique ID: top 10 recommendations          │
└─────────────────────────────────────────────────────────────┘

TOTAL DATA COLLECTED:
• ~500-600 unique anime with complete metadata
• ~5,000-6,000 recommendation relationships  
• Full viewing statistics for trend analysis
• Complete genre taxonomy
    """)


if __name__ == "__main__":
    demonstrate_jikan_endpoints()