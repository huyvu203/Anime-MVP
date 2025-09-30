#!/usr/bin/env python3
"""
Personal Anime Watch History Generator

Generates realistic personal anime watch history for a single-user system.
Optimized for the personal anime assistant with meaningful viewing patterns.
"""

import sqlite3
import random
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class PersonalAnimeHistory:
    """Generate realistic personal anime watch history."""
    
    def __init__(self, db_path: str = "data/user_history.db"):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.user_id = "personal_user"
        
        # Curated anime list with diverse genres and popularity levels
        self.anime_collection = [
            # Classic/Must-Watch
            {"id": 1, "title": "Cowboy Bebop", "episodes": 26, "genre": "Space Western", "score": 8.8, "type": "classic"},
            {"id": 121, "title": "Fullmetal Alchemist: Brotherhood", "episodes": 64, "genre": "Dark Fantasy", "score": 9.1, "type": "classic"},
            {"id": 199, "title": "Spirited Away", "episodes": 1, "genre": "Fantasy Film", "score": 9.3, "type": "classic"},
            {"id": 164, "title": "Princess Mononoke", "episodes": 1, "genre": "Fantasy Film", "score": 8.4, "type": "classic"},
            {"id": 32, "title": "Akira", "episodes": 1, "genre": "Cyberpunk Film", "score": 8.0, "type": "classic"},
            
            # Popular Long-Running
            {"id": 21, "title": "One Piece", "episodes": 1000, "genre": "Adventure Shounen", "score": 9.0, "type": "long_running"},
            {"id": 20, "title": "Naruto", "episodes": 220, "genre": "Ninja Shounen", "score": 8.4, "type": "long_running"},
            {"id": 1575, "title": "Code Geass R2", "episodes": 25, "genre": "Mecha Drama", "score": 8.9, "type": "popular"},
            {"id": 1535, "title": "Death Note", "episodes": 37, "genre": "Psychological Thriller", "score": 8.6, "type": "popular"},
            
            # Slice of Life / Romance
            {"id": 14813, "title": "Clannad: After Story", "episodes": 24, "genre": "Drama Romance", "score": 9.0, "type": "emotional"},
            {"id": 2904, "title": "Code Geass", "episodes": 25, "genre": "Mecha Drama", "score": 8.7, "type": "popular"},
            {"id": 431, "title": "Howl's Moving Castle", "episodes": 1, "genre": "Romance Film", "score": 8.2, "type": "film"},
            
            # Action/Adventure
            {"id": 19, "title": "Monster", "episodes": 74, "genre": "Psychological Thriller", "score": 9.0, "type": "mature"},
            {"id": 44, "title": "Rurouni Kenshin", "episodes": 94, "genre": "Historical Action", "score": 8.5, "type": "classic"},
            {"id": 33, "title": "Berserk", "episodes": 25, "genre": "Dark Fantasy", "score": 8.7, "type": "mature"},
            {"id": 245, "title": "Great Teacher Onizuka", "episodes": 43, "genre": "Comedy Drama", "score": 8.7, "type": "comedy"},
            
            # Modern Hits
            {"id": 16498, "title": "Attack on Titan", "episodes": 25, "genre": "Dark Action", "score": 8.7, "type": "modern"},
            {"id": 11061, "title": "Hunter x Hunter (2011)", "episodes": 148, "genre": "Adventure Shounen", "score": 9.0, "type": "modern"},
            {"id": 15417, "title": "Gintama", "episodes": 201, "genre": "Comedy Action", "score": 8.9, "type": "comedy"},
            {"id": 28851, "title": "Koe no Katachi", "episodes": 1, "genre": "Drama Film", "score": 8.9, "type": "film"},
            
            # Sports/Competitive
            {"id": 263, "title": "Hajime no Ippo", "episodes": 75, "genre": "Sports Boxing", "score": 8.8, "type": "sports"},
            {"id": 2921, "title": "Ashita no Joe 2", "episodes": 47, "genre": "Sports Boxing", "score": 8.8, "type": "sports"},
            
            # Sci-Fi/Mecha
            {"id": 30, "title": "Neon Genesis Evangelion", "episodes": 26, "genre": "Mecha Psychological", "score": 8.5, "type": "complex"},
            {"id": 22135, "title": "Ping Pong The Animation", "episodes": 11, "genre": "Sports Art", "score": 8.6, "type": "artistic"},
            
            # Lighter/Recent
            {"id": 32281, "title": "Kimi no Na wa", "episodes": 1, "genre": "Romance Film", "score": 8.4, "type": "film"},
            {"id": 28977, "title": "Gintama°", "episodes": 51, "genre": "Comedy Action", "score": 9.0, "type": "comedy"},
            {"id": 35180, "title": "3-gatsu no Lion", "episodes": 22, "genre": "Slice of Life", "score": 8.9, "type": "emotional"},
            {"id": 34096, "title": "Gintama.", "episodes": 12, "genre": "Comedy Action", "score": 8.9, "type": "comedy"},
        ]
        
        # Personal viewing preferences (configurable)
        self.preferences = {
            "favorite_genres": ["Dark Fantasy", "Psychological Thriller", "Adventure Shounen"],
            "completed_rate": 0.4,  # 40% of started anime are completed
            "drop_rate": 0.15,      # 15% are dropped
            "on_hold_rate": 0.1,    # 10% are on hold
            "watching_rate": 0.15,  # 15% currently watching
            "plan_to_watch_rate": 0.2, # 20% planned
            "high_score_threshold": 8.0,  # Shows above this get better ratings
        }
    
    def init_database(self):
        """Initialize the personal watch history database."""
        logger.info("🗄️  Initializing personal watch history database...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Drop existing table if it exists (fresh start)
        cursor.execute("DROP TABLE IF EXISTS user_watch_history")
        
        # Create the watch history table
        cursor.execute("""
            CREATE TABLE user_watch_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL DEFAULT 'personal_user',
                anime_id INTEGER NOT NULL,
                anime_title TEXT NOT NULL,
                watch_status TEXT NOT NULL,  -- completed, watching, plan_to_watch, dropped, on_hold
                rating INTEGER,  -- 1-10 personal rating
                episodes_watched INTEGER DEFAULT 0,
                total_episodes INTEGER,
                genre TEXT,
                anime_score REAL,  -- MAL/external score
                started_date TEXT,
                completed_date TEXT,
                notes TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, anime_id)
            )
        """)
        
        # Create indexes for performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_user_status ON user_watch_history(user_id, watch_status)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_rating ON user_watch_history(rating)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_genre ON user_watch_history(genre)")
        
        conn.commit()
        conn.close()
        logger.info("✅ Database initialized successfully")
    
    def generate_personal_history(self, target_entries: int = 35):
        """Generate realistic personal anime watch history."""
        logger.info(f"🎬 Generating personal watch history ({target_entries} entries)...")
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Select anime for personal library (mix of types)
        selected_anime = self._select_personal_anime(target_entries)
        
        # Generate watch history for each selected anime
        for anime in selected_anime:
            entry = self._create_watch_entry(anime)
            
            cursor.execute("""
                INSERT OR REPLACE INTO user_watch_history 
                (user_id, anime_id, anime_title, watch_status, rating, episodes_watched, 
                 total_episodes, genre, anime_score, started_date, completed_date, notes)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                self.user_id,
                anime["id"],
                anime["title"],
                entry["status"],
                entry["rating"],
                entry["episodes_watched"],
                anime["episodes"],
                anime["genre"],
                anime["score"],
                entry["started_date"],
                entry["completed_date"],
                entry["notes"]
            ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"✅ Generated {len(selected_anime)} personal watch entries")
        
    def _select_personal_anime(self, target_count: int) -> List[Dict]:
        """Select anime for personal library with realistic preferences."""
        
        # Categorize anime by type for balanced selection
        by_type = {}
        for anime in self.anime_collection:
            anime_type = anime["type"]
            if anime_type not in by_type:
                by_type[anime_type] = []
            by_type[anime_type].append(anime)
        
        # Selection strategy: ensure variety
        selected = []
        
        # Always include some classics (high completion rate)
        if "classic" in by_type and len(by_type["classic"]) > 0:
            count = min(5, len(by_type["classic"]))
            if count > 0:
                selected.extend(random.sample(by_type["classic"], count))
        
        # Add popular shows
        if "popular" in by_type and len(by_type["popular"]) > 0:
            count = min(4, len(by_type["popular"]))
            if count > 0:
                selected.extend(random.sample(by_type["popular"], count))
        
        # Add some modern hits
        if "modern" in by_type and len(by_type["modern"]) > 0:
            count = min(3, len(by_type["modern"]))
            if count > 0:
                selected.extend(random.sample(by_type["modern"], count))
        
        # Add films (quick watches)
        if "film" in by_type and len(by_type["film"]) > 0:
            count = min(4, len(by_type["film"]))
            if count > 0:
                selected.extend(random.sample(by_type["film"], count))
        
        # Fill remaining slots randomly from all types
        remaining_anime = [a for a in self.anime_collection if a not in selected]
        remaining_needed = target_count - len(selected)
        
        if remaining_needed > 0 and remaining_anime:
            selected.extend(random.sample(remaining_anime, min(remaining_needed, len(remaining_anime))))
        
        return selected[:target_count]
    
    def _create_watch_entry(self, anime: Dict) -> Dict:
        """Create a realistic watch entry for an anime."""
        
        # Determine status based on preferences and anime characteristics
        status = self._determine_status(anime)
        
        # Generate viewing details based on status
        entry = {
            "status": status,
            "rating": None,
            "episodes_watched": 0,
            "started_date": None,
            "completed_date": None,
            "notes": None
        }
        
        # Generate realistic dates and progress
        if status == "completed":
            entry.update(self._generate_completed_entry(anime))
        elif status == "watching":
            entry.update(self._generate_watching_entry(anime))
        elif status == "dropped":
            entry.update(self._generate_dropped_entry(anime))
        elif status == "on_hold":
            entry.update(self._generate_on_hold_entry(anime))
        elif status == "plan_to_watch":
            entry.update(self._generate_plan_to_watch_entry(anime))
        
        return entry
    
    def _determine_status(self, anime: Dict) -> str:
        """Determine watch status based on anime characteristics and preferences."""
        
        # Higher chance to complete highly rated anime
        if anime["score"] >= self.preferences["high_score_threshold"]:
            weights = {
                "completed": 0.6, "watching": 0.2, "plan_to_watch": 0.1,
                "on_hold": 0.05, "dropped": 0.05
            }
        # Lower rated or very long anime are more likely to be dropped/on hold
        elif anime["episodes"] > 100:
            weights = {
                "completed": 0.2, "watching": 0.3, "plan_to_watch": 0.2,
                "on_hold": 0.2, "dropped": 0.1
            }
        # Films are usually completed quickly
        elif anime["episodes"] == 1:
            weights = {
                "completed": 0.8, "watching": 0.05, "plan_to_watch": 0.1,
                "on_hold": 0.03, "dropped": 0.02
            }
        else:
            # Default distribution
            weights = {
                "completed": self.preferences["completed_rate"],
                "watching": self.preferences["watching_rate"],
                "plan_to_watch": self.preferences["plan_to_watch_rate"],
                "on_hold": self.preferences["on_hold_rate"],
                "dropped": self.preferences["drop_rate"]
            }
        
        return random.choices(list(weights.keys()), weights=list(weights.values()))[0]
    
    def _generate_completed_entry(self, anime: Dict) -> Dict:
        """Generate entry for completed anime."""
        completed_date = datetime.now() - timedelta(days=random.randint(7, 365))
        min_duration = max(1, anime["episodes"] // 10)
        max_duration = anime["episodes"] * 2
        watch_duration = random.randint(min_duration, max(min_duration, max_duration))
        started_date = completed_date - timedelta(days=watch_duration)
        
        # Rating based on anime quality with personal variance
        base_rating = anime["score"]
        personal_rating = max(1, min(10, int(base_rating + random.uniform(-1.5, 1.5))))
        
        notes_options = [
            None, "Great series!", "Really enjoyed this one", "Classic for a reason",
            "Exceeded expectations", "Beautiful animation", "Amazing story"
        ]
        
        return {
            "rating": personal_rating,
            "episodes_watched": anime["episodes"],
            "started_date": started_date.isoformat(),
            "completed_date": completed_date.isoformat(),
            "notes": random.choice(notes_options) if random.random() < 0.3 else None
        }
    
    def _generate_watching_entry(self, anime: Dict) -> Dict:
        """Generate entry for currently watching anime."""
        started_date = datetime.now() - timedelta(days=random.randint(1, 90))
        max_episodes = max(1, anime["episodes"] // 2)
        episodes_watched = random.randint(1, max(1, min(anime["episodes"], max_episodes)))
        
        # Might have a provisional rating
        rating = None
        if random.random() < 0.4:  # 40% chance of rating while watching
            rating = random.randint(6, 9)
        
        return {
            "rating": rating,
            "episodes_watched": episodes_watched,
            "started_date": started_date.isoformat(),
            "notes": random.choice([None, "Currently watching", "So far so good"]) if random.random() < 0.2 else None
        }
    
    def _generate_dropped_entry(self, anime: Dict) -> Dict:
        """Generate entry for dropped anime."""
        started_date = datetime.now() - timedelta(days=random.randint(14, 180))
        episodes_watched = random.randint(1, min(anime["episodes"], 8))  # Usually dropped early
        
        # Dropped shows get lower ratings
        rating = random.randint(3, 6)
        
        notes_options = ["Not for me", "Couldn't get into it", "Lost interest", None]
        
        return {
            "rating": rating,
            "episodes_watched": episodes_watched,
            "started_date": started_date.isoformat(),
            "notes": random.choice(notes_options)
        }
    
    def _generate_on_hold_entry(self, anime: Dict) -> Dict:
        """Generate entry for on-hold anime."""
        started_date = datetime.now() - timedelta(days=random.randint(30, 200))
        max_episodes = max(1, anime["episodes"] // 3)
        episodes_watched = random.randint(1, max(1, min(anime["episodes"], max_episodes)))
        
        notes_options = ["Will finish later", "Taking a break", "Got distracted", None]
        
        return {
            "episodes_watched": episodes_watched,
            "started_date": started_date.isoformat(),
            "notes": random.choice(notes_options) if random.random() < 0.4 else None
        }
    
    def _generate_plan_to_watch_entry(self, anime: Dict) -> Dict:
        """Generate entry for plan to watch anime."""
        notes_options = ["Want to watch", "Heard good things", "On my list", None]
        
        return {
            "notes": random.choice(notes_options) if random.random() < 0.3 else None
        }
    
    def print_summary(self):
        """Print a summary of the generated watch history."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Overall statistics
        cursor.execute("SELECT COUNT(*) FROM user_watch_history")
        total_entries = cursor.fetchone()[0]
        
        # Status breakdown
        cursor.execute("""
            SELECT watch_status, COUNT(*) 
            FROM user_watch_history 
            GROUP BY watch_status 
            ORDER BY COUNT(*) DESC
        """)
        status_counts = cursor.fetchall()
        
        # Rating statistics
        cursor.execute("""
            SELECT AVG(rating), MIN(rating), MAX(rating) 
            FROM user_watch_history 
            WHERE rating IS NOT NULL
        """)
        rating_stats = cursor.fetchone()
        
        # Genre breakdown
        cursor.execute("""
            SELECT genre, COUNT(*) 
            FROM user_watch_history 
            GROUP BY genre 
            ORDER BY COUNT(*) DESC 
            LIMIT 5
        """)
        top_genres = cursor.fetchall()
        
        print("\n" + "=" * 60)
        print("📊 PERSONAL ANIME WATCH HISTORY SUMMARY")
        print("=" * 60)
        print(f"Total Entries: {total_entries}")
        print("\nStatus Breakdown:")
        for status, count in status_counts:
            percentage = (count / total_entries) * 100
            print(f"  • {status.replace('_', ' ').title()}: {count} ({percentage:.1f}%)")
        
        if rating_stats[0]:
            print(f"\nRating Statistics:")
            print(f"  • Average Rating: {rating_stats[0]:.1f}/10")
            print(f"  • Rating Range: {rating_stats[1]} - {rating_stats[2]}")
        
        print(f"\nTop Genres:")
        for genre, count in top_genres:
            print(f"  • {genre}: {count}")
        
        print(f"\nDatabase Location: {self.db_path}")
        print("=" * 60)
        
        conn.close()
    
    def export_recommendations_sample(self):
        """Export sample data for recommendation testing."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Get completed high-rated anime for recommendations
        cursor.execute("""
            SELECT anime_title, rating, genre 
            FROM user_watch_history 
            WHERE watch_status = 'completed' AND rating >= 8
            ORDER BY rating DESC
        """)
        
        favorites = cursor.fetchall()
        
        sample_data = {
            "user_profile": "personal_user",
            "favorites": [{"title": title, "rating": rating, "genre": genre} 
                         for title, rating, genre in favorites[:10]],
            "generated_at": datetime.now().isoformat()
        }
        
        sample_path = self.db_path.parent / "sample_preferences.json"
        with open(sample_path, 'w') as f:
            json.dump(sample_data, f, indent=2)
        
        logger.info(f"📄 Sample preferences exported to: {sample_path}")
        conn.close()


def main():
    """Main function to generate personal anime watch history."""
    print("🎌 Personal Anime Watch History Generator")
    print("=" * 50)
    
    generator = PersonalAnimeHistory()
    
    try:
        # Initialize database
        generator.init_database()
        
        # Generate personal watch history
        generator.generate_personal_history(target_entries=35)
        
        # Show summary
        generator.print_summary()
        
        # Export sample data
        generator.export_recommendations_sample()
        
        print("\n🎉 Personal watch history generation completed!")
        print("Your anime assistant now has realistic personal viewing data!")
        
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())