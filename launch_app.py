#!/usr/bin/env python3
"""
Launch script for the Personal Anime Assistant Streamlit app.
"""

import subprocess
import sys
import os
from pathlib import Path

def check_requirements():
    """Check if required files exist."""
    missing = []
    
    if not Path('.env').exists():
        missing.append(".env file (with OPENAI_API_KEY)")
    
    if not Path('data/user_history.db').exists():
        missing.append("Personal watch history database")
    
    return missing

def main():
    """Launch the Streamlit app."""
    print("🎌 Personal Anime Assistant")
    print("=" * 40)
    
    # Check requirements
    missing = check_requirements()
    if missing:
        print("❌ Missing requirements:")
        for item in missing:
            print(f"   - {item}")
        
        print("\n📝 Setup instructions:")
        if ".env file" in str(missing):
            print("   1. Create .env file and add: OPENAI_API_KEY=your_key")
        if "watch history" in str(missing):
            print("   2. Generate watch history: poetry run python personal_watch_history.py")
        
        return 1
    
    print("✅ All requirements met!")
    print("🚀 Launching Streamlit app...")
    print("   URL: http://localhost:8501")
    print("   Press Ctrl+C to stop")
    print()
    
    try:
        # Launch streamlit
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "app.py",
            "--server.address", "localhost",
            "--server.port", "8501",
            "--browser.gatherUsageStats", "false"
        ], check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Anime Assistant stopped!")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"❌ Error launching app: {e}")
        return 1

if __name__ == "__main__":
    exit(main())