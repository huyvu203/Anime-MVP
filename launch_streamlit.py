#!/usr/bin/env python3
"""
Launch script for Anime Assistant Streamlit app
"""

import subprocess
import sys
import os

def main():
    """Launch the Streamlit app"""
    
    # Ensure we're in the right directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    os.chdir(script_dir)
    
    print("🎌 Starting Anime Assistant Web Interface...")
    print("=" * 50)
    print("📱 The app will open in your browser automatically")
    print("🔗 If it doesn't open, go to: http://localhost:8501")
    print("⚡ Use Ctrl+C to stop the server")
    print("=" * 50)
    
    try:
        # Launch Streamlit app
        subprocess.run([
            sys.executable, "-m", "streamlit", "run", "streamlit_app.py",
            "--server.port=8501",
            "--server.address=localhost",
            "--browser.gatherUsageStats=false"
        ], check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Shutting down Anime Assistant...")
    except subprocess.CalledProcessError as e:
        print(f"❌ Error launching Streamlit app: {e}")
        return 1
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())