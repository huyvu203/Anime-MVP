#!/usr/bin/env python3
"""
Test script for the anime assistant agent orchestration system.
"""

import os
import sys
import logging
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).parent / "src"))

from dotenv import load_dotenv
from src.agents import create_anime_assistant

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def test_orchestration():
    """Test the agent orchestration system."""
    
    print("=" * 60)
    print("ANIME ASSISTANT ORCHESTRATION TEST")
    print("=" * 60)
    
    # Load environment variables
    load_dotenv()
    
    # Check for OpenAI API key
    if not os.getenv('OPENAI_API_KEY'):
        print("❌ ERROR: OPENAI_API_KEY not found in environment")
        print("Please add your OpenAI API key to .env file:")
        print("OPENAI_API_KEY=your_api_key_here")
        return False
    
    try:
        # Create the assistant orchestrator
        print("🤖 Initializing Anime Assistant Orchestrator...")
        assistant = create_anime_assistant()
        
        # Show agent status
        status = assistant.get_agent_status()
        print("✅ Orchestrator initialized successfully!")
        print(f"   - UI Agent: {status['ui_agent']['name']}")
        print(f"   - Data Agent: {status['data_agent']['name']}")
        print(f"   - Available functions: {status['data_agent']['available_functions']}")
        
        print("\n" + "=" * 60)
        print("TESTING AGENT COMMUNICATION")
        print("=" * 60)
        
        # Test queries
        test_queries = [
            "What are some popular action anime?",
            "Show me currently airing anime",
            "What data sources do you have available?",
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n📋 Test Query {i}: {query}")
            print("-" * 50)
            
            try:
                response = assistant.process_user_query(query)
                print(f"🤖 Response: {response}")
                
            except Exception as e:
                print(f"❌ Error processing query: {e}")
            
            # Reset conversation for next test
            assistant.reset_conversation()
        
        print("\n" + "=" * 60)
        print("✅ ORCHESTRATION TEST COMPLETED")
        print("=" * 60)
        return True
        
    except Exception as e:
        print(f"❌ ERROR: Failed to initialize orchestrator: {e}")
        return False

def interactive_mode():
    """Run in interactive mode for manual testing."""
    
    print("\n" + "=" * 60)
    print("INTERACTIVE MODE")
    print("=" * 60)
    print("Type 'quit' to exit")
    
    try:
        assistant = create_anime_assistant()
        print("🤖 Assistant ready! Ask me about anime!")
        
        while True:
            user_input = input("\n👤 You: ").strip()
            
            if user_input.lower() in ['quit', 'exit', 'q']:
                print("👋 Goodbye!")
                break
            
            if not user_input:
                continue
            
            try:
                response = assistant.process_user_query(user_input)
                print(f"🤖 Assistant: {response}")
                
            except Exception as e:
                print(f"❌ Error: {e}")
    
    except Exception as e:
        print(f"❌ Failed to start interactive mode: {e}")

if __name__ == "__main__":
    success = test_orchestration()
    
    if success:
        # Ask if user wants interactive mode
        while True:
            choice = input("\n🤔 Would you like to try interactive mode? (y/n): ").strip().lower()
            if choice in ['y', 'yes']:
                interactive_mode()
                break
            elif choice in ['n', 'no']:
                print("👋 Thanks for testing!")
                break
            else:
                print("Please enter 'y' or 'n'")