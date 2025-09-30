#!/usr/bin/env python3
"""
Main entry point for the Anime Assistant
"""

import os
import sys
import logging

# Add the src directory to the path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from sequential_workflow import AnimeAssistantWorkflow

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def main():
    """Main anime assistant application"""
    print("🎌 Welcome to the Anime Assistant! 🎌")
    print("=" * 50)
    
    # Initialize the workflow
    try:
        workflow = AnimeAssistantWorkflow()
        print("✅ Anime Assistant initialized successfully!")
        
        print("\nType 'quit' or 'exit' to stop")
        print("=" * 50)
        
        while True:
            # Get user input
            user_query = input("\n🤔 What would you like to know about anime? ")
            
            # Check for exit commands
            if user_query.lower() in ['quit', 'exit', 'q']:
                print("\n👋 Thanks for using Anime Assistant! Goodbye!")
                break
            
            # Skip empty queries
            if not user_query.strip():
                continue
            
            print("\n🔍 Processing your request...")
            
            try:
                # Process the query through the workflow
                response = workflow.process_user_query(user_query)
                
                # Display the response
                print(f"\n📋 Status: {response.get('status', 'unknown')}")
                if response.get('status') == 'success':
                    print(f"📊 Results: {response.get('results_count', 0)} found")
                    print(f"\n{response.get('message', 'No message')}")
                else:
                    print(f"❌ Error: {response.get('error', 'Unknown error')}")
                    
            except Exception as e:
                logger.error(f"Error processing query: {e}")
                print(f"❌ Sorry, there was an error processing your request: {e}")
                
    except Exception as e:
        logger.error(f"Failed to initialize Anime Assistant: {e}")
        print(f"❌ Failed to start Anime Assistant: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())