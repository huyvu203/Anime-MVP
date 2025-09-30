#!/usr/bin/env python3
"""
Streamlit Chat Interface for Anime Assistant
"""

import os
import sys
import logging
import streamlit as st
from datetime import datetime

# Add the src and tests directories to the path to import modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
sys.path.append(os.path.join(os.path.dirname(__file__), 'tests', 'integration'))

from sequential_workflow import AnimeAssistantWorkflow

# Configure page
st.set_page_config(
    page_title="Anime Assistant",
    page_icon="🎌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def initialize_workflow():
    """Initialize the anime assistant workflow"""
    if 'workflow' not in st.session_state:
        try:
            with st.spinner("🚀 Initializing Anime Assistant..."):
                st.session_state.workflow = AnimeAssistantWorkflow()
            st.success("✅ Anime Assistant ready!")
            return True
        except Exception as e:
            st.error(f"❌ Failed to initialize Anime Assistant: {e}")
            logger.error(f"Failed to initialize workflow: {e}")
            return False
    return True

def initialize_chat_history():
    """Initialize chat history in session state"""
    if 'messages' not in st.session_state:
        st.session_state.messages = [
            {
                "role": "assistant",
                "content": "🎌 Welcome to the Anime Assistant! I can help you discover anime based on your preferences. Ask me about top-rated anime, specific genres, currently airing shows, or search for specific titles!",
                "timestamp": datetime.now()
            }
        ]

def display_chat_message(message):
    """Display a chat message with proper styling"""
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "timestamp" in message:
            st.caption(f"⏰ {message['timestamp'].strftime('%H:%M:%S')}")

def process_user_query(query):
    """Process user query through the workflow"""
    try:
        with st.spinner("🔍 Processing your request..."):
            response = st.session_state.workflow.process_user_query(query)
        
        # Format the response
        if response.get('status') == 'success':
            result_count = response.get('results_count', 0)
            message_content = response.get('message', 'No message available')
            
            # Add result summary
            if result_count > 0:
                success_msg = f"📊 **Found {result_count} results**\n\n{message_content}"
            else:
                success_msg = message_content
                
            return success_msg
        else:
            error_msg = response.get('error', 'Unknown error occurred')
            return f"❌ **Error**: {error_msg}"
            
    except Exception as e:
        logger.error(f"Error processing query: {e}")
        return f"❌ **Sorry, there was an error processing your request**: {e}"

def main():
    """Main Streamlit app"""
    
    # App title and description
    st.title("🎌 Anime Assistant")
    st.markdown("*Your AI-powered anime discovery companion*")
    
    # Sidebar with information
    with st.sidebar:
        st.header("🤖 About")
        st.markdown("""
        This Anime Assistant uses:
        - **GPT-4o-mini** for natural language processing
        - **AWS Athena** for querying anime data  
        - **AutoGen** for agent coordination
        
        ### 💡 Example Queries:
        - "What are the top rated anime?"
        - "Show me action anime"
        - "Find anime about pirates"
        - "What's currently airing?"
        - "Recommend something with high scores"
        """)
        
        # System status
        st.header("📊 Status")
        if 'workflow' in st.session_state:
            st.success("🟢 Connected")
            st.info("💾 Using Athena database")
        else:
            st.warning("🟡 Initializing...")
    
    # Initialize components
    if not initialize_workflow():
        st.stop()
    
    initialize_chat_history()
    
    # Display chat history
    st.subheader("💬 Chat")
    
    # Create a container for messages
    chat_container = st.container()
    
    with chat_container:
        for message in st.session_state.messages:
            display_chat_message(message)
    
    # Chat input
    if prompt := st.chat_input("Ask me anything about anime..."):
        # Add user message to history
        user_message = {
            "role": "user", 
            "content": prompt,
            "timestamp": datetime.now()
        }
        st.session_state.messages.append(user_message)
        
        # Display user message immediately
        with st.chat_message("user"):
            st.markdown(prompt)
            st.caption(f"⏰ {user_message['timestamp'].strftime('%H:%M:%S')}")
        
        # Process query and get response
        response_content = process_user_query(prompt)
        
        # Add assistant response to history
        assistant_message = {
            "role": "assistant",
            "content": response_content,
            "timestamp": datetime.now()
        }
        st.session_state.messages.append(assistant_message)
        
        # Display assistant response
        with st.chat_message("assistant"):
            st.markdown(response_content)
            st.caption(f"⏰ {assistant_message['timestamp'].strftime('%H:%M:%S')}")
        
        # Auto-scroll to bottom (rerun to show new messages)
        st.rerun()
    
    # Footer
    st.markdown("---")
    st.markdown("🚀 *Powered by AutoGen, GPT-4o-mini, and AWS Athena*")

if __name__ == "__main__":
    main()