"""
Modern AutoGen AgentChat 0.7+ User Interface Agent for Anime Assistant.

This agent acts as the front-end that:
1. Receives natural language questions from users
2. Analyzes what data is needed to answer the query
3. Formats structured requests for the Data Retrieval Agent
4. Converts raw data results back into conversational responses

Built using AutoGen AgentChat 0.7+ with async/await patterns and modern Python.

Example workflow:
User: "What's the best anime from 2023?"
↓
UI Agent: Creates structured request → {"query_type": "top_rated", "year": 2023}
↓
Data Agent: Executes query against S3/DB
↓ 
UI Agent: Formats results → "Here are the top 5 anime from 2023..."
"""

import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional, List, Union, Sequence
from dataclasses import dataclass

# Modern AutoGen imports
try:
    from autogen_agentchat.agents import AssistantAgent
    from autogen_agentchat.messages import TextMessage, ChatMessage
    from autogen_ext.models.openai import OpenAIChatCompletionClient
    AUTOGEN_AVAILABLE = True
except ImportError:
    # Fallback for systems without AutoGen
    AssistantAgent = object
    TextMessage = object
    ChatMessage = object
    OpenAIChatCompletionClient = object
    AUTOGEN_AVAILABLE = False
    print("⚠️ Modern AutoGen not available. Install with:")
    print("   poetry add autogen-agentchat 'autogen-ext[openai]'")

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class DataRequest:
    """Represents a structured data request to be sent to the Data Retrieval Agent."""
    query_type: str  # search_title, genre_filter, currently_airing, top_rated, watch_history, recommendations
    parameters: Dict[str, Any]
    original_query: str
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary format."""
        return {
            "action": "data_request",
            "query_type": self.query_type,
            "parameters": self.parameters,
            "user_query": self.original_query
        }


class UserInterfaceAgent:
    """
    Modern AutoGen AgentChat 0.7+ front-end agent for anime conversations.
    
    Built using AssistantAgent with OpenAI ChatCompletion client.
    Supports async/await patterns and modern Python type hints.
    
    Responsibilities:
    - Process user queries and understand intent
    - Create structured data requests when needed
    - Format raw data into conversational responses
    - Handle direct conversational queries that don't need data
    """
    
    def __init__(self, openai_api_key: Optional[str] = None, **kwargs):
        """Initialize the Modern AutoGen User Interface Agent."""
        
        if not AUTOGEN_AVAILABLE:
            raise ImportError("Modern AutoGen is required but not installed. Install with:\n"
                            "  poetry add autogen-agentchat 'autogen-ext[openai]'")
        
        self.api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable.")
        
        # Create OpenAI client for AutoGen AgentChat 0.7+
        self.model_client = OpenAIChatCompletionClient(
            model="gpt-4o-mini",
            api_key=self.api_key,
            **kwargs
        )
        
        # System message that defines the agent's behavior
        self.system_message = """You are a friendly anime assistant that helps users discover and track anime.

Your role in the multi-agent workflow:
1. Receive natural language queries from users
2. Analyze what data is needed to answer the query
3. Create structured requests for the Data Retrieval Agent when data is needed
4. Format raw data results back into conversational responses

Your personality:
- Enthusiastic about anime and helpful
- Conversational and engaging
- Provide clear, organized responses
- Ask follow-up questions when appropriate

## Query Analysis and Routing

When you receive a user query, determine if it needs data retrieval or if you can respond directly.

### Queries that NEED data retrieval:
- Searching for specific anime: "Tell me about Naruto", "What's Attack on Titan about?"
- Genre-based queries: "What are good action anime?", "Show me romance anime"
- Year/season queries: "Best anime from 2023", "What's airing this season?"
- Rating-based: "Top rated anime", "Highest scoring shows"
- Personal history: "What am I watching?", "My completed list"
- Recommendations: "Recommend something for me", "Based on my history"

### Queries you can answer DIRECTLY (no data needed):
- Greetings: "Hello", "Hi there"
- General anime questions: "What is anime?", "How do ratings work?"
- Help requests: "What can you help me with?"
- Casual conversation: "How are you?"

## Creating Data Requests

When data is needed, create a JSON request in this EXACT format:

```json
{
    "action": "data_request",
    "query_type": "search_title|genre_filter|currently_airing|top_rated|watch_history|recommendations",
    "parameters": {
        "title": "search term (for search_title)",
        "genre": "genre name (for genre_filter)",
        "limit": 10,
        "year": "YYYY (optional year filter)",
        "user_id": "personal_user",
        "status": "watching|completed|plan_to_watch (for watch_history)"
    },
    "user_query": "original user question"
}
```

## Query Type Mapping Examples:
- "Tell me about Naruto" → "search_title" with title="Naruto"
- "Action anime" → "genre_filter" with genre="Action"
- "What's airing now?" → "currently_airing"
- "Best anime from 2023" → "top_rated" with year="2023"
- "What am I watching?" → "watch_history" with status="watching"
- "Recommend something" → "recommendations"

## Response Formatting

When formatting data results, make responses:
- Conversational and friendly
- Well-structured with clear headings
- Include relevant details (ratings, year, episodes, genres)
- Use markdown for better readability
- Suggest follow-up questions

Always be helpful and enthusiastic about anime!"""

        # Initialize Modern AutoGen AssistantAgent
        try:
            self.agent = AssistantAgent(
                name="UserInterfaceAgent",
                model_client=self.model_client,
                system_message=self.system_message
            )
            
            logger.info(f"✅ Modern AutoGen User Interface Agent initialized successfully")
            logger.info(f"🔑 Using API key: {self.api_key[:20]}...")
            logger.info(f"🤖 Model: gpt-4o-mini via OpenAIChatCompletionClient")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize Modern AutoGen agent: {e}")
            raise

    async def process_user_query(self, user_query: str) -> Dict[str, Any]:
        """
        Process a user query and determine the appropriate response using Modern AutoGen.
        
        Args:
            user_query: The user's natural language question
            
        Returns:
            Dictionary with either:
            - {"type": "data_request", "request": DataRequest} - needs data retrieval
            - {"type": "direct_response", "response": str} - can answer directly
            - {"type": "error", "message": str} - error occurred
        """
        try:
            logger.info(f"Processing user query: {user_query}")
            
            # Create message for Modern AutoGen
            message = TextMessage(content=user_query, source="user")
            
            # Use Modern AutoGen's async on_messages method
            response = await self.agent.on_messages(
                messages=[message],
                cancellation_token=None
            )
            
            # Extract response content
            if hasattr(response, 'content'):
                response_content = response.content
            elif isinstance(response, str):
                response_content = response
            else:
                response_content = str(response)
            
            logger.debug(f"Raw Modern AutoGen response: {response_content}")
            
            # Check if this is a data request (contains JSON)
            if self._contains_data_request(response_content):
                data_request = self._extract_data_request(response_content, user_query)
                return {
                    "type": "data_request",
                    "request": data_request
                }
            else:
                # Direct response - no data needed
                return {
                    "type": "direct_response", 
                    "response": response_content
                }
                
        except Exception as e:
            logger.error(f"Error processing user query with Modern AutoGen: {e}")
            return {
                "type": "error",
                "message": f"I encountered an error processing your request: {str(e)}"
            }

    def process_user_query_sync(self, user_query: str) -> Dict[str, Any]:
        """
        Synchronous wrapper for async process_user_query method.
        
        Args:
            user_query: The user's natural language question
            
        Returns:
            Same as async version
        """
        try:
            # Run the async method in a new event loop
            return asyncio.run(self.process_user_query(user_query))
        except Exception as e:
            logger.error(f"Error in sync wrapper: {e}")
            return {
                "type": "error",
                "message": f"I encountered an error processing your request: {str(e)}"
            }

    async def format_data_response(self, original_query: str, data_results: Dict[str, Any]) -> str:
        """
        Take raw data results and format them into a conversational response using Modern AutoGen.
        
        Args:
            original_query: The user's original question
            data_results: Raw data from the Data Retrieval Agent
            
        Returns:
            Formatted conversational response
        """
        try:
            logger.info(f"Formatting data response for query: {original_query}")
            
            # Create a prompt for formatting the response
            format_prompt = f"""The user asked: "{original_query}"

The Data Retrieval Agent returned this data:
```json
{json.dumps(data_results, indent=2)}
```

Your task: Convert this raw data into a friendly, conversational response.

Guidelines:
- Be enthusiastic and conversational
- Format anime lists clearly with titles, ratings, years, episodes
- Include relevant details like genres, status, studios
- If no results found, suggest alternatives or ask clarifying questions
- Use markdown formatting for better readability
- Keep the response engaging and helpful
- Suggest follow-up queries when appropriate

Make it feel like you're talking to a fellow anime fan!"""

            # Create message for Modern AutoGen
            message = TextMessage(content=format_prompt, source="user")
            
            # Use Modern AutoGen's async on_messages method
            response = await self.agent.on_messages(
                messages=[message],
                cancellation_token=None
            )
            
            # Extract response content
            if hasattr(response, 'content'):
                formatted_response = response.content
            elif isinstance(response, str):
                formatted_response = response
            else:
                formatted_response = str(response)
            
            logger.debug(f"Formatted response: {formatted_response[:100]}...")
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error formatting data response with Modern AutoGen: {e}")
            return f"I got the data but had trouble formatting it nicely. Here's what I found: {str(data_results)}"

    def format_data_response_sync(self, original_query: str, data_results: Dict[str, Any]) -> str:
        """
        Synchronous wrapper for async format_data_response method.
        
        Args:
            original_query: The user's original question
            data_results: Raw data from the Data Retrieval Agent
            
        Returns:
            Same as async version
        """
        try:
            return asyncio.run(self.format_data_response(original_query, data_results))
        except Exception as e:
            logger.error(f"Error in sync format wrapper: {e}")
            return f"I got the data but had trouble formatting it nicely. Here's what I found: {str(data_results)}"

    def _contains_data_request(self, response: str) -> bool:
        """Check if the response contains a data request JSON."""
        return "```json" in response and "action" in response and "data_request" in response

    def _extract_data_request(self, response: str, original_query: str) -> DataRequest:
        """Extract and parse the data request from the AI response."""
        try:
            # Find the JSON block
            start_idx = response.find('```json')
            end_idx = response.find('```', start_idx + 7)
            
            if start_idx == -1 or end_idx == -1:
                raise ValueError("Could not find JSON block in response")
            
            json_content = response[start_idx + 7:end_idx].strip()
            request_data = json.loads(json_content)
            
            # Validate the request format
            if request_data.get("action") != "data_request":
                raise ValueError("Invalid request format - missing 'data_request' action")
            
            return DataRequest(
                query_type=request_data.get("query_type"),
                parameters=request_data.get("parameters", {}),
                original_query=original_query
            )
            
        except Exception as e:
            logger.error(f"Error extracting data request: {e}")
            # Fallback - create a generic search request
            return DataRequest(
                query_type="search_title",
                parameters={"title": original_query, "limit": 10},
                original_query=original_query
            )

    async def initiate_chat_with_data_agent(self, data_agent, user_query: str) -> Dict[str, Any]:
        """
        Initiate a conversation with the Data Retrieval Agent using Modern AutoGen's async interface.
        
        Args:
            data_agent: The Data Retrieval Agent instance (must be Modern AutoGen compatible)
            user_query: The user's original query
            
        Returns:
            Chat result from Modern AutoGen
        """
        try:
            logger.info(f"Initiating Modern AutoGen chat for query: {user_query}")
            
            # Create initial message
            message = TextMessage(content=user_query, source="user")
            
            # Start async conversation with data agent
            # Note: This requires the data_agent to also be Modern AutoGen compatible
            if hasattr(data_agent, 'on_messages'):
                response = await data_agent.on_messages(
                    messages=[message],
                    cancellation_token=None
                )
                return {"response": response, "success": True}
            else:
                logger.warning("Data agent is not Modern AutoGen compatible")
                return {"error": "Data agent not compatible with Modern AutoGen", "success": False}
            
        except Exception as e:
            logger.error(f"Error in Modern AutoGen chat: {e}")
            return {"error": str(e), "success": False}

    def initiate_chat_with_data_agent_sync(self, data_agent, user_query: str) -> Dict[str, Any]:
        """
        Synchronous wrapper for async initiate_chat_with_data_agent method.
        
        Args:
            data_agent: The Data Retrieval Agent instance
            user_query: The user's original query
            
        Returns:
            Same as async version
        """
        try:
            return asyncio.run(self.initiate_chat_with_data_agent(data_agent, user_query))
        except Exception as e:
            logger.error(f"Error in sync chat wrapper: {e}")
            return {"error": str(e), "success": False}
    
    def get_capabilities(self) -> Dict[str, Any]:
        """Return information about the agent's capabilities."""
        return {
            "name": "UserInterfaceAgent",
            "type": "user_interface",
            "framework": "AutoGen AgentChat 0.7+ AssistantAgent",
            "model": "gpt-4o-mini",
            "model_client": "OpenAIChatCompletionClient",
            "async_support": True,
            "capabilities": [
                "Natural language query processing",
                "Intent analysis and routing", 
                "Structured data request generation",
                "Conversational response formatting",
                "Direct query handling",
                "Async/await pattern support",
                "Modern AutoGen multi-agent conversations"
            ],
            "supported_query_types": [
                "search_title",
                "genre_filter", 
                "currently_airing",
                "top_rated",
                "watch_history",
                "recommendations"
            ],
            "modern_autogen_features": [
                "AssistantAgent with OpenAI client",
                "Async message handling with on_messages",
                "TextMessage and ChatMessage support",
                "Modern Python async/await patterns",
                "Built-in cancellation token support",
                "Enhanced error handling and logging"
            ],
            "methods": {
                "async": [
                    "process_user_query",
                    "format_data_response", 
                    "initiate_chat_with_data_agent"
                ],
                "sync": [
                    "process_user_query_sync",
                    "format_data_response_sync",
                    "initiate_chat_with_data_agent_sync"
                ]
            }
        }

    @property
    def name(self) -> str:
        """Agent name property."""
        return "UserInterfaceAgent"


# Convenience functions for different use cases
def create_user_interface_agent(api_key: Optional[str] = None, **kwargs) -> UserInterfaceAgent:
    """Create a Modern AutoGen User Interface Agent with optional API key."""
    if not AUTOGEN_AVAILABLE:
        raise ImportError("Modern AutoGen is required but not installed. Install with:\n"
                         "  poetry add autogen-agentchat 'autogen-ext[openai]'")
    return UserInterfaceAgent(openai_api_key=api_key, **kwargs)

def create_standalone_ui_agent(api_key: Optional[str] = None, **kwargs) -> UserInterfaceAgent:
    """Create a standalone UI agent for testing without other agents."""
    return create_user_interface_agent(api_key=api_key, **kwargs)

async def create_async_ui_agent(api_key: Optional[str] = None, **kwargs) -> UserInterfaceAgent:
    """Create and initialize an async-ready UI agent."""
    agent = create_user_interface_agent(api_key=api_key, **kwargs)
    # Agent is ready for async operations immediately
    return agent


async def test_async_agent():
    """Async test function for Modern AutoGen User Interface Agent."""
    
    # Test queries
    test_queries = [
        "Hello there!",                           # Should be direct response
        "What are some good action anime?",       # Should create data request
        "Tell me about Attack on Titan",         # Should create data request
        "What's the best anime from 2023?",      # Should create data request
        "What can you help me with?",            # Should be direct response
    ]
    
    try:
        # Check Modern AutoGen availability
        if not AUTOGEN_AVAILABLE:
            print("❌ Modern AutoGen not available. Install with:")
            print("   poetry add autogen-agentchat 'autogen-ext[openai]'")
            return
        
        # Create agent
        agent = await create_async_ui_agent()
        print(f"✅ Modern AutoGen User Interface Agent created successfully")
        print(f"📋 Agent Name: {agent.name}")
        print(f"🛠️ Capabilities: {json.dumps(agent.get_capabilities(), indent=2)}")
        
        # Test queries with async methods
        print(f"\n🧪 Testing {len(test_queries)} queries with Modern AutoGen (Async)...")
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n{'='*70}")
            print(f"Modern AutoGen Async Test {i}: {query}")
            print('='*70)
            
            # Test async method
            result = await agent.process_user_query(query)
            
            if result["type"] == "direct_response":
                print("📝 Modern AutoGen Async Direct Response:")
                print(result["response"])
                
            elif result["type"] == "data_request":
                print("🔍 Modern AutoGen Async Data Request Created:")
                print(json.dumps(result["request"].to_dict(), indent=2))
                
            elif result["type"] == "error":
                print("❌ Modern AutoGen Async Error:")
                print(result["message"])
        
        print(f"\n✅ Modern AutoGen async testing completed!")
        
        # Test sync wrappers
        print(f"\n🔄 Testing sync wrapper methods...")
        test_query = "What are some good romance anime?"
        print(f"Sync Test Query: {test_query}")
        
        sync_result = agent.process_user_query_sync(test_query)
        print(f"Sync Result Type: {sync_result['type']}")
        if sync_result["type"] == "data_request":
            print("🔍 Sync Data Request:")
            print(json.dumps(sync_result["request"].to_dict(), indent=2))
        
        print(f"\n✅ Modern AutoGen sync wrapper testing completed!")
        
    except Exception as e:
        print(f"❌ Async test failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    """Test the Modern AutoGen User Interface Agent."""
    
    logging.basicConfig(level=logging.INFO)
    
    try:
        # Run async tests
        print("🚀 Starting Modern AutoGen AgentChat 0.7+ User Interface Agent Tests")
        print("="*80)
        
        # Run the async test
        asyncio.run(test_async_agent())
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        print("💡 Install Modern AutoGen with:")
        print("   poetry add autogen-agentchat 'autogen-ext[openai]'")
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()