"""
User Interface Agent for Anime Assistant Sequential Workflow.

This agent acts as the front-end that:
1. Receives natural language questions from users
2. Analyzes what data is needed to answer the query
3. Formats structured requests for the Data Retrieval Agent
4. Converts raw data results back into conversational responses

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
from typing import Dict, Any, Optional
from dataclasses import dataclass
from openai import OpenAI
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
    Front-end agent that handles natural language conversations.
    
    Responsibilities:
    - Process user queries and understand intent
    - Create structured data requests when needed
    - Format raw data into conversational responses
    - Handle direct conversational queries that don't need data
    """
    
    def __init__(self, openai_api_key: Optional[str] = None):
        """Initialize the User Interface Agent."""
        
        self.api_key = openai_api_key or os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            raise ValueError("OpenAI API key is required. Set OPENAI_API_KEY environment variable.")
        
        self.client = OpenAI(api_key=self.api_key)
        self.name = "UserInterfaceAgent"
        
        # System prompt that defines the agent's behavior
        self.system_prompt = """You are a friendly anime assistant that helps users discover and track anime.

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

        logger.info(f"User Interface Agent initialized with API key: {self.api_key[:20]}...")

    def process_user_query(self, user_query: str) -> Dict[str, Any]:
        """
        Process a user query and determine the appropriate response.
        
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
            
            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_query}
                ],
                temperature=0.7,  # More creative for conversation
                max_tokens=800
            )
            
            response_content = response.choices[0].message.content
            logger.debug(f"Raw AI response: {response_content}")
            
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
            logger.error(f"Error processing user query: {e}")
            return {
                "type": "error",
                "message": f"I encountered an error processing your request: {str(e)}"
            }

    def format_data_response(self, original_query: str, data_results: Dict[str, Any]) -> str:
        """
        Take raw data results and format them into a conversational response.
        
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

            response = self.client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": format_prompt}
                ],
                temperature=0.7,
                max_tokens=1200
            )
            
            formatted_response = response.choices[0].message.content
            logger.debug(f"Formatted response: {formatted_response[:100]}...")
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"Error formatting data response: {e}")
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

    def get_capabilities(self) -> Dict[str, Any]:
        """Return information about the agent's capabilities."""
        return {
            "name": self.name,
            "type": "user_interface",
            "model": "gpt-4o-mini",
            "temperature": 0.7,
            "capabilities": [
                "Natural language query processing",
                "Intent analysis and routing", 
                "Structured data request generation",
                "Conversational response formatting",
                "Direct query handling"
            ],
            "supported_query_types": [
                "search_title",
                "genre_filter", 
                "currently_airing",
                "top_rated",
                "watch_history",
                "recommendations"
            ]
        }


# Convenience function for testing
def create_user_interface_agent(api_key: Optional[str] = None) -> UserInterfaceAgent:
    """Create a User Interface Agent with optional API key."""
    return UserInterfaceAgent(api_key)


if __name__ == "__main__":
    """Simple test of the User Interface Agent."""
    
    logging.basicConfig(level=logging.INFO)
    
    # Test queries
    test_queries = [
        "Hello there!",                           # Should be direct response
        "What are some good action anime?",       # Should create data request
        "Tell me about Attack on Titan",         # Should create data request
        "What's the best anime from 2023?",      # Should create data request
        "What can you help me with?",            # Should be direct response
    ]
    
    try:
        # Create agent
        agent = create_user_interface_agent()
        print(f"✅ User Interface Agent created successfully")
        print(f"📋 Capabilities: {agent.get_capabilities()}")
        
        # Test queries
        print(f"\n🧪 Testing {len(test_queries)} queries...")
        
        for i, query in enumerate(test_queries, 1):
            print(f"\n{'='*50}")
            print(f"Test {i}: {query}")
            print('='*50)
            
            result = agent.process_user_query(query)
            
            if result["type"] == "direct_response":
                print("📝 Direct Response:")
                print(result["response"])
                
            elif result["type"] == "data_request":
                print("🔍 Data Request Created:")
                print(json.dumps(result["request"].to_dict(), indent=2))
                
            elif result["type"] == "error":
                print("❌ Error:")
                print(result["message"])
                
    except Exception as e:
        print(f"❌ Test failed: {e}")