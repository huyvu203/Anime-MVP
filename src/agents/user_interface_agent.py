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
from typing import Dict, Any, Optional
from dataclasses import dataclass
from openai import OpenAI
from dotenv import load_dotenv
from loguru import logger
import sys

# Load environment variables
load_dotenv()

# Configure loguru logger for detailed UI Agent logging
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>UI_AGENT</cyan> | <level>{message}</level>",
    level="DEBUG"
)
logger.add(
    "logs/ui_agent_detailed.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | UI_AGENT | {message}",
    level="DEBUG",
    rotation="10 MB"
)


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
        
        # Model configuration with environment variable support
        self.model = os.getenv('OPENAI_MODEL', 'gpt-5-mini')
        
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

## Step-by-Step Thinking Process

ALWAYS think through your response step by step. For EVERY user query, follow this process:

1. **ANALYZE**: What is the user asking for? What is their intent?
2. **CATEGORIZE**: Does this need data retrieval or can I answer directly?
3. **DECIDE**: If data needed, what specific query type and parameters?
4. **EXECUTE**: Create the appropriate response (direct answer or JSON data request)

Think out loud by briefly explaining your reasoning before giving your final response.

## Query Analysis and Routing

When you receive a user query, determine if it needs data retrieval or if you can respond directly.

### Queries that NEED data retrieval (CREATE JSON IMMEDIATELY):
- Searching for specific anime: "Tell me about Naruto", "What's Attack on Titan about?"
- Genre-based queries: "What are good action anime?", "Show me romance anime"
- Year/season queries: "Best anime from 2023", "What's airing this season?"
- Rating-based: "Top rated anime", "Highest scoring shows"
- Personal history: "What am I watching?", "My completed list", "what's my watch history", "show my anime"
- Recommendations: "Recommend something for me", "Based on my history"

IMPORTANT: For watch history queries, ALWAYS create a data request immediately. Do NOT ask clarifying questions first.

### Queries you can answer DIRECTLY (no data needed):

For direct responses, follow these steps:
1. **RECOGNIZE**: "This is a [greeting|general question|help request|casual conversation]"  
2. **RESPOND**: "I can answer this directly because..."
3. **ENGAGE**: Provide a helpful, enthusiastic response

Examples:
- Greetings: "Hello", "Hi there"
- General anime questions: "What is anime?", "How do ratings work?"
- Help requests: "What can you help me with?"
- Casual conversation: "How are you?"

## Creating Data Requests

When data is needed, follow these steps:

1. **THINK**: "This query needs data because..."
2. **IDENTIFY**: "The query type should be [search_title|genre_filter|currently_airing|top_rated|watch_history|recommendations]"
3. **PARAMETERS**: "I need these parameters: title=X, genre=Y, limit=Z, etc."
4. **CREATE**: Generate the JSON request immediately

Create a JSON request IMMEDIATELY in this EXACT format. Do NOT ask clarifying questions first - be decisive and use reasonable defaults:

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
- "What's my watch history?" → "watch_history" with status="" (all statuses)
- "My completed anime" → "watch_history" with status="completed"
- "Show my anime list" → "watch_history" with status=""
- "Recommend something" → "recommendations"

CRITICAL: Watch history queries should NEVER ask for clarification. Create the data request immediately with reasonable defaults.

## Response Formatting

When formatting data results, make responses:
- Conversational and friendly
- Well-structured with clear headings
- Include relevant details (ratings, year, episodes, genres)
- Use markdown for better readability
- Suggest follow-up questions

## Step-by-Step Example

User: "What's my watch history?"

Your thinking process:
1. **ANALYZE**: "The user wants to see their personal watch history"
2. **CATEGORIZE**: "This needs data retrieval - it's about personal data"  
3. **DECIDE**: "Query type: watch_history, parameters: user_id='personal_user', status='', limit=20"
4. **EXECUTE**: Create JSON data request immediately

## Special Handling for Watch History Queries

For ANY watch history related query ("what's my watch history", "my anime", "what am I watching", etc.):
- IMMEDIATELY create a "watch_history" data request  
- Use reasonable defaults: limit=20, status="" (for all statuses)
- Do NOT ask clarifying questions - let the Data Retrieval Agent handle the query intelligently

Always be helpful and enthusiastic about anime!"""

        logger.info(f"🚀 User Interface Agent initialized")
        logger.debug(f"API Key: {self.api_key[:20]}...{self.api_key[-4:]}")
        logger.debug(f"Model: {self.model}")
        logger.debug(f"System prompt length: {len(self.system_prompt)} characters")

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
            logger.info(f"📝 STARTING query processing: '{user_query}'")
            logger.debug(f"Query length: {len(user_query)} characters")
            
            # Log the step-by-step thinking process the agent should follow
            logger.debug("🧠 Agent should follow: ANALYZE → CATEGORIZE → DECIDE → EXECUTE")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_query}
                ],
                max_completion_tokens=800
            )
            
            response_content = response.choices[0].message.content
            logger.debug(f"📤 Raw GPT response ({len(response_content)} chars): {response_content[:200]}...")
            logger.trace(f"📤 Full GPT response: {response_content}")
            
            # Log the decision-making process
            logger.info("🔍 ANALYZING response type...")
            
            # Check if this is a data request (contains JSON)
            contains_data_request = self._contains_data_request(response_content)
            logger.debug(f"Contains data request: {contains_data_request}")
            
            if contains_data_request:
                logger.info("📊 DECISION: Data request detected - routing to Data Retrieval Agent")
                data_request = self._extract_data_request(response_content, user_query)
                
                # Log the extracted data request details
                request_dict = data_request.to_dict()
                logger.info(f"🎯 DATA REQUEST created:")
                logger.info(f"  • Query Type: {request_dict.get('query_type')}")
                logger.info(f"  • Parameters: {request_dict.get('parameters')}")
                logger.info(f"  • Original Query: {request_dict.get('user_query')}")
                logger.debug(f"📋 Complete data request: {json.dumps(request_dict, indent=2)}")
                
                return {
                    "type": "data_request",
                    "request": data_request
                }
            else:
                logger.info("💬 DECISION: Direct response - no external data needed")
                logger.debug(f"Direct response preview: {response_content[:100]}...")
                return {
                    "type": "direct_response", 
                    "response": response_content
                }
                
        except Exception as e:
            logger.error(f"❌ ERROR processing user query: {e}")
            logger.exception("Full error traceback:")
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
            logger.info(f"🎨 FORMATTING data response for query: '{original_query}'")
            
            # Log data analysis
            data_summary = {
                "status": data_results.get("status", "unknown"),
                "count": data_results.get("count", 0),
                "query_type": data_results.get("query_type", "unknown"),
                "has_results": bool(data_results.get("results"))
            }
            logger.debug(f"📊 Data summary: {data_summary}")
            logger.trace(f"📊 Raw data results: {json.dumps(data_results, indent=2)}")
            
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

            logger.debug(f"🎭 Sending formatting prompt ({len(format_prompt)} chars)")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": format_prompt}
                ],
                max_completion_tokens=1200
            )
            
            formatted_response = response.choices[0].message.content
            logger.info(f"✨ FORMATTED response created ({len(formatted_response)} chars)")
            logger.debug(f"Formatted response preview: {formatted_response[:200]}...")
            logger.trace(f"Full formatted response: {formatted_response}")
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"❌ ERROR formatting data response: {e}")
            logger.exception("Full formatting error traceback:")
            return f"I got the data but had trouble formatting it nicely. Here's what I found: {str(data_results)}"

    def _contains_data_request(self, response: str) -> bool:
        """Check if the response contains a data request JSON."""
        # Check for both markdown-wrapped JSON and raw JSON
        has_markdown_json = "```json" in response and "action" in response and "data_request" in response
        has_raw_json = '"action": "data_request"' in response or "'action': 'data_request'" in response
        
        result = has_markdown_json or has_raw_json
        
        logger.debug(f"🔍 JSON detection analysis:")
        logger.debug(f"  • Has markdown JSON: {has_markdown_json}")
        logger.debug(f"  • Has raw JSON: {has_raw_json}")
        logger.debug(f"  • Final decision: {result}")
        
        return result

    def _extract_data_request(self, response: str, original_query: str) -> DataRequest:
        """Extract and parse the data request from the AI response."""
        logger.debug(f"🔧 EXTRACTING data request from response...")
        
        try:
            json_content = ""
            extraction_method = ""
            
            # Try to find markdown-wrapped JSON first
            start_idx = response.find('```json')
            end_idx = response.find('```', start_idx + 7)
            
            if start_idx != -1 and end_idx != -1:
                # Markdown-wrapped JSON
                json_content = response[start_idx + 7:end_idx].strip()
                extraction_method = "markdown-wrapped"
                logger.debug(f"📝 Found markdown-wrapped JSON at positions {start_idx}-{end_idx}")
            else:
                # Raw JSON - try to parse the entire response
                json_content = response.strip()
                extraction_method = "raw-response"
                logger.debug(f"📝 No markdown wrapper found, treating as raw JSON")
            
            logger.debug(f"📋 JSON extraction method: {extraction_method}")
            logger.debug(f"📋 JSON content length: {len(json_content)} chars")
            logger.trace(f"📋 Raw JSON content: {json_content}")
            
            request_data = json.loads(json_content)
            logger.debug(f"✅ JSON parsing successful")
            
            # Validate the request format
            if request_data.get("action") != "data_request":
                raise ValueError(f"Invalid request format - expected 'data_request', got '{request_data.get('action')}'")
            
            logger.debug(f"✅ JSON validation successful")
            
            data_request = DataRequest(
                query_type=request_data.get("query_type"),
                parameters=request_data.get("parameters", {}),
                original_query=original_query
            )
            
            logger.info(f"✨ DATA REQUEST successfully extracted:")
            logger.info(f"  • Method: {extraction_method}")
            logger.info(f"  • Query Type: {data_request.query_type}")
            logger.info(f"  • Parameters: {data_request.parameters}")
            
            return data_request
            
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parsing failed: {e}")
            logger.debug(f"Failed JSON content: {json_content[:500]}...")
        except Exception as e:
            logger.error(f"❌ Error extracting data request: {e}")
            
        # Fallback - create a generic search request
        logger.warning(f"🔄 FALLBACK: Creating generic search request")
        fallback_request = DataRequest(
            query_type="search_title",
            parameters={"title": original_query, "limit": 10},
            original_query=original_query
        )
        
        logger.info(f"🔄 Fallback request created: {fallback_request.to_dict()}")
        return fallback_request

    def get_capabilities(self) -> Dict[str, Any]:
        """Return information about the agent's capabilities."""
        logger.debug("📋 Retrieving agent capabilities")
        
        capabilities = {
            "name": self.name,
            "type": "user_interface",
            "model": self.model,
            "temperature": "default (1.0)",
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
        
        logger.trace(f"📋 Agent capabilities: {json.dumps(capabilities, indent=2)}")
        return capabilities


# Convenience function for testing
def create_user_interface_agent(api_key: Optional[str] = None) -> UserInterfaceAgent:
    """Create a User Interface Agent with optional API key."""
    return UserInterfaceAgent(api_key)


if __name__ == "__main__":
    """Simple test of the User Interface Agent."""
    
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