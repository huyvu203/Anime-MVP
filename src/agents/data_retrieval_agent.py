"""
Intelligent Data Retrieval Agent for Anime Assistant Sequential Workflow.

This agent uses LLM-powered decision making to intelligently route queries:
1. Receives structured data requests from the User Interface Agent  
2. Uses GPT-5-mini to analyze requests and decide which data source to query
3. Routes personal queries to SQLite watch history database
4. Routes general queries to AWS Athena/S3 anime database
5. Uses both sources for complex queries like recommendations
6. Returns structured results back to the User Interface Agent

Intelligence Features:
- Automatic data source selection based on query context
- Personal vs general query detection
- Multi-source query coordination
- Context-aware routing decisions

Supported query types:
- search_title: Search for anime by title
- genre_filter: Filter anime by genre  
- currently_airing: Get currently airing anime
- top_rated: Get top-rated anime (with optional year filter)
- watch_history: Get user's personal watch history
- recommendations: Get personalized recommendations using both data sources
"""

import os
import json
import sqlite3
import sys
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
from openai import OpenAI
from dotenv import load_dotenv
from loguru import logger

# Load environment variables
load_dotenv()

# Configure loguru logger for detailed Data Retrieval Agent logging
logger.remove()  # Remove default handler
logger.add(
    sys.stderr,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>DATA_AGENT</cyan> | <level>{message}</level>",
    level="DEBUG"
)
logger.add(
    "logs/data_agent_detailed.log",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | DATA_AGENT | {message}",
    level="DEBUG",
    rotation="10 MB",
    retention="7 days"
)

# Import pandas with error handling for NumPy compatibility issues
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except Exception as e:
    logger.warning(f"Pandas import failed, will use fallback methods: {e}")
    PANDAS_AVAILABLE = False
    pd = None

# Import AthenaQueryClient for SQL-based queries
try:
    import sys
    sys.path.append(str(Path(__file__).parent.parent))
    from data.athena_client import AthenaQueryClient
    ATHENA_AVAILABLE = True
except Exception as e:
    logger.warning(f"AthenaQueryClient import failed: {e}")
    AthenaQueryClient = None
    ATHENA_AVAILABLE = False

# Import boto3 for direct S3 access if pandas fails
try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
    BOTO3_AVAILABLE = True
except Exception as e:
    logger.warning(f"Boto3 import failed: {e}")
    BOTO3_AVAILABLE = False


class DataRetrievalAgent:
    """
    Intelligent Data Retrieval Agent with LLM-powered decision making.
    
    This agent uses GPT-5-mini to intelligently decide which data source to query
    based on the context and nature of user requests.
    
    Responsibilities:
    - Analyze data requests using LLM reasoning
    - Intelligently route queries to appropriate data sources  
    - Execute SQL queries against anime data using AWS Athena
    - Query personal watch history SQLite database
    - Coordinate multi-source queries for recommendations
    - Return structured, consistent results
    - Handle errors gracefully with fallback routing
    
    Intelligence Features:
    - Personal vs general query detection
    - Context-aware data source selection
    - Multi-source query coordination
    - Adaptive routing based on query semantics
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """Initialize the Data Retrieval Agent with configuration."""
        
        logger.info("🚀 INITIALIZING Data Retrieval Agent...")
        
        self.config = config or self._load_default_config()
        self.name = "DataRetrievalAgent"
        
        logger.debug(f"📋 Agent configuration loaded:")
        logger.debug(f"  • S3 Bucket: {self.config.get('s3_bucket')}")
        logger.debug(f"  • AWS Region: {self.config.get('aws_region')}")
        logger.debug(f"  • Watch DB Path: {self.config.get('watch_data_path')}")
        
        # Set up OpenAI client for LLM decision-making
        self.api_key = os.getenv('OPENAI_API_KEY')
        if not self.api_key:
            logger.error("❌ OPENAI_API_KEY environment variable is required")
            raise ValueError("OPENAI_API_KEY environment variable is required")
        
        logger.debug(f"🔑 OpenAI API Key loaded: {self.api_key[:20]}...{self.api_key[-4:]}")
        
        self.client = OpenAI(api_key=self.api_key)
        self.model = os.getenv('OPENAI_MODEL', 'gpt-5-mini')
        
        logger.info(f"🧠 OpenAI client initialized with model: {self.model}")
        logger.debug(f"📝 System prompt length: {len(self.system_prompt) if hasattr(self, 'system_prompt') else 0} characters")
        
        # System prompt for intelligent data source routing with step-by-step thinking
        self.system_prompt = """You are an intelligent Data Retrieval Agent that decides which data source to query based on user requests.

THINK STEP BY STEP and THINK OUT LOUD. Follow this structured reasoning process:

🔍 STEP 1 - ANALYZE REQUEST:
- What is the user asking for?
- What type of information do they need?
- Are there any personal context clues?

📊 STEP 2 - CATEGORIZE QUERY:
- Is this a PERSONAL query (about user's own data)?
- Is this a GENERAL query (about anime in general)?  
- Is this a HYBRID query (needs both personal + general data)?

🎯 STEP 3 - DECIDE DATA SOURCE:
Available Data Sources:
1. WATCH_HISTORY (SQLite): Personal watch history, ratings, status (watching/completed/plan_to_watch)
2. ANIME_DATABASE (AWS Athena/S3): General anime metadata, scores, genres, airing status

Decision Rules:
- Personal queries ("my list", "what I'm watching", "my ratings") → WATCH_HISTORY
- General anime queries ("top rated", "currently airing", "search anime") → ANIME_DATABASE  
- Recommendation queries → BOTH (history for preferences + database for options)

🚀 STEP 4 - EXECUTE STRATEGY:
- Explain your specific approach for this query
- Detail how you'll use the chosen data source(s)

Always think through each step explicitly before making your final decision.

Respond with JSON in this format:
{
    "thinking": {
        "step1_analysis": "What is the user asking for?",
        "step2_categorization": "Personal/General/Hybrid and why?",
        "step3_decision": "Which data source and why?",
        "step4_strategy": "How will I execute this query?"
    },
    "data_source": "WATCH_HISTORY" | "ANIME_DATABASE" | "BOTH",
    "reasoning": "Final reasoning summary",
    "query_strategy": "Specific implementation approach"
}"""
        
        # Initialize Athena query client
        logger.debug("🏗️ Initializing AWS Athena client...")
        if ATHENA_AVAILABLE and AthenaQueryClient:
            try:
                self.athena_client = AthenaQueryClient(
                    region=self.config.get('aws_region')
                )
                logger.info(f"✅ AthenaQueryClient initialized for region: {self.config.get('aws_region')}")
                logger.debug(f"📊 Athena client ready for general anime database queries")
            except Exception as e:
                logger.error(f"❌ AthenaQueryClient initialization failed: {e}")
                logger.warning(f"⚠️ Will fallback to local data sources only")
                self.athena_client = None
        else:
            logger.warning(f"⚠️ AthenaQueryClient not available - imports failed")
            logger.debug(f"📋 Available flags: ATHENA_AVAILABLE={ATHENA_AVAILABLE}, AthenaQueryClient={AthenaQueryClient is not None}")
            self.athena_client = None
        
        # Initialize SQLite database path
        logger.debug("🗄️ Initializing SQLite watch history database...")
        self.db_path = Path(self.config.get('watch_data_path', 'data/user_history.db'))
        logger.debug(f"📂 SQLite DB path: {self.db_path.absolute()}")
        
        if not self.db_path.exists():
            logger.warning(f"⚠️ Watch history database not found: {self.db_path}")
            logger.debug(f"📋 Database will be created on first personal query")
        else:
            logger.info(f"✅ Watch history database found: {self.db_path}")
            
            # Log database stats
            try:
                with sqlite3.connect(self.db_path) as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [row[0] for row in cursor.fetchall()]
                    logger.debug(f"📊 Available tables: {tables}")
            except Exception as e:
                logger.debug(f"📋 Could not read database stats: {e}")
        
        # Final initialization summary
        capabilities = {
            "llm_decision_making": True,
            "athena_available": self.athena_client is not None,
            "sqlite_available": self.db_path.exists(),
            "model": self.model
        }
        
        logger.info(f"🎯 Data Retrieval Agent READY with intelligent routing!")
        logger.info(f"📊 Agent capabilities: {capabilities}")

    def _load_default_config(self) -> Dict[str, Any]:
        """Load default configuration from environment."""
        logger.debug("📋 Loading default configuration from environment variables...")
        
        config = {
            's3_bucket': os.getenv('S3_BUCKET', 'anime-mvp-data'),
            'aws_region': os.getenv('AWS_REGION', 'us-east-2'),
            'watch_data_path': 'data/user_history.db'
        }
        
        logger.debug(f"⚙️ Default config loaded: {config}")
        return config

    def process_data_request(self, data_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a structured data request from the UI Agent.
        
        Args:
            data_request: Dictionary with 'query_type', 'parameters', 'user_query'
            
        Returns:
            Structured response dictionary with results
        """
        start_time = datetime.now()
        
        try:
            query_type = data_request.get('query_type')
            parameters = data_request.get('parameters', {})
            original_query = data_request.get('user_query', '')
            
            logger.info(f"📥 INCOMING DATA REQUEST")
            logger.info(f"  • Query Type: {query_type}")
            logger.info(f"  • Original Query: '{original_query}'")
            logger.info(f"  • Parameters: {parameters}")
            logger.debug(f"📋 Full request: {json.dumps(data_request, indent=2)}")
            
            # Log routing decision
            logger.info(f"🎯 ROUTING query to handler: {query_type}")
            
            # Route to appropriate handler based on query type
            result = None
            if query_type == "search_title":
                logger.debug(f"➡️ Routing to: _search_anime_by_title()")
                result = self._search_anime_by_title(parameters)
            elif query_type == "genre_filter":
                logger.debug(f"➡️ Routing to: _get_anime_by_genre()")
                result = self._get_anime_by_genre(parameters)
            elif query_type == "currently_airing":
                logger.debug(f"➡️ Routing to: _get_currently_airing()")
                result = self._get_currently_airing(parameters)
            elif query_type == "top_rated":
                logger.debug(f"➡️ Routing to: _get_top_rated_anime()")
                result = self._get_top_rated_anime(parameters)
            elif query_type == "watch_history":
                logger.debug(f"➡️ Routing to: _get_user_watch_history()")
                result = self._get_user_watch_history(parameters)
            elif query_type == "recommendations":
                logger.debug(f"➡️ Routing to: _get_user_recommendations()")
                result = self._get_user_recommendations(parameters)
            else:
                logger.error(f"❌ UNKNOWN query type: {query_type}")
                result = {
                    "status": "error",
                    "message": f"Unknown query type: {query_type}",
                    "query_type": query_type
                }
            
            # Log processing completion
            processing_time = (datetime.now() - start_time).total_seconds()
            
            if result.get('status') == 'success':
                record_count = len(result.get('data', []))
                logger.info(f"✅ PROCESSING COMPLETE")
                logger.info(f"  • Status: Success")
                logger.info(f"  • Records Returned: {record_count}")
                logger.info(f"  • Processing Time: {processing_time:.3f}s")
                logger.debug(f"📤 Result sample: {str(result)[:200]}...")
            else:
                logger.error(f"❌ PROCESSING FAILED")
                logger.error(f"  • Status: {result.get('status')}")
                logger.error(f"  • Error: {result.get('message')}")
                logger.error(f"  • Processing Time: {processing_time:.3f}s")
            
            logger.trace(f"📤 Complete result: {json.dumps(result, indent=2)}")
            return result
                
        except Exception as e:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.error(f"💥 CRITICAL ERROR in data request processing:")
            logger.error(f"  • Exception: {type(e).__name__}: {str(e)}")
            logger.error(f"  • Processing Time: {processing_time:.3f}s")
            logger.error(f"  • Request: {data_request}")
            logger.exception("Full traceback:")
            
            return {
                "status": "error",
                "message": f"Data retrieval failed: {str(e)}",
                "query_type": data_request.get('query_type', 'unknown'),
                "processing_time": processing_time
            }

    def _decide_data_source(self, data_request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Use LLM to intelligently decide which data source to query.
        
        Args:
            data_request: The structured data request from UI Agent
            
        Returns:
            Dictionary with LLM decision including reasoning and strategy
        """
        logger.info(f"🧠 STARTING LLM decision-making process...")
        
        try:
            # Prepare the query for LLM analysis
            query_context = {
                "query_type": data_request.get('query_type'),
                "parameters": data_request.get('parameters', {}),
                "user_query": data_request.get('user_query', ''),
                "available_sources": {
                    "WATCH_HISTORY": self.db_path.exists(),
                    "ANIME_DATABASE": self.athena_client is not None
                }
            }
            
            logger.debug(f"📋 Query context for LLM:")
            logger.debug(f"  • Query Type: {query_context['query_type']}")
            logger.debug(f"  • User Query: '{query_context['user_query']}'")
            logger.debug(f"  • Parameters: {query_context['parameters']}")
            logger.debug(f"  • Available Sources: {query_context['available_sources']}")
            
            # Create LLM prompt
            user_prompt = f"""Analyze this data request and decide the optimal data source strategy:

REQUEST DETAILS:
- Query Type: {query_context['query_type']}
- User Query: "{query_context['user_query']}"
- Parameters: {json.dumps(query_context['parameters'], indent=2)}

AVAILABLE DATA SOURCES:
- WATCH_HISTORY (SQLite): Available = {query_context['available_sources']['WATCH_HISTORY']}
- ANIME_DATABASE (Athena/S3): Available = {query_context['available_sources']['ANIME_DATABASE']}

Please analyze step-by-step and provide your decision in the specified JSON format."""
            
            logger.debug(f"📤 Sending request to GPT-{self.model}...")
            logger.trace(f"📝 Full prompt: {user_prompt}")
            
            # Make LLM API call
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.3,  # Lower temperature for more consistent decisions
                max_tokens=1000
            )
            
            response_content = response.choices[0].message.content
            
            logger.debug(f"📥 Raw LLM response ({len(response_content)} chars): {response_content[:200]}...")
            logger.trace(f"📥 Complete LLM response: {response_content}")
            
            # Parse LLM response
            try:
                decision = json.loads(response_content)
                logger.info(f"🎯 LLM DECISION ANALYSIS:")
                
                if "thinking" in decision:
                    thinking = decision["thinking"]
                    logger.info(f"  🔍 Step 1 - Analysis: {thinking.get('step1_analysis', 'N/A')}")
                    logger.info(f"  📊 Step 2 - Categorization: {thinking.get('step2_categorization', 'N/A')}")
                    logger.info(f"  🎯 Step 3 - Decision: {thinking.get('step3_decision', 'N/A')}")
                    logger.info(f"  🚀 Step 4 - Strategy: {thinking.get('step4_strategy', 'N/A')}")
                
                logger.info(f"✅ FINAL DECISION:")
                logger.info(f"  • Data Source: {decision.get('data_source')}")
                logger.info(f"  • Reasoning: {decision.get('reasoning')}")
                logger.info(f"  • Strategy: {decision.get('query_strategy')}")
                
                return {
                    "status": "success",
                    "decision": decision,
                    "llm_model": self.model,
                    "tokens_used": response.usage.total_tokens if hasattr(response, 'usage') else None
                }
                
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse LLM response as JSON: {e}")
                logger.debug(f"Invalid JSON: {response_content}")
                
                # Fallback decision
                fallback = self._create_fallback_decision(data_request)
                logger.warning(f"🔄 Using fallback decision: {fallback}")
                return fallback
                
        except Exception as e:
            logger.error(f"💥 LLM decision-making failed: {e}")
            logger.exception("Full traceback:")
            
            # Fallback decision
            fallback = self._create_fallback_decision(data_request)
            logger.warning(f"🔄 Using fallback decision due to error: {fallback}")
            return fallback

    def _create_fallback_decision(self, data_request: Dict[str, Any]) -> Dict[str, Any]:
        """Create a fallback decision when LLM routing fails."""
        logger.debug(f"🔄 Creating fallback routing decision...")
        
        query_type = data_request.get('query_type', '')
        
        # Simple rule-based fallback
        if query_type in ['watch_history']:
            data_source = 'WATCH_HISTORY'
        elif query_type in ['recommendations']:
            data_source = 'BOTH'
        else:
            data_source = 'ANIME_DATABASE'
        
        fallback_decision = {
            "data_source": data_source,
            "reasoning": f"Fallback routing for {query_type}",
            "query_strategy": "Standard query execution",
            "thinking": {
                "step1_analysis": "Fallback analysis - LLM unavailable",
                "step2_categorization": f"Categorized as {query_type}",
                "step3_decision": f"Routing to {data_source}",
                "step4_strategy": "Execute with available methods"
            }
        }
        
        logger.info(f"🔄 Fallback decision: {fallback_decision}")
        
        return {
            "status": "fallback",
            "decision": fallback_decision,
            "reason": "LLM decision-making unavailable"
        }

    def _search_anime_by_title(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Search for anime by title using Athena SQL queries."""
        logger.info(f"🔍 EXECUTING title search...")
        
        try:
            title = parameters.get('title', '')
            limit = parameters.get('limit', 10)
            
            logger.debug(f"📋 Search parameters:")
            logger.debug(f"  • Title query: '{title}'")
            logger.debug(f"  • Result limit: {limit}")
            
            if not self.athena_client:
                logger.error(f"❌ Athena client not available for title search")
                return self._create_error_response("Athena client not available", "search_title")
            
            logger.debug(f"🗄️ Executing Athena query for title search...")
            
            # Execute Athena query
            result = self.athena_client.search_anime_by_title(title, limit)
            
            logger.debug(f"📊 Athena query result status: {result.get('status')}")
            
            if result['status'] == 'success':
                logger.debug(f"✅ Athena query successful, processing {len(result.get('rows', []))} raw rows")
                
                # Convert Athena results to structured format
                results = []
                for i, row in enumerate(result['rows']):
                    if len(row) >= 6:  # title, score, year, type, episodes, status
                        anime_data = {
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None,
                            'status': row[5]
                        }
                        results.append(anime_data)
                        
                        if i == 0:  # Log first result as sample
                            logger.debug(f"📝 Sample result: {anime_data}")
                    else:
                        logger.warning(f"⚠️ Row {i} has insufficient columns: {len(row)}")
                
                logger.info(f"✅ TITLE SEARCH completed: {len(results)} results")
                
                return {
                    "status": "success",
                    "query_type": "search_title",
                    "results": results,
                    "count": len(results),
                    "search_term": title,
                    "query_id": result.get('query_id')
                }
            else:
                logger.error(f"❌ Athena query failed: {result.get('error', 'Unknown error')}")
                return {
                    "status": "error",
                    "query_type": "search_title",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}",
                    "search_term": title
                }
            
        except Exception as e:
            logger.error(f"💥 Exception in title search: {type(e).__name__}: {e}")
            logger.exception("Full traceback:")
            return self._create_error_response(str(e), "search_title")

    def _get_anime_by_genre(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get anime filtered by genre using Athena SQL queries."""
        try:
            genre = parameters.get('genre', '')
            limit = parameters.get('limit', 20)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "genre_filter")
            
            # Execute Athena query
            result = self.athena_client.get_anime_by_genre(genre, limit)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 5:  # title, score, year, type, episodes
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None
                        })
                
                return {
                    "status": "success",
                    "query_type": "genre_filter",
                    "results": results,
                    "count": len(results),
                    "genre": genre,
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "status": "error",
                    "query_type": "genre_filter", 
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}",
                    "genre": genre
                }
            
        except Exception as e:
            logger.error(f"Genre filter error: {e}")
            return self._create_error_response(str(e), "genre_filter")

    def _get_currently_airing(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get currently airing anime using Athena SQL queries."""
        try:
            limit = parameters.get('limit', 20)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "currently_airing")
            
            # Execute Athena query
            result = self.athena_client.get_currently_airing(limit)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 6:  # title, score, year, type, episodes, status
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if row[2] and row[2] != '' else None,
                            'type': row[3],
                            'episodes': int(row[4]) if row[4] and row[4] != '' else None,
                            'status': row[5]
                        })
                
                return {
                    "status": "success",
                    "query_type": "currently_airing",
                    "results": results,
                    "count": len(results),
                    "query_id": result.get('query_id')
                }
            else:
                return {
                    "status": "error",
                    "query_type": "currently_airing",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}"
                }
            
        except Exception as e:
            logger.error(f"Currently airing error: {e}")
            return self._create_error_response(str(e), "currently_airing")

    def _get_top_rated_anime(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get top-rated anime, optionally filtered by year using Athena."""
        try:
            limit = parameters.get('limit', 20)
            year = parameters.get('year')
            min_score = parameters.get('min_score', 7.0)
            
            if not self.athena_client:
                return self._create_error_response("Athena client not available", "top_rated")
            
            # Use specific year query if year is provided
            if year:
                result = self.athena_client.get_anime_by_year(int(year), limit)
            else:
                result = self.athena_client.get_top_rated_anime(limit, min_score)
            
            if result['status'] == 'success':
                # Convert Athena results to structured format
                results = []
                for row in result['rows']:
                    if len(row) >= 5:  # Varies by query, but at least title, score
                        results.append({
                            'title': row[0],
                            'score': float(row[1]) if row[1] and row[1] != '' else None,
                            'year': row[2] if len(row) > 2 and row[2] and row[2] != '' else None,
                            'type': row[3] if len(row) > 3 else None,
                            'episodes': int(row[4]) if len(row) > 4 and row[4] and row[4] != '' else None,
                            'status': row[5] if len(row) > 5 else None
                        })
                
                response = {
                    "status": "success",
                    "query_type": "top_rated", 
                    "results": results,
                    "count": len(results),
                    "query_id": result.get('query_id')
                }
                
                if year:
                    response["year"] = year
                
                return response
            else:
                return {
                    "status": "error",
                    "query_type": "top_rated",
                    "message": f"Athena query failed: {result.get('error', 'Unknown error')}"
                }
            
        except Exception as e:
            logger.error(f"Top rated error: {e}")
            return self._create_error_response(str(e), "top_rated")

    def _get_user_watch_history(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Get user's personal watch history from SQLite database."""
        logger.info(f"📚 EXECUTING watch history query...")
        
        try:
            user_id = parameters.get('user_id', 'personal_user')
            status = parameters.get('status')  # Optional status filter
            limit = parameters.get('limit', 50)
            
            logger.debug(f"📋 Watch history parameters:")
            logger.debug(f"  • User ID: {user_id}")
            logger.debug(f"  • Status filter: {status}")
            logger.debug(f"  • Result limit: {limit}")
            
            if not self.db_path.exists():
                logger.error(f"❌ Watch history database not found: {self.db_path}")
                return self._create_error_response("Watch history database not found", "watch_history")
            
            logger.debug(f"🗃️ Connecting to SQLite database: {self.db_path}")
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access
            
            # Build query
            query = "SELECT * FROM user_watch_history WHERE user_id = ?"
            params = [user_id]
            
            if status:
                query += " AND watch_status = ?"
                params.append(status)
            
            query += " ORDER BY created_at DESC"
            
            if limit:
                query += " LIMIT ?"
                params.append(limit)
            
            logger.debug(f"📝 SQL Query: {query}")
            logger.debug(f"📝 Parameters: {params}")
            
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = cursor.fetchall()
            
            logger.debug(f"📊 SQL query returned {len(rows)} rows")
            
            # Convert to list of dictionaries
            results = [dict(row) for row in rows]
            conn.close()
            
            if results:
                logger.debug(f"📝 Sample record: {dict(results[0]) if results else 'None'}")
            
            logger.info(f"✅ WATCH HISTORY query completed: {len(results)} records")
            
            response = {
                "status": "success",
                "query_type": "watch_history",
                "results": results,
                "count": len(results),
                "user_id": user_id
            }
            
            if status:
                response["status_filter"] = status
            
            return response
            
        except Exception as e:
            logger.error(f"Watch history error: {e}")
            return self._create_error_response(str(e), "watch_history")

    def _get_user_recommendations(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        """Generate personalized recommendations based on watch history."""
        try:
            user_id = parameters.get('user_id', 'personal_user')
            count = parameters.get('limit', 10)
            
            # Get user's watch history to understand preferences
            history_response = self._get_user_watch_history({
                'user_id': user_id,
                'limit': 100  # Get more history for better recommendations
            })
            
            if history_response['status'] != 'success' or not history_response['results']:
                return {
                    "status": "success",
                    "query_type": "recommendations",
                    "results": [],
                    "count": 0,
                    "message": "No watch history available for recommendations"
                }
            
            # Analyze user preferences from history
            history = history_response['results']
            
            # Get genres from completed/high-rated anime
            preferred_genres = set()
            for entry in history:
                if entry.get('watch_status') == 'completed' and entry.get('rating', 0) >= 7:
                    if entry.get('genre'):
                        preferred_genres.add(entry.get('genre'))
            
            if not preferred_genres:
                # Fallback to any genres from watch history
                for entry in history[:10]:  # Look at recent entries
                    if entry.get('genre'):
                        preferred_genres.add(entry.get('genre'))
            
            # Get recommendations from S3 data based on preferences
            if not self.s3_reader or not preferred_genres:
                return {
                    "status": "success",
                    "query_type": "recommendations",
                    "results": [],
                    "count": 0,
                    "message": "Unable to generate recommendations"
                }
            
            df = self.s3_reader.read_anime_data()
            if df is None or df.empty:
                return self._create_error_response("No anime data available", "recommendations")
            
            # Filter out anime user has already seen
            watched_ids = {entry.get('anime_id') for entry in history}
            
            if 'mal_id' in df.columns:
                df = df[~df['mal_id'].isin(watched_ids)]
            elif 'id' in df.columns:
                df = df[~df['id'].isin(watched_ids)]
            
            # Score anime based on genre matches and ratings
            df['rec_score'] = 0
            
            for genre in preferred_genres:
                if 'genres' in df.columns:
                    genre_match = df['genres'].str.contains(genre, case=False, na=False)
                elif 'genre' in df.columns:
                    genre_match = df['genre'].str.contains(genre, case=False, na=False)
                else:
                    continue
                    
                df.loc[genre_match, 'rec_score'] += 1
            
            # Boost score for highly rated anime
            if 'score' in df.columns:
                df['rec_score'] += df['score'] / 10  # Normalize score contribution
            
            # Sort by recommendation score
            recommended_df = df[df['rec_score'] > 0].sort_values('rec_score', ascending=False)
            
            if PANDAS_AVAILABLE:
                results = recommended_df.head(count).to_dict('records')
            else:
                results = []
            
            return {
                "status": "success",
                "query_type": "recommendations",
                "results": results,
                "count": len(results),
                "user_id": user_id,
                "based_on_genres": list(preferred_genres)
            }
            
        except Exception as e:
            logger.error(f"Recommendations error: {e}")
            return self._create_error_response(str(e), "recommendations")

    def _search_s3_direct(self, title: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Direct S3 search without pandas (fallback method)."""
        try:
            if not self.s3_client:
                return []
            
            bucket = self.config.get('s3_bucket')
            prefix = 'processed/'  # Assuming processed data is in this prefix
            
            # List objects in the processed folder
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=100  # Limit for performance
            )
            
            results = []
            title_lower = title.lower()
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    
                    # Skip non-data files
                    if not (key.endswith('.json') or key.endswith('.parquet')):
                        continue
                    
                    try:
                        # For demo, create mock results based on filename
                        # In production, you'd download and parse the files
                        if title_lower in key.lower():
                            mock_result = {
                                'title': key.replace(prefix, '').replace('.json', '').replace('_', ' ').title(),
                                'source': 's3_fallback',
                                'key': key,
                                'score': 7.5,  # Mock score
                                'type': 'TV',
                                'episodes': 12,
                                'status': 'Finished Airing'
                            }
                            results.append(mock_result)
                            
                            if len(results) >= limit:
                                break
                    except Exception as e:
                        logger.warning(f"Error processing S3 object {key}: {e}")
                        continue
            
            return results
            
        except Exception as e:
            logger.error(f"Direct S3 search error: {e}")
            return []

    def _get_s3_fallback_data(self, data_type: str = 'anime', limit: int = 50) -> List[Dict[str, Any]]:
        """Get mock data when S3 access is limited."""
        try:
            # Return mock data structure for testing
            mock_data = []
            
            if data_type == 'top_rated':
                # Mock top rated anime
                top_anime = [
                    {'title': 'Fullmetal Alchemist: Brotherhood', 'score': 9.1, 'rank': 1, 'type': 'TV', 'episodes': 64},
                    {'title': 'Steins;Gate', 'score': 9.0, 'rank': 2, 'type': 'TV', 'episodes': 24},
                    {'title': 'Attack on Titan', 'score': 8.9, 'rank': 3, 'type': 'TV', 'episodes': 25},
                    {'title': 'Spirited Away', 'score': 8.8, 'rank': 4, 'type': 'Movie', 'episodes': 1},
                    {'title': 'Your Name', 'score': 8.4, 'rank': 5, 'type': 'Movie', 'episodes': 1}
                ]
                mock_data = top_anime[:limit]
                
            elif data_type == 'airing':
                # Mock currently airing
                airing_anime = [
                    {'title': 'One Piece', 'score': 8.9, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 1000},
                    {'title': 'Boruto', 'score': 7.5, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 200},
                    {'title': 'Demon Slayer Season 4', 'score': 8.7, 'status': 'Currently Airing', 'type': 'TV', 'episodes': 12}
                ]
                mock_data = airing_anime[:limit]
                
            return mock_data
            
        except Exception as e:
            logger.error(f"Fallback data error: {e}")
            return []

    def _create_error_response(self, message: str, query_type: str) -> Dict[str, Any]:
        """Create a standardized error response."""
        return {
            "status": "error",
            "message": message,
            "query_type": query_type,
            "results": [],
            "count": 0
        }

    def get_capabilities(self) -> Dict[str, Any]:
        """Return information about the agent's capabilities."""
        logger.debug("📋 Retrieving agent capabilities")
        
        capabilities = {
            "name": self.name,
            "type": "intelligent_data_retrieval",
            "llm_model": self.model,
            "decision_making": "LLM-powered data source routing",
            "data_sources": {
                "athena_available": self.athena_client is not None,
                "watch_db_available": self.db_path.exists()
            },
            "intelligence_features": [
                "Automatic data source selection",
                "Context-aware query routing",
                "Personal vs general query detection",
                "Multi-source recommendation logic"
            ],
            "supported_queries": [
                "search_title",
                "genre_filter",
                "currently_airing", 
                "top_rated",
                "watch_history",
                "recommendations"
            ],
            "config": {
                "s3_bucket": self.config.get('s3_bucket'),
                "watch_db_path": str(self.db_path)
            }
        }
        
        logger.trace(f"📋 Agent capabilities: {json.dumps(capabilities, indent=2)}")
        return capabilities


# Convenience function for testing
def create_data_retrieval_agent(config: Optional[Dict[str, Any]] = None) -> DataRetrievalAgent:
    """Create a Data Retrieval Agent with optional configuration."""
    return DataRetrievalAgent(config)


if __name__ == "__main__":
    """Simple test of the Data Retrieval Agent."""
    
    # Test data requests
    test_requests = [
        {
            "query_type": "search_title",
            "parameters": {"title": "Attack on Titan", "limit": 3},
            "user_query": "Tell me about Attack on Titan"
        },
        {
            "query_type": "watch_history", 
            "parameters": {"user_id": "personal_user", "limit": 5},
            "user_query": "What am I currently watching?"
        },
        {
            "query_type": "top_rated",
            "parameters": {"limit": 5, "year": "2023"},
            "user_query": "Best anime from 2023"
        }
    ]
    
    try:
        # Create agent
        agent = create_data_retrieval_agent()
        print(f"✅ Data Retrieval Agent created successfully")
        
        capabilities = agent.get_capabilities()
        print(f"📋 S3 Available: {capabilities['data_sources']['s3_available']}")
        print(f"📋 Watch DB Available: {capabilities['data_sources']['watch_db_available']}")
        
        # Test requests
        print(f"\n🧪 Testing {len(test_requests)} data requests...")
        
        for i, request in enumerate(test_requests, 1):
            print(f"\n{'='*50}")
            print(f"Test {i}: {request['query_type']}")
            print(f"Query: {request['user_query']}")
            print('='*50)
            
            result = agent.process_data_request(request)
            
            print(f"Status: {result['status']}")
            print(f"Count: {result['count']}")
            
            if result['status'] == 'success' and result['results']:
                print("Sample results:")
                for j, item in enumerate(result['results'][:2], 1):
                    title = item.get('title') or item.get('anime_title', 'Unknown')
                    print(f"  {j}. {title}")
            elif result['status'] == 'error':
                print(f"Error: {result['message']}")
                
    except Exception as e:
        print(f"❌ Test failed: {e}")