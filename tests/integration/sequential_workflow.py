#!/usr/bin/env python3
"""
Sequential Workflow Coordinator for Anime Assistant

This module coordinates the sequential workflow between:
1. User Interface Agent - Processes natural language queries  
2. Data Retrieval Agent - Executes SQL queries against Athena

Flow: User Question → UI Agent → Data Agent → UI Agent → Formatted Response
"""

import os
import sys
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / "src"))

import logging
import json
import sys
from typing import Dict, Any, Optional
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from agents.user_interface_agent import create_user_interface_agent
from agents.data_retrieval_agent import DataRetrievalAgent

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AnimeAssistantWorkflow:
    """
    Sequential workflow coordinator for anime assistant.
    
    Manages communication between UI Agent and Data Retrieval Agent
    using a sequential pattern where each agent processes and forwards
    the request to the next agent in the chain.
    """
    
    def __init__(self):
        """Initialize both agents and workflow coordinator."""
        
        logger.info("🚀 Initializing Anime Assistant Sequential Workflow")
        
        # Initialize UI Agent
        try:
            self.ui_agent = create_user_interface_agent()
            logger.info("✅ User Interface Agent initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize UI Agent: {e}")
            raise
        
        # Initialize Data Retrieval Agent  
        try:
            self.data_agent = DataRetrievalAgent()
            logger.info("✅ Data Retrieval Agent initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Data Agent: {e}")
            raise
        
        logger.info("🎉 Sequential workflow ready!")
    
    def process_user_query(self, user_question: str) -> Dict[str, Any]:
        """
        Process a user query through the sequential workflow.
        
        Args:
            user_question: Natural language question from user
            
        Returns:
            Complete response with formatted results
        """
        
        logger.info(f"📝 Processing user query: {user_question[:100]}...")
        
        try:
            # Step 1: UI Agent processes natural language query
            logger.info("🧠 Step 1: UI Agent processing natural language...")
            
            # Get structured request from UI Agent
            ui_response = self.ui_agent.process_user_query(user_question)
            logger.info(f"UI Agent response type: {type(ui_response)}")
            
            # Extract structured request from UI Agent response
            structured_request = self._extract_data_request_from_ui_response(ui_response)
            
            if not structured_request:
                # Check if UI Agent provided a direct conversational response
                if isinstance(ui_response, dict) and ui_response.get('status') == 'conversational':
                    return {
                        "status": "success",
                        "message": ui_response.get('response', 'I can help you with anime questions!'),
                        "user_query": user_question,
                        "response_type": "conversational"
                    }
                else:
                    return {
                        "status": "error", 
                        "message": "Could not parse query into structured request",
                        "user_query": user_question,
                        "ui_response": str(ui_response)
                    }
            
            logger.info(f"📊 Structured request: {structured_request['query_type']} with {len(structured_request.get('parameters', {}))} parameters")
            
            # Step 2: Data Agent executes the structured request
            logger.info("🔍 Step 2: Data Agent executing query...")
            
            data_response = self.data_agent.process_data_request(structured_request)
            
            logger.info(f"Data query result: {data_response['status']} - {data_response.get('count', 0)} results")
            
            # Step 3: UI Agent formats the response  
            logger.info("📋 Step 3: UI Agent formatting response...")
            
            formatted_response = self._format_final_response(
                user_question, 
                structured_request,
                data_response
            )
            
            return formatted_response
            
        except Exception as e:
            logger.error(f"❌ Workflow error: {e}")
            return {
                "status": "error", 
                "message": f"Workflow processing failed: {str(e)}",
                "user_query": user_question
            }
    
    def _extract_data_request_from_ui_response(self, ui_response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract structured data request from UI Agent response."""
        
        try:
            # The UI Agent returns a structured response  
            if isinstance(ui_response, dict):
                # Check for the actual format returned by UI Agent
                if 'request' in ui_response and ui_response.get('type') == 'data_request':
                    # UI Agent found a structured request
                    data_request = ui_response['request']
                    return {
                        'query_type': data_request.query_type,
                        'parameters': data_request.parameters
                    }
                elif 'data_request' in ui_response and ui_response['data_request']:
                    # Alternative format
                    data_request = ui_response['data_request']
                    return {
                        'query_type': data_request.query_type,
                        'parameters': data_request.parameters
                    }
                elif ui_response.get('type') == 'conversational':
                    # UI Agent provided a conversational response - no data needed
                    logger.info("UI Agent provided conversational response, no data query needed")
                    return None
                else:
                    logger.warning(f"UI response format: {ui_response}")
                    return None
            
            logger.warning(f"Unexpected UI response format: {type(ui_response)}")
            return None
            
        except Exception as e:
            logger.error(f"Error extracting data request: {e}")
            return None
    
    def _get_default_parameters(self, query_type: str) -> Dict[str, Any]:
        """Get default parameters for a query type."""
        
        defaults = {
            "search_title": {"limit": 10},
            "genre_filter": {"limit": 20},
            "top_rated": {"limit": 10, "min_score": 7.0},
            "currently_airing": {"limit": 20},
            "watch_history": {"limit": 50},
            "recommendations": {"limit": 15}
        }
        
        return defaults.get(query_type, {"limit": 10})
    
    def _format_final_response(self, user_question: str, structured_request: Dict[str, Any], data_response: Dict[str, Any]) -> Dict[str, Any]:
        """Format the final response with UI Agent help."""
        
        try:
            # If data query was successful, format the results
            if data_response['status'] == 'success':
                results = data_response.get('results', [])
                count = data_response.get('count', 0)
                
                # Create a summary for the UI Agent to format
                summary_request = {
                    "user_question": user_question,
                    "query_type": structured_request['query_type'],
                    "results_count": count,
                    "sample_results": results[:3] if results else [],
                    "total_results": len(results)
                }
                
                # Use UI Agent to create human-friendly response
                formatted_text = self.ui_agent.format_data_response(user_question, data_response)
                
                return {
                    "status": "success",
                    "message": formatted_text,
                    "user_query": user_question,
                    "structured_request": structured_request,
                    "data_results": data_response,
                    "results_count": count,
                    "sample_results": results[:5]
                }
            else:
                # Handle error case
                error_message = data_response.get('message', 'Unknown error occurred')
                
                return {
                    "status": "error",
                    "message": f"I'm sorry, I couldn't find the anime information you requested. {error_message}",
                    "user_query": user_question,
                    "structured_request": structured_request,
                    "error_details": data_response
                }
                
        except Exception as e:
            logger.error(f"Error formatting final response: {e}")
            
            # Fallback formatting
            if data_response['status'] == 'success':
                results = data_response.get('results', [])
                if results:
                    titles = [r.get('title', 'Unknown') for r in results[:5]]
                    return {
                        "status": "success", 
                        "message": f"Found {len(results)} anime: {', '.join(titles)}",
                        "user_query": user_question,
                        "data_results": data_response
                    }
            
            return {
                "status": "error",
                "message": "Sorry, I encountered an error processing your request.",
                "user_query": user_question,
                "error": str(e)
            }


def test_sequential_workflow():
    """Test the sequential workflow with sample queries."""
    
    print("🧪 Testing Sequential Workflow")
    print("=" * 50)
    
    try:
        # Initialize workflow
        workflow = AnimeAssistantWorkflow()
        
        # Test queries
        test_queries = [
            "What are the top rated anime?",
            "Find anime about attack on titan",
            "Show me action anime",
            "What anime is currently airing?"
        ]
        
        for i, query in enumerate(test_queries, 1):
            print(f"\\n📝 Test {i}: {query}")
            print("-" * 30)
            
            result = workflow.process_user_query(query)
            
            print(f"Status: {result['status']}")
            if result['status'] == 'success':
                print(f"Results: {result.get('results_count', 0)} found")
                print(f"Message: {result['message'][:200]}...")
            else:
                print(f"Error: {result.get('message', 'Unknown error')}")
        
        print("\\n✅ Sequential workflow test completed!")
        return True
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        logger.exception("Test error details:")
        return False


if __name__ == "__main__":
    success = test_sequential_workflow()
    sys.exit(0 if success else 1)