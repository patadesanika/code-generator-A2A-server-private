from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.tools import tool
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver
from langchain_core.messages import AIMessage, ToolMessage
from typing import Any, Dict, AsyncIterable, List, Literal, Optional
import os
import tempfile
from pydantic import BaseModel
import asyncio
import re
import json
from ..tool import coder_tools_client
import logging
# from utils.observability import GeminiLoggingHandler
from utils.obs import TokenTracker

memory = MemorySaver()

if os.getenv("MODEL_NAME") is None:
    raise ValueError("MODEL_NAME is missing in ENV")

GEMINI_MODEL = os.getenv("MODEL_NAME")

def get_coder_tools():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    coder_tools = loop.run_until_complete(coder_tools_client.get_tools())

    return coder_tools

class ResponseFormat(BaseModel):
    """Respond to the user in this format.
        Args: 
            status: status of the request
            message: Text message to output from the LLM
            files: List of file paths that are provided by the Tools which are available to transfer, not included the files provided by the user
       """
    status: Literal["input_required", "completed", "error"] = "input_required"
    message: str
    files: List[str]

class CoderAgent:

    SUPPORTED_CONTENT_TYPES = ["text", "text/plain", "application/zip"]
    SYSTEM_INSTRUCTION = (
        """You are a Code Generation Automation Agent that specializes in generating code, and delivering high-quality code in multiple programming languages. Your purpose is to transform
user requirements into production-ready code through a structured multi-agent workflow
involving development.

EXECUTION WORKFLOW:

STEP 1: REQUIREMENT CAPTURE & VALIDATION (MANDATORY ENTRY POINT)
- Always check that the user provides:
  * programming language (e.g., 'python', 'javascript')
  * query (description of the required functionality or feature)
- If either field is missing, return an error response and DO NOT proceed to code generation.
- Always echo back the validated inputs (language + query) before invoking the code generation process.

STEP 2: CODE GENERATION (MANDATORY)
- Always invoke the `generate_code` tool with InputCodeGenerator containing:
  * language: Programming language provided by the user
  * query: User’s code request / requirements
  * llm_model: Model string (default: gemini-2.5-flash-lite unless overridden)
  * api_key: Gemini API key (taken from env or passed via input)
- The `generate_code` tool automatically orchestrates the Developer.
- Ensure the call executes successfully and WAIT for the tool response.
- If generation fails, return the error message and STOP the workflow.

STEP 3: OUTPUT FORMATTING & DELIVERABLE (MANDATORY)

- Ensure the COMPLETE generated code is preserved without truncation.
- Do NOT add placeholder text or summaries — always deliver the exact code.

COMMUNICATION PROTOCOL:
- Always ensure COMPLETE preservation of tool outputs. The host agent MUST receive:
  * The final complete code
- Always show the user the final code snippet
- Never omit, compress, or paraphrase the code.
- Log any errors with error details in `"error": "<message>"` while setting `"success": false` in the final response.

ALWAYS REMEMBER IT IS A SEQUENTIAL PROCESS:
1. Validate inputs
2. Invoke generate_code
- Never skip steps or output partial results.
- Respect strict tool-to-host response integrity."""
    )

    def __init__(self):
        # self.gemini_logging_handler = GeminiLoggingHandler()
        self.model = ChatGoogleGenerativeAI(model=GEMINI_MODEL, thinking_budget=-1)
        self.tools = [
            *get_coder_tools(),
        ]
        logging.info(f"Tools:{self.tools}")
        self.graph = create_react_agent(
            self.model, tools=self.tools, checkpointer=memory, prompt = self.SYSTEM_INSTRUCTION, response_format=ResponseFormat
        )

    def _extract_auth_token(self, query) -> Optional[str]:
        """Extract auth token from query structure."""
        if 'auth_token' in query:
            return query['auth_token']
        return None
    
    async def invoke(self, query, sessionId) -> str:
        token_tracker = TokenTracker()
        config = {"configurable": {"thread_id": sessionId}, "callbacks": [token_tracker]}
        
        # Set the request on the callback handler
        # self.gemini_logging_handler.set_request(request)
        
        auth_token = self._extract_auth_token(query)
        auth_file_path = None
        
        if auth_token:
            try:
                temp_fd, auth_file_path = tempfile.mkstemp(suffix='.txt', prefix='auth_')
                with os.fdopen(temp_fd, 'w') as f:
                    f.write(auth_token)
                logging.info(f"Auth token stored in temporary file: {auth_file_path}")
            except Exception as e:
                logging.error(f"Failed to create or write to auth temp file: {e}", exc_info=True)
                if 'temp_fd' in locals() and temp_fd is not None:
                    os.close(temp_fd)
                if auth_file_path and os.path.exists(auth_file_path):
                    os.remove(auth_file_path)
                auth_file_path = None

        file_paths_query = ""
        if len(query['files']) > 0:
            file_paths_query = "File Paths:\n" + "\n".join(
                f"- {file['uri']} ({file['mimeType']})" for file in query['files']
            )
        
        # Include auth file path in query if present
        auth_info = ""
        if auth_file_path:
            auth_info = f"\nAuth File Path: {auth_file_path}"
        
        full_query = query['text'] + "\n\n" + file_paths_query + auth_info
        logging.info(f"agent:{await self.graph.ainvoke({'messages': [('user', full_query)]}, config)}")  
        logging.info(f"Auth file path used: {auth_file_path}")
        
        return self.get_agent_response(config)

    async def stream(self, query, sessionId) -> AsyncIterable[Dict[str, Any]]:
        inputs = {"messages": [("user", query)]}
        config = {"configurable": {"thread_id": sessionId}}

        for item in self.graph.stream(inputs, config, stream_mode="values"):
            message = item["messages"][-1]
            if (
                isinstance(message, AIMessage)
                and message.tool_calls
                and len(message.tool_calls) > 0
            ):
                yield {
                    "is_task_complete": False,
                    "require_user_input": False,
                    "content": "Looking up the exchange rates...",
                }
            elif isinstance(message, ToolMessage):
                yield {
                    "is_task_complete": False,
                    "require_user_input": False,
                    "content": "Processing the exchange rates..",
                }            
        
        yield self.get_agent_response(config)

        
    def get_agent_response(self, config):

        current_state = self.graph.get_state(config)
        # Get messages from the state
        messages = current_state.values.get('messages', [])
        logging.info(f"Number of messages found: {len(messages)}")
        # Find the last AIMessage
        if messages:
            last_message = messages[-1]
            if isinstance(last_message, AIMessage):
                # Return the AI's response directly
                # Ensure content is always a string
                content = last_message.content
                if isinstance(content, list):
                    content = "\n".join(str(item) for item in content)
                elif not isinstance(content, str):
                    content = str(content)
                return {
                    "is_task_complete": True,
                    "require_user_input": False,
                    "content": content
                }

        return {
            "is_task_complete": False,
            "require_user_input": True,
            "content": "We are unable to process your request at the moment. Please try again.",
        }
