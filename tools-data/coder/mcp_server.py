from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field
from typing import Optional, List, Any, Dict
import requests
import os
import logging
 
# Configure logging to write INFO and above logs to the /tmp directory for AWS Lambda compatibility
log_file_path = os.getenv('LOG_FILE_PATH', '/tmp/app.log')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename=log_file_path, filemode="a")
 
API_BASE_URL = os.getenv('CODER_BACKEND', "http://code-generator-backend-service.default.svc:8000")
 
def read_auth_token(auth_file_path: Optional[str]) -> Optional[str]:
    """Helper function to read auth token from file"""
    if auth_file_path and os.path.exists(auth_file_path):
        try:
            with open(auth_file_path, 'r') as f:
                auth_token = f.read().strip()
            return auth_token if auth_token else None
        except Exception as e:
            logging.error(f"Error reading auth token from file: {e}")
            return None
    return None
 
def get_auth_headers(auth_file_path: Optional[str]) -> Dict[str, str]:
    """Helper function to get authorization headers"""
    headers = {}
    auth_token = read_auth_token(auth_file_path)
    if auth_token:
        headers["Authorization"] = f"Bearer {auth_token}"
        logging.info(f"Using auth token from file: {len(auth_token)} characters")
    else:
        logging.warning("No auth token available")
    return headers
 
mcp = FastMCP("Coder")
 
class CodeGenerationRequest(BaseModel):
    """Request model for generating code using CrewAI agents."""
    language: str = Field(..., description="Programming language (e.g., python, java, javascript)")
    query: str = Field(..., description="User query describing what code to generate")
    auth_file_path: Optional[str] = Field(None, title="Auth File Path", description="Path to file containing auth token")
 
 
class CodeGenerationResponse(BaseModel):
    """Response model for code generation results."""
    success: bool = Field(..., description="Whether the operation was successful")
    code: Optional[str] = Field(None, description="Generated code, if successful")
    error: Optional[str] = Field(None, description="Error message, if the operation failed")
    message: str = Field(..., description="A general message about the operation's outcome")
   
@mcp.tool()
async def generate_code(request: CodeGenerationRequest) -> str:
    """Generate code using CrewAI agents based on a user query and language preference.
 
    This tool sends a request to a CrewAI backend API to generate code for a given
    programming language and a detailed user query.
 
    Args:
        request: A CodeGenerationRequest object containing:
                 - language: The target programming language (e.g., "python", "javascript").
                 - query: A detailed description of the code to be generated.
 
    Returns:
        A JSON string representing a CodeGenerationResponse object. This includes
        a 'success' boolean, 'code' if successful, 'error' if failed, and a 'message'.
 
    Raises:
        Exception: Catches various exceptions during the HTTP request or response
                   processing and returns a structured error message within the
                   CodeGenerationResponse JSON.
    """
    try:
        # The base URL for the CrewAI Developer API should be configured via environment variable
        api_base_url = os.getenv("BACKEND_URL", "http://localhost:8000")
        url = f"{api_base_url}/generate-code"
        headers = {"Content-Type": "application/json"}
        headers.update(get_auth_headers(request.auth_file_path))
 
        payload = request.model_dump()
        logging.info(f"Attempting to generate code. Request URL: {url}, Payload: {payload}")
 
        response = requests.post(url, json=payload,headers=headers, timeout=900) # Added timeout for potentially long operations
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
 
        response_data = response.json()
        logging.info(f"Code generation successful. Status: {response.status_code}, Response: {response_data}")
 
        # Validate and return the response using the Pydantic model
        return CodeGenerationResponse(**response_data).model_dump_json()
 
    except requests.exceptions.HTTPError as e:
        status_code = e.response.status_code
        error_detail = e.response.text
        error_message = f"HTTP Error {status_code} from CrewAI API: {error_detail}"
        logging.error(f"HTTPError in generate_code: {error_message}", exc_info=True)
        return CodeGenerationResponse(
            success=False,
            code=None,
            error=error_detail,
            message=error_message
        ).model_dump_json()
    except requests.exceptions.ConnectionError as e:
        error_message = f"Connection to CrewAI API failed: {str(e)}. Please check the API server status and CREWAI_API_BASE_URL."
        logging.error(f"ConnectionError in generate_code: {error_message}", exc_info=True)
        return CodeGenerationResponse(
            success=False,
            code=None,
            error=str(e),
            message=error_message
        ).json()
    except requests.exceptions.Timeout as e:
        error_message = f"CrewAI API request timed out after 300 seconds: {str(e)}. The operation may be too long-running or the server is slow."
        logging.error(f"Timeout in generate_code: {error_message}", exc_info=True)
        return CodeGenerationResponse(
            success=False,
            code=None,
            error=str(e),
            message=error_message
        ).json()
    except requests.exceptions.RequestException as e:
        error_message = f"An unknown request error occurred with CrewAI API: {str(e)}"
        logging.error(f"RequestException in generate_code: {error_message}", exc_info=True)
        return CodeGenerationResponse(
            success=False,
            code=None,
            error=str(e),
            message=error_message
        ).json()
    except Exception as e:
        error_message = f"An unexpected error occurred during code generation: {str(e)}"
        logging.error(f"Unexpected Exception in generate_code: {error_message}", exc_info=True)
        return CodeGenerationResponse(
            success=False,
            code=None,
            error=str(e),
            message=error_message
        ).json()
 
if __name__ == "__main__":
    mcp.run()
