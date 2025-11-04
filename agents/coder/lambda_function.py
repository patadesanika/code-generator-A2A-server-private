import os
import logging
import asyncio
from mangum import Mangum
from dotenv import load_dotenv
 
# Load environment variables from .env file
load_dotenv()
 
from common.server import A2AServer
from common.types import (
    AgentCard,
    AgentCapabilities,
    AgentSkill,
    HTTPAuthSecurityScheme,
    MissingAPIKeyError,
    AgentConstraints
)
from common.utils.push_notification_auth import PushNotificationSenderAuth
from agents.coder.task_manager import AgentTaskManager
from agents.coder.agent import CoderAgent
 
 
# Configure logging to write INFO and above logs to the /tmp directory for AWS Lambda compatibility
log_file_path = os.getenv('LOG_FILE_PATH', '/tmp/app.log')
logging.basicConfig(level=logging.INFO, filemode="a", filename=log_file_path, format="%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(funcName)s - %(message)s")
logger = logging.getLogger(__name__)
 
def create_app():
    """
    Creates and configures the A2A server application.
    """
    try:
        # Set up the asyncio event loop
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        # Define agent capabilities (no streaming or push notifications)
        capabilities = AgentCapabilities(
            streaming=False,
            pushNotifications=False
        )
 
        # Construct the agent card with metadata, skills, and security schema
        agent_card = AgentCard(
            name="Code Helper",
            description="This is a Code Generation Automation Agent that specializes in generating, and delivering high-quality code in multiple programming languages",
            url=os.getenv('LAMBDA_URL'),
            version="1.0.0",
            defaultInputModes=CoderAgent.SUPPORTED_CONTENT_TYPES,
            defaultOutputModes=CoderAgent.SUPPORTED_CONTENT_TYPES,
            capabilities=capabilities,
            agent_constraints=AgentConstraints(
                    max_file_size =  "",
                    supported_file_types =  [],
                    max_files = 0,
                    prompt_template = [
                    "Generate a script to parse a CSV file and calculate summary statistics in {{language}}",
                    "Create a {{langauge}} code implementing a stack data structure with push/pop operations"
                    ],
                    prompt_template_variable_name = ["language"]
                ),
            skills=[
                AgentSkill(
                    id="multi-agent-code-generator",
                    name="Multi-Agent Code Generator",
                    description=(
                        "Generate production-ready code in multiple programming languages "
                        "using a CrewAI agent workflow (Developer). "
                        "This agent transforms natural language requirements into validated, dependency-documented and syntactically correct code."
                    ),
                    tags=["Code Generation", "Multi-Agent", "CrewAI", "MCP"],
                    examples=[
                        "Generate a Python script to parse a CSV file and calculate summary statistics.",
                        "Write a JavaScript function to fetch data from an API and render it in a table.",
                        "Create a Java class implementing a stack data structure with push/pop operations.",
                        "Generate a production-ready Python FastAPI service for user authentication.",
                        "Produce well-documented code in Go for file I/O operations with error handling.",
                    ],            
                )
            ],
            customAgentMetaData="CODE_GENERATOR_AGENT"
        )
        # Set up push notification sender authentication and generate JWK
        notification_sender_auth = PushNotificationSenderAuth()
        notification_sender_auth.generate_jwk()
 
        # Initialize the server with the agent card and task manager
        server = A2AServer(
            agent_card=agent_card,
            task_manager=AgentTaskManager(
                agent=CoderAgent(),
                notification_sender_auth=notification_sender_auth
            ),
            host="0.0.0.0",
            port=int(os.getenv("CODER_PORT", 5000)),
        )
 
        # Add endpoint for serving the JWKs (for push notification verification)
        server.app.add_route(
            "/.well-known/jwks.json",
            notification_sender_auth.handle_jwks_endpoint,
            methods=["GET"],
        )
 
        return server.app
 
    except MissingAPIKeyError as e:
        logger.error(f"Missing API Key Error: {e}")
        raise
    except Exception as e:
        import traceback
        logger.error(f"An error occurred during server startup: {traceback.format_exc()}")
        raise
 
app = create_app()
handler = Mangum(app)