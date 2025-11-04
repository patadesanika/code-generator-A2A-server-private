import os
from langchain_mcp_adapters.client import MultiServerMCPClient
from typing import cast
from langchain_mcp_adapters.sessions import Connection, StdioConnection

from dotenv import load_dotenv

load_dotenv()

# Coder tool
coder_configs = {
    "coder": {
        "command": "python",
        "args" : [
            "tools-data/coder/mcp_server.py",
        ],
        "env": {
            "BACKEND_URL": os.getenv("CODER_BACKEND"),
        },
        "transport": "stdio",
    }
}
coder_tools_client = MultiServerMCPClient(coder_configs)
