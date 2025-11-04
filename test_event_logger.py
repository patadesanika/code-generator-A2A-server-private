#!/usr/bin/env python3
"""
Test script for the Custom Kafka Event Logger
"""

import os
import sys
import time
import asyncio

# Add the current directory to Python path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Load environment variables
from dotenv import load_dotenv

load_dotenv()

from utils.kafka import create_event_logger
from utils.event_messages import EventMessages, EventTypes, EventPriority


async def test_event_logger():
    """Test the custom event logger functionality"""
    print("Testing Custom Kafka Event Logger")
    print("=" * 50)

    # Mock user context with encrypted_payload
    user_context = {
        "user_email": "test@example.com",
        "user_id": "test-user-123",
        "encrypted_payload": "mock-encrypted-payload-abc123",
    }

    # Create event logger
    event_logger = create_event_logger(
        session_id="test-session-456", user_context=user_context
    )

    print("1. Testing system events...")
    event_logger.log_event(EventMessages.SYSTEM_STARTUP)
    await asyncio.sleep(0.1)

    print("2. Testing task events...")
    event_logger.log_event(EventMessages.TASK_RECEIVED)
    event_logger.log_event(EventMessages.TASK_PARSING)
    await asyncio.sleep(0.1)

    print("3. Testing thinking events...")
    event_logger.log_thinking(EventMessages.AGENT_THINKING)
    event_logger.log_thinking(EventMessages.AGENT_ANALYZING)
    await asyncio.sleep(0.1)

    print("4. Testing progress events...")
    event_logger.log_progress(EventMessages.PROGRESS_25, 25)
    event_logger.log_progress(EventMessages.PROGRESS_50, 50)
    event_logger.log_progress(EventMessages.PROGRESS_75, 75)
    await asyncio.sleep(0.1)

    print("5. Testing LLM interaction events...")
    event_logger.log_llm_interaction(EventMessages.LLM_REQUEST_PREPARING)
    event_logger.log_llm_interaction(EventMessages.LLM_REQUEST_SENT)
    event_logger.log_llm_interaction(EventMessages.LLM_RESPONSE_RECEIVED)
    await asyncio.sleep(0.1)

    print("6. Testing code generation events...")
    event_logger.log_code_event(EventMessages.CODE_GENERATION_START)
    event_logger.log_code_event(EventMessages.CODE_GENERATION_PROGRESS)
    event_logger.log_code_event(EventMessages.CODE_FINALIZATION)
    await asyncio.sleep(0.1)

    print("7. Testing streaming events...")
    event_logger.log_stream_event(EventMessages.STREAM_START)
    event_logger.log_stream_event(EventMessages.STREAM_CHUNK)
    event_logger.log_stream_event(EventMessages.STREAM_END)
    await asyncio.sleep(0.1)

    print("8. Testing success events...")
    event_logger.log_success(EventMessages.SUCCESS_CODE_GENERATED)
    event_logger.log_success(EventMessages.SUCCESS_TASK_COMPLETED)
    await asyncio.sleep(0.1)

    print("9. Testing error events...")
    event_logger.log_error(
        EventMessages.ERROR_TIMEOUT, "Operation timed out after 30 seconds"
    )
    await asyncio.sleep(0.1)

    print("10. Testing custom events...")
    event_logger.log_event("Custom system message")
    await asyncio.sleep(0.1)

    print("\n✅ All event logger tests completed!")
    print(f"📡 Events sent to topic: {event_logger.EVENT_TOPIC}")
    print(f"🏷️  Session ID: {event_logger.session_id}")
    print(f"👤 User: {user_context['user_email']}")
    print(f"🖥️  Agent: {event_logger.agent_name}")
    print(f"🌐 Server: {event_logger.server_name}")

    # Close the logger
    event_logger.close()


if __name__ == "__main__":
    asyncio.run(test_event_logger())
