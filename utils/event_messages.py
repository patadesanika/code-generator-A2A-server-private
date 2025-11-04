# utils/event_messages.py
"""
Constants file containing predefined messages for the custom Kafka event logger.
These messages provide user-friendly descriptions of what the system is doing.
"""


class EventMessages:
    """
    Centralized collection of event messages for the Kafka event logger.
    Categories: SYSTEM, AGENT, TASK, ERROR, SUCCESS
    """

    # SYSTEM EVENTS
    SYSTEM_STARTUP = "System initializing..."
    SYSTEM_READY = "System ready to process requests"
    SYSTEM_SHUTDOWN = "System shutting down gracefully"

    # AGENT EVENTS
    AGENT_THINKING = "AI agent is thinking..."
    AGENT_ANALYZING = "Analyzing your request..."
    AGENT_PROCESSING = "Processing your request..."
    AGENT_PREPARING_RESPONSE = "Preparing response..."
    AGENT_FINALIZING = "Finalizing results..."

    # TASK EVENTS
    TASK_RECEIVED = "New task received"
    TASK_PARSING = "Parsing task requirements..."
    TASK_VALIDATION = "Validating input parameters..."
    TASK_EXECUTION_START = "Starting task execution..."
    TASK_EXECUTION_PROGRESS = "Task execution in progress..."
    TASK_EXECUTION_COMPLETE = "Task execution completed"

    # CODE GENERATION SPECIFIC EVENTS
    CODE_ANALYSIS_START = "Analyzing code requirements..."
    CODE_GENERATION_START = "Starting code generation..."
    CODE_GENERATION_PROGRESS = "Generating code..."
    CODE_REVIEW_START = "Reviewing generated code..."
    CODE_OPTIMIZATION = "Optimizing code structure..."
    CODE_FINALIZATION = "Finalizing code output..."

    # LLM INTERACTION EVENTS
    LLM_REQUEST_PREPARING = "Preparing request for AI model..."
    LLM_REQUEST_SENT = "Request sent to AI model..."
    LLM_RESPONSE_RECEIVED = "Response received from AI model..."
    LLM_RESPONSE_PROCESSING = "Processing AI model response..."

    # SUCCESS EVENTS
    SUCCESS_TASK_COMPLETED = "Task completed successfully"
    SUCCESS_CODE_GENERATED = "Code generated successfully"
    SUCCESS_RESPONSE_READY = "Response ready for delivery"

    # ERROR EVENTS
    ERROR_TASK_FAILED = "Task execution failed"
    ERROR_INVALID_INPUT = "Invalid input parameters detected"
    ERROR_SYSTEM_ERROR = "System error encountered"
    ERROR_TIMEOUT = "Operation timed out"
    ERROR_EXTERNAL_SERVICE = "External service error"

    # PROGRESS EVENTS
    PROGRESS_25 = "Task 25% complete..."
    PROGRESS_50 = "Task 50% complete..."
    PROGRESS_75 = "Task 75% complete..."
    PROGRESS_90 = "Task 90% complete..."
    PROGRESS_COMPLETE = "Task 100% complete"

    # FILE OPERATIONS
    FILE_READING = "Reading input files..."
    FILE_PROCESSING = "Processing file contents..."
    FILE_WRITING = "Writing output files..."
    FILE_VALIDATION = "Validating file formats..."

    # CONTEXT EVENTS
    CONTEXT_LOADING = "Loading context information..."
    CONTEXT_ANALYSIS = "Analyzing context data..."
    CONTEXT_APPLYING = "Applying context to task..."

    # STREAMING EVENTS
    STREAM_START = "Starting response stream..."
    STREAM_CHUNK = "Sending response chunk..."
    STREAM_END = "Response stream completed"


class EventTypes:
    """
    Event type categories for classification
    """

    SYSTEM = "system"
    AGENT = "agent"
    TASK = "task"
    CODE = "code"
    LLM = "llm"
    SUCCESS = "success"
    ERROR = "error"
    PROGRESS = "progress"
    FILE = "file"
    CONTEXT = "context"
    STREAM = "stream"


class EventPriority:
    """
    Event priority levels
    """

    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"
