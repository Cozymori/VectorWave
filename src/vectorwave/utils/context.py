from contextvars import ContextVar

#default is realtime context
execution_source_context: ContextVar[str] = ContextVar("execution_source", default="REALTIME")
