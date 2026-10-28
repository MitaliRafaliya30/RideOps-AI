"""
LangGraph SQL agent for the RideOps AI assistant.

Uses create_react_agent instead of LangChain's deprecated create_sql_agent.
The classic agent builds prompts ending in a partial assistant turn
("prefilling"), which current models reject outright.

The agent gets four tools from SQLDatabaseToolkit:
  - list tables
  - get schema for specific tables
  - run a query
  - check a query for errors before running

It decides which to use and in what order. We don't script the steps.

Model note: served via OpenRouter using the OpenAI-compatible API.
The "~" prefix on the model slug is OpenRouter's floating-tag syntax,
it always resolves to the current version rather than pinning to a
specific one, which avoids breakage when a model is retired.
"""

from langchain_openai import ChatOpenAI
from langchain_openai import ChatOpenAI
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_core.tools import BaseTool
from langgraph.prebuilt import create_react_agent
from langgraph.checkpoint.memory import MemorySaver

from src.config import OPENROUTER_API_KEY, OPENROUTER_BASE_URL
from src.connection import get_database
from src.prompts import SYSTEM_PROMPT
from src.guardrails import validate_query, GuardrailViolation


def build_llm():
    """
    Claude Haiku via OpenRouter.

    temperature=0 for deterministic SQL, the same question should
    produce the same query every time.

    max_tokens caps response length, keeps answers concise and costs down.
    max_retries handles transient upstream rate limits with backoff.
    """
    return ChatOpenAI(
        model="~anthropic/claude-haiku-latest",
        api_key=OPENROUTER_API_KEY,
        base_url=OPENROUTER_BASE_URL,
        temperature=0,
        max_tokens=1000,
        max_retries=6,
    )

from langchain_core.tools import BaseTool
from src.guardrails import validate_query, GuardrailViolation

def _wrap_query_tool(tool: BaseTool) -> BaseTool:
    """
    Wrap the SQL execution tool so every query passes guardrails first.

    Returns the violation message as a tool result rather than raising,
    so the agent sees the rejection and can correct itself instead of
    the whole run crashing.
    """
    original_run = tool._run

    def guarded_run(query: str, *args, **kwargs):
        try:
            safe_query = validate_query(query)
        except GuardrailViolation as e:
            return f"QUERY REJECTED BY GUARDRAILS: {e}"
        return original_run(safe_query, *args, **kwargs)

    tool._run = guarded_run
    return tool

def build_agent():
    """
    Assemble the LangGraph agent.

    MemorySaver gives conversational memory: follow-up questions like
    "and what about last week?" work because the agent remembers the
    previous turn. Each conversation is keyed by a thread_id.
    """
    db = get_database()
    llm = build_llm()

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    tools = toolkit.get_tools()

    # Wrap the query-execution tool with guardrails.
    # The other three tools (list, schema, checker) are read-only by
    # nature and don't execute arbitrary SQL.
    tools = [
        _wrap_query_tool(t) if t.name == "sql_db_query" else t
        for t in tools
    ]

    agent = create_react_agent(
        model=llm,
        tools=tools,
        prompt=SYSTEM_PROMPT,
        checkpointer=MemorySaver(),
    )

    return agent


# Built once and reused, so MemorySaver persists across calls.
# Rebuilding per call gave every question a fresh, empty checkpointer,
# which silently disabled conversational memory.
_AGENT = None


def get_agent():
    """Return the shared agent instance, building it on first use."""
    global _AGENT
    if _AGENT is None:
        _AGENT = build_agent()
    return _AGENT


def _extract_text(message) -> str:
    """
    Some models return content as a list of blocks rather than a plain
    string. Pull out just the text parts and join them.
    """
    content = message.content

    if isinstance(content, str):
        return content

    if isinstance(content, list):
        parts = []
        for block in content:
            if isinstance(block, dict) and block.get("type") == "text":
                parts.append(block.get("text", ""))
            elif isinstance(block, str):
                parts.append(block)
        return "\n".join(parts).strip()

    return str(content)


def ask(question: str, thread_id: str = "default", verbose: bool = True) -> str:
    """
    Ask a question, get an answer.

    thread_id groups messages into a conversation. Same id means the agent
    remembers earlier turns; a new id starts fresh.

    recursion_limit is a hard ceiling on agent steps, a safety net in case
    the prompt alone doesn't stop it exploring.
    """
    agent = get_agent()

    config = {
        "configurable": {"thread_id": thread_id},
        "recursion_limit": 10,
    }
    inputs = {"messages": [("user", question)]}

    final_message = None

    for step in agent.stream(inputs, config, stream_mode="values"):
        message = step["messages"][-1]
        final_message = message

        if verbose:
            message.pretty_print()

    return _extract_text(final_message)

def ask_with_trace(question: str, thread_id: str = "default") -> dict:
    """
    Same as ask(), but also returns the SQL queries the agent ran.

    Costs nothing extra: the tool calls are already in the message stream,
    ask() just discards them. This walks the same stream and keeps them.

    Returns:
        {
            "answer": str,
            "queries": [{"sql": str, "result": str}, ...],
        }
    """
    agent = get_agent()

    config = {
        "configurable": {"thread_id": thread_id},
        "recursion_limit": 10,
    }
    inputs = {"messages": [("user", question)]}

    final_message = None
    queries = []
    pending_sql = None

    for step in agent.stream(inputs, config, stream_mode="values"):
        message = step["messages"][-1]
        final_message = message

        # An AI message requesting sql_db_query: capture the SQL
        tool_calls = getattr(message, "tool_calls", None)
        if tool_calls:
            for call in tool_calls:
                if call.get("name") == "sql_db_query":
                    pending_sql = call.get("args", {}).get("query", "")

        # The matching tool result: pair it with the SQL we just saw
        if getattr(message, "name", None) == "sql_db_query" and pending_sql:
            queries.append({
                "sql": pending_sql.strip(),
                "result": str(message.content)[:500],
            })
            pending_sql = None

    return {
        "answer": _extract_text(final_message),
        "queries": queries,
    }


def ask_quiet(question: str, thread_id: str = "default") -> str:
    """
    Same as ask(), but returns the answer without printing steps.
    Used by the Streamlit UI, which renders output itself.
    """
    return ask(question, thread_id=thread_id, verbose=False)