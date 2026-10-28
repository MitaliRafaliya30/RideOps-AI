"""
Streamlit chat UI for the RideOps AI assistant.

Two things matter structurally here:

1. Streamlit reruns this entire script on every interaction. The agent
   is expensive to build (DB connection, LLM client, tool setup), so it's
   cached with @st.cache_resource, which builds it once per session
   instead of on every keystroke.

2. Conversation memory needs a stable thread_id across those reruns.
   It's generated once and stored in session_state.
"""

import uuid
import streamlit as st

from src.agent import ask_with_trace
from src.config import validate_config, DATABRICKS_CATALOG, DATABRICKS_SCHEMA


st.set_page_config(
    page_title="RideOps AI Assistant",
    page_icon="🚕",
    layout="centered",
)


# --- Config check, fail loudly and early rather than mid-conversation ---
try:
    validate_config()
except ValueError as e:
    st.error(f"Configuration error: {e}")
    st.stop()


# --- Session state ---
# thread_id must persist across reruns or the agent forgets everything
# between messages.
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

if "messages" not in st.session_state:
    st.session_state.messages = []


# --- Header ---
st.title("🚕 RideOps AI Assistant")
st.caption(
    f"Ask questions about ride operations in plain English. "
    f"Queries run against `{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}` "
    f"(curated Gold tables only)."
)


# --- Sidebar ---
with st.sidebar:
    st.subheader("About")
    st.markdown(
        """
This assistant translates plain-English questions into SQL and runs them
against the RideOps Gold layer on Databricks.

**Scope:** curated Gold tables only. Bronze and Silver (raw and
intermediate layers) are deliberately out of reach.

**Read-only:** guardrails reject any query that isn't a SELECT.
        """
    )

    st.divider()

    if st.button("Clear conversation"):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()


# --- Render conversation history ---
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

        # Replay the SQL for past assistant messages too, otherwise
        # scrolling back loses the transparency.
        if msg.get("queries"):
            n = len(msg["queries"])
            label = f"View SQL ({n} {'query' if n == 1 else 'queries'})"
            with st.expander(label):
                for i, q in enumerate(msg["queries"], 1):
                    if n > 1:
                        st.caption(f"Query {i}")
                    st.code(q["sql"], language="sql")
                    st.caption(f"Result: {q['result']}")


# --- Handle new input ---
if prompt := st.chat_input("Ask about rides, drivers, zones, revenue..."):

    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Querying Databricks..."):
            try:
                result = ask_with_trace(
                    prompt, thread_id=st.session_state.thread_id
                )
                answer = result["answer"]
                queries = result["queries"]
            except Exception as e:
                answer = (
                    f"Something went wrong: `{type(e).__name__}`\n\n"
                    f"{str(e)[:300]}\n\n"
                    "If this is a rate limit, wait a moment and try again. "
                    "If it's a connection error, check that the Databricks "
                    "SQL Warehouse is running."
                )
                queries = []

        st.markdown(answer)

        # Transparency: show exactly what SQL produced this answer.
        # This is the difference between "trust me" and "verify me".
        if queries:
            label = f"View SQL ({len(queries)} " \
                    f"{'query' if len(queries) == 1 else 'queries'})"
            with st.expander(label):
                for i, q in enumerate(queries, 1):
                    if len(queries) > 1:
                        st.caption(f"Query {i}")
                    st.code(q["sql"], language="sql")
                    st.caption(f"Result: {q['result']}")

    st.session_state.messages.append({
        "role": "assistant",
        "content": answer,
        "queries": queries,
    })