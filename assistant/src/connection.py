"""
Databricks connection layer for the RideOps AI assistant.

Builds a LangChain SQLDatabase object scoped to the Gold schema only.
Bronze and Silver are deliberately not exposed, the assistant works
with curated, verified data, never raw or in-progress tables.
"""

from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase

from src.config import (
    DATABRICKS_SERVER_HOSTNAME,
    DATABRICKS_HTTP_PATH,
    DATABRICKS_TOKEN,
    DATABRICKS_CATALOG,
    DATABRICKS_SCHEMA,
    ALLOWED_TABLES,
)


def build_engine():
    """
    Create a SQLAlchemy engine pointed at the Databricks SQL Warehouse.

    The connection string format is specific to databricks-sql-connector:
      databricks://token:<PAT>@<hostname>?http_path=<path>&catalog=<cat>&schema=<schema>
    """

    connection_string = (
        f"databricks://token:{DATABRICKS_TOKEN}@{DATABRICKS_SERVER_HOSTNAME}"
        f"?http_path={DATABRICKS_HTTP_PATH}"
        f"&catalog={DATABRICKS_CATALOG}"
        f"&schema={DATABRICKS_SCHEMA}"
    )

    engine = create_engine(connection_string)
    return engine


def get_database():
    """
    Build the LangChain SQLDatabase object.

    include_tables is the key governance control here: LangChain will only
    ever see, describe, or query these 14 tables. Anything outside this list
    is invisible to the agent, it can't query what it doesn't know exists.

    sample_rows_in_table_info=3 gives the LLM a few example rows per table
    when it inspects schema, which meaningfully improves the SQL it writes.
    """

    engine = build_engine()

    db = SQLDatabase(
        engine=engine,
        schema=DATABRICKS_SCHEMA,
        include_tables=ALLOWED_TABLES,
        sample_rows_in_table_info=3,
        view_support=True
    )

    return db


def test_connection():
    """
    Verify the connection works and the table scoping is correct.
    Returns a dict of results rather than printing, so callers decide
    how to display it.
    """

    db = get_database()

    visible_tables = db.get_usable_table_names()

    # Run a real query to prove the connection actually works,
    # not just that the object was created
    result = db.run("SELECT count(*) AS total_rides FROM fact_ride")

    return {
        "dialect": db.dialect,
        "visible_table_count": len(visible_tables),
        "visible_tables": sorted(visible_tables),
        "test_query_result": result,
    }