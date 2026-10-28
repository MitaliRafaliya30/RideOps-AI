"""
Query guardrails for the RideOps AI assistant.

M2 confirmed that LangChain's include_tables controls what the agent SEES,
not what it can EXECUTE, a fully-qualified Bronze query ran successfully
despite Bronze being excluded from the schema. These guardrails enforce
at execution time what schema scoping only suggests.

Defense in depth: the agent shouldn't try to query Bronze (it doesn't know
it exists), but if it does, this stops it.
"""

import re

from src.config import ALLOWED_TABLES, DATABRICKS_CATALOG, DATABRICKS_SCHEMA


# SQL keywords that modify data or schema. Any of these = reject.
BLOCKED_KEYWORDS = [
    "insert", "update", "delete", "drop", "truncate", "alter",
    "create", "replace", "merge", "grant", "revoke",
    "copy", "restore", "vacuum", "optimize",
]

# Schemas the assistant must never touch, even fully qualified.
BLOCKED_SCHEMAS = ["bronze", "silver"]

MAX_ROWS = 1000


class GuardrailViolation(Exception):
    """Raised when a query fails a safety check."""
    pass


def _strip_sql_comments(sql: str) -> str:
    """
    Remove comments before checking keywords.
    Without this, "-- delete" or "/* drop */" could hide intent,
    or a harmless comment could trigger a false rejection.
    """
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql


def check_read_only(sql: str) -> None:
    """
    Reject anything that isn't a plain SELECT.

    Checks two things: the statement must START with SELECT or WITH,
    and must not contain any data-modifying keyword anywhere (which
    catches things hidden after a semicolon).
    """
    clean = _strip_sql_comments(sql).strip().lower()

    if not clean:
        raise GuardrailViolation("Empty query.")

    if not (clean.startswith("select") or clean.startswith("with")):
        raise GuardrailViolation(
            "Only SELECT queries are allowed. "
            f"Query starts with: '{clean.split()[0]}'"
        )

    for keyword in BLOCKED_KEYWORDS:
        if re.search(rf"\b{keyword}\b", clean):
            raise GuardrailViolation(
                f"Query contains blocked keyword: '{keyword}'. "
                "This assistant is read-only."
            )


def check_allowed_tables(sql: str) -> None:
    """
    Reject queries touching anything outside the Gold whitelist.

    Two checks:
      1. No reference to bronze/silver schemas, even fully qualified
      2. Every table-like name after FROM/JOIN must be in ALLOWED_TABLES
    """
    clean = _strip_sql_comments(sql).lower()

    # Block raw/intermediate layers explicitly
    for schema in BLOCKED_SCHEMAS:
        if re.search(rf"\b{schema}\.", clean):
            raise GuardrailViolation(
                f"Query references the '{schema}' schema. "
                "This assistant only has access to curated Gold tables."
            )

    # Extract table names following FROM or JOIN
    referenced = re.findall(
        r"\b(?:from|join)\s+([a-zA-Z0-9_.]+)",
        clean,
    )

    allowed_lower = {t.lower() for t in ALLOWED_TABLES}

    for ref in referenced:
        # Strip catalog/schema qualifiers: rideops_ai.gold.fact_ride -> fact_ride
        table = ref.split(".")[-1]

        # Ignore subquery aliases and CTE names, they aren't real tables.
        # A CTE name won't be in ALLOWED_TABLES but also isn't a security
        # risk, so only reject names that look like a different schema.
        if table not in allowed_lower:
            # Only reject if it was schema-qualified (a real table reference)
            if "." in ref:
                raise GuardrailViolation(
                    f"Query references table '{ref}', which is not in the "
                    "allowed Gold table list."
                )


def enforce_row_limit(sql: str, max_rows: int = MAX_ROWS) -> str:
    """
    Add a LIMIT if the query doesn't have one.

    Returns the (possibly modified) query rather than raising, this is
    a safety net, not a violation.
    """
    clean = _strip_sql_comments(sql).strip().rstrip(";")

    if re.search(r"\blimit\s+\d+", clean, re.IGNORECASE):
        return clean

    return f"{clean} LIMIT {max_rows}"


def validate_query(sql: str) -> str:
    """
    Run all checks. Returns the safe, possibly-modified query.
    Raises GuardrailViolation if the query is rejected.
    """
    check_read_only(sql)
    check_allowed_tables(sql)
    return enforce_row_limit(sql)