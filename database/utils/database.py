from psycopg import Cursor


def truncate_tables(cursor: Cursor, tables: list[str]) -> None:
    """
    Truncates tables and resets identity values.
    """

    for table in tables:
        cursor.execute(
            f"TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;"
        )