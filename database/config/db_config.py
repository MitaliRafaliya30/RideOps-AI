import psycopg
from database.config.settings import POSTGRES


def get_connection():
    """
    Creates and returns a PostgreSQL connection.
    """

    return psycopg.connect(
        host=POSTGRES["host"],
        port=POSTGRES["port"],
        dbname=POSTGRES["dbname"],
        user=POSTGRES["user"],
        password=POSTGRES["password"],
    )