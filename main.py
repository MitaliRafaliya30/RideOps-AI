from database.config.db_config import get_connection

conn = get_connection()

print("Connected Successfully!")

conn.close()