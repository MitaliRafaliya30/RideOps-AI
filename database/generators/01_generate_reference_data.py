from database.config.db_config import get_connection

from database.utils.logger import (
    print_header,
    print_info,
    print_success,
    print_error,
)

from database.utils.timer import Timer
from database.utils.database import truncate_tables
from database.utils.constants import REFERENCE_TABLES

from psycopg import Connection
from psycopg import Cursor


# ============================================================
# Reference Data
# ============================================================

VEHICLE_TYPES = [
    ("UberX", 4, 1.00),
    ("UberXL", 6, 1.30),
    ("Uber Comfort", 4, 1.20),
    ("Uber Black", 4, 2.20),
    ("Uber Green", 4, 1.10),
    ("Uber WAV", 4, 1.15),
]

MEMBERSHIP_TIERS = [
    ("Basic", 1.00),
    ("Silver", 1.10),
    ("Gold", 1.25),
    ("Platinum", 1.50),
]

PAYMENT_METHODS = [
    ("Credit Card",),
    ("Debit Card",),
    ("Cash",),
    ("UPI",),
    ("Digital Wallet",),
]

RIDE_STATUS = [
    ("Requested",),
    ("Assigned",),
    ("Accepted",),
    ("Arrived",),
    ("Started",),
    ("Completed",),
    ("Cancelled",),
]

CANCELLATION_REASONS = [
    ("Driver Cancelled", "DRIVER"),
    ("Rider Cancelled", "RIDER"),
    ("No Driver Available", "SYSTEM"),
    ("Payment Failed", "SYSTEM"),
    ("Duplicate Request", "SYSTEM"),
    ("Driver No Show", "DRIVER"),
    ("Rider No Show", "RIDER"),
    ("Vehicle Breakdown", "SYSTEM"),
]


# ============================================================
# Insert Functions
# ============================================================

def insert_vehicle_types(cursor: Cursor) -> None:
    """
    Inserts vehicle types into master.vehicle_types.
    """

    query = """
        INSERT INTO master.vehicle_types
        (
            vehicle_type_name,
            capacity,
            base_fare_multiplier
        )
        VALUES (%s, %s, %s);
    """

    cursor.executemany(query, VEHICLE_TYPES)

    print_success(f"Inserted {len(VEHICLE_TYPES)} vehicle types")


def insert_membership_tiers(cursor: Cursor) -> None:
    """
    Inserts membership tiers into master.membership_tiers.
    """

    query = """
        INSERT INTO master.membership_tiers
        (
            membership_name,
            reward_points_multiplier
        )
        VALUES (%s, %s);
    """

    cursor.executemany(query, MEMBERSHIP_TIERS)

    print_success(f"Inserted {len(MEMBERSHIP_TIERS)} membership tiers")


def insert_payment_methods(cursor: Cursor) -> None:
    """
    Inserts payment methods into master.payment_methods.
    """

    query = """
        INSERT INTO master.payment_methods
        (
            payment_method_name
        )
        VALUES (%s);
    """

    cursor.executemany(query, PAYMENT_METHODS)

    print_success(f"Inserted {len(PAYMENT_METHODS)} payment methods")


def insert_ride_status(cursor: Cursor) -> None:
    """
    Inserts ride statuses into master.ride_status.
    """

    query = """
        INSERT INTO master.ride_status
        (
            ride_status_name
        )
        VALUES (%s);
    """

    cursor.executemany(query, RIDE_STATUS)

    print_success(f"Inserted {len(RIDE_STATUS)} ride statuses")


def insert_cancellation_reasons(cursor: Cursor) -> None:
    """
    Inserts cancellation reasons into master.cancellation_reasons.
    """

    query = """
        INSERT INTO master.cancellation_reasons
        (
            cancellation_reason_name,
            reason_category
        )
        VALUES (%s, %s);
    """

    cursor.executemany(query, CANCELLATION_REASONS)

    print_success(
        f"Inserted {len(CANCELLATION_REASONS)} cancellation reasons"
    )


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Generates and loads all reference data into PostgreSQL.
    """

    timer = Timer()
    timer.start()

    conn: Connection | None = None
    cursor: Cursor | None = None

    try:

        print_header("RideOps AI - Reference Data Generator")

        conn = get_connection()
        cursor = conn.cursor()

        print_info("Connected to PostgreSQL.")

        print_info("Cleaning existing reference data...")

        truncate_tables(cursor, REFERENCE_TABLES)

        print_info("Inserting reference data...")

        insert_vehicle_types(cursor)
        insert_membership_tiers(cursor)
        insert_payment_methods(cursor)
        insert_ride_status(cursor)
        insert_cancellation_reasons(cursor)

        conn.commit()

        total_records = (
            len(VEHICLE_TYPES)
            + len(MEMBERSHIP_TIERS)
            + len(PAYMENT_METHODS)
            + len(RIDE_STATUS)
            + len(CANCELLATION_REASONS)
        )

        print_success("Reference data generated successfully.")
        print_info(f"Total Records Inserted : {total_records}")
        print_info(f"Execution Time : {timer.stop():.2f} seconds")

    except Exception as e:

        if conn is not None:
            conn.rollback()

        print_error(f"Reference data generation failed: {e}")

    finally:

        if cursor is not None:
            cursor.close()

        if conn is not None:
            conn.close()

        print_info("Database connection closed.")


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":
    main()