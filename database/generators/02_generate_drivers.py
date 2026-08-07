from datetime import date
import random
from database.utils.database import truncate_tables
from database.utils.constants import MASTER_TABLES

from faker import Faker

from psycopg import Connection
from psycopg import Cursor

from database.config.db_config import get_connection

from database.utils.logger import (
    print_header,
    print_info,
    print_success,
    print_error,
)

from database.utils.timer import Timer

# ============================================================
# Faker Configuration
# ============================================================

fake = Faker("en_IN")

# ============================================================
# Driver Generation Configuration
# ============================================================

TOTAL_DRIVERS = 20_000

BATCH_SIZE = 1_000

ACTIVE_PERCENTAGE = 90
INACTIVE_PERCENTAGE = 8
SUSPENDED_PERCENTAGE = 2

TODAY = date.today()

START_DATE = date(2018, 1, 1)


# ============================================================
# Helper Functions
# ============================================================

def generate_driver_status() -> str:
    """
    Generates driver status based on predefined distribution.
    """

    return random.choices(
        population=[
            "ACTIVE",
            "INACTIVE",
            "SUSPENDED",
        ],
        weights=[
            ACTIVE_PERCENTAGE,
            INACTIVE_PERCENTAGE,
            SUSPENDED_PERCENTAGE,
        ],
        k=1,
    )[0]


def generate_driver_rating() -> float:
    """
    Generates realistic Uber driver ratings.
    """

    rating_band = random.choices(
        population=[
            (4.8, 5.0),
            (4.5, 4.8),
            (4.0, 4.5),
            (3.0, 4.0),
        ],
        weights=[
            40,
            40,
            15,
            5,
        ],
        k=1,
    )[0]

    return round(
        random.uniform(
            rating_band[0],
            rating_band[1],
        ),
        1,
    )


# ============================================================
# Driver Generator
# ============================================================

def generate_driver(driver_number: int) -> tuple:
    """
    Generates a single driver record.
    """

    driver_code = f"DRV{driver_number:07d}"

    driver_name = fake.name()

    rating = generate_driver_rating()

    # Date the driver joined the RideOps platform
    join_date = fake.date_between(
        start_date=START_DATE,
        end_date=TODAY,
    )

    # RideOps tenure (completed years)
    rideops_tenure = (
        TODAY.year
        - join_date.year
        - (
            (TODAY.month, TODAY.day)
            < (join_date.month, join_date.day)
        )
    )

    # Total professional driving experience
    experience_years = random.randint(
        rideops_tenure,
        20,
    )

    status = generate_driver_status()

    return (
        driver_code,
        driver_name,
        rating,
        experience_years,
        join_date,
        status,
    )


# ============================================================
# Batch Generator
# ============================================================

def generate_driver_batch(
    start_driver_number: int,
    batch_size: int,
) -> list[tuple]:
    """
    Generates a batch of driver records.
    """

    drivers = []

    for driver_number in range(
        start_driver_number,
        start_driver_number + batch_size,
    ):
        drivers.append(
            generate_driver(driver_number)
        )

    return drivers

# ============================================================
# Insert Drivers
# ============================================================
def insert_drivers(
    cursor: Cursor,
    drivers: list[tuple],
) -> None:
    """
    Inserts a batch of drivers into master.drivers.
    """

    query = """
        INSERT INTO master.drivers
        (
            driver_code,
            driver_name,
            rating,
            experience_years,
            join_date,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s);
    """

    cursor.executemany(
        query,
        drivers,
    )

    print_success(
        f"Inserted {len(drivers)} drivers."
    )

# ============================================================
# Temporary Testing
# ============================================================

# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Generates and loads driver data into PostgreSQL.
    """

    timer = Timer()
    timer.start()

    conn: Connection | None = None
    cursor: Cursor | None = None

    try:

        print_header("RideOps AI - Driver Data Generator")

        conn = get_connection()
        cursor = conn.cursor()

        print_info("Connected to PostgreSQL.")
        print_info("Cleaning existing driver data...")

        truncate_tables(
            cursor,
            MASTER_TABLES,
        )
        total_inserted = 0

        for start_driver_number in range(
            1,
            TOTAL_DRIVERS + 1,
            BATCH_SIZE,
        ):

            batch_size = min(
                BATCH_SIZE,
                TOTAL_DRIVERS - start_driver_number + 1,
            )

            drivers = generate_driver_batch(
                start_driver_number=start_driver_number,
                batch_size=batch_size,
            )

            insert_drivers(
                cursor,
                drivers,
            )

            total_inserted += len(drivers)

        conn.commit()

        print_success("Driver data generated successfully.")

        print_info(
            f"Total Drivers Inserted : {total_inserted}"
        )

        print_info(
            f"Execution Time : {timer.stop():.2f} seconds"
        )

    except Exception as e:

        if conn is not None:
            conn.rollback()

        print_error(
            f"Driver data generation failed: {e}"
        )

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