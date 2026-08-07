from datetime import date
import random

from faker import Faker

from psycopg import Connection
from psycopg import Cursor

from database.config.db_config import get_connection

from database.utils.database import truncate_tables

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
# Passenger Generation Configuration
# ============================================================

TOTAL_PASSENGERS = 200_000

BATCH_SIZE = 1_000

ACTIVE_PERCENTAGE = 95
INACTIVE_PERCENTAGE = 5

TODAY = date.today()

START_DATE = date(2018, 1, 1)

MASTER_TABLES = [
    "master.passengers",
]

def generate_membership_tier() -> int:
    """
    Generates membership tier based on predefined distribution.
    """

    return random.choices(
        population=[
            1,  # Basic
            2,  # Silver
            3,  # Gold
            4,  # Platinum
        ],
        weights=[
            60,
            20,
            15,
            5,
        ],
        k=1,
    )[0]

def generate_payment_method() -> int:
    """
    Generates preferred payment method based on predefined distribution.
    """

    return random.choices(
        population=[
            4,  # UPI
            1,  # Credit Card
            2,  # Debit Card
            5,  # Digital Wallet
            3,  # Cash
        ],
        weights=[
            40,
            25,
            15,
            10,
            10,
        ],
        k=1,
    )[0]

def generate_passenger_rating() -> float:
    """
    Generates realistic average driver rating given by a passenger.
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

def generate_passenger_status() -> bool:
    """
    Generates passenger active status based on predefined distribution.
    """

    return random.choices(
        population=[
            True,
            False,
        ],
        weights=[
            ACTIVE_PERCENTAGE,
            INACTIVE_PERCENTAGE,
        ],
        k=1,
    )[0]

def generate_passenger(passenger_number: int) -> tuple:
    """
    Generates a single passenger record.
    """

    passenger_code = f"PASS{passenger_number:06d}"

    signup_date = fake.date_between(
        start_date=START_DATE,
        end_date=TODAY,
    )

    membership_tier_id = generate_membership_tier()

    preferred_payment_method_id = generate_payment_method()

    home_zone_id = random.randint(1, 265)

    avg_driver_rating_given = generate_passenger_rating()

    is_active = generate_passenger_status()

    return (
        passenger_code,
        signup_date,
        membership_tier_id,
        preferred_payment_method_id,
        home_zone_id,
        avg_driver_rating_given,
        is_active,
    )


def generate_passenger_batch(
    start_passenger_number: int,
    batch_size: int,
) -> list[tuple]:
    """
    Generates a batch of passenger records.
    """

    passengers = []

    for passenger_number in range(
        start_passenger_number,
        start_passenger_number + batch_size,
    ):
        passengers.append(
            generate_passenger(passenger_number)
        )

    return passengers

# ============================================================
# Insert Passengers
# ============================================================

def insert_passengers(
    cursor: Cursor,
    passengers: list[tuple],
) -> None:
    """
    Inserts a batch of passengers into master.passengers.
    """

    query = """
        INSERT INTO master.passengers
        (
            passenger_code,
            signup_date,
            membership_tier_id,
            preferred_payment_method_id,
            home_zone_id,
            avg_driver_rating_given,
            is_active
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cursor.executemany(
        query,
        passengers,
    )

    print_success(
        f"Inserted {len(passengers)} passengers."
    )


# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Generates and loads passenger data into PostgreSQL.
    """

    timer = Timer()
    timer.start()

    conn: Connection | None = None
    cursor: Cursor | None = None

    try:

        print_header("RideOps AI - Passenger Data Generator")

        conn = get_connection()
        cursor = conn.cursor()

        print_info("Connected to PostgreSQL.")

        print_info("Cleaning existing passenger data...")

        truncate_tables(
            cursor,
            MASTER_TABLES,
        )

        total_inserted = 0

        for start_passenger_number in range(
            1,
            TOTAL_PASSENGERS + 1,
            BATCH_SIZE,
        ):

            batch_size = min(
                BATCH_SIZE,
                TOTAL_PASSENGERS - start_passenger_number + 1,
            )

            passengers = generate_passenger_batch(
                start_passenger_number=start_passenger_number,
                batch_size=batch_size,
            )

            insert_passengers(
                cursor,
                passengers,
            )

            total_inserted += len(passengers)

        conn.commit()

        print_success(
            "Passenger data generated successfully."
        )

        print_info(
            f"Total Passengers Inserted : {total_inserted}"
        )

        print_info(
            f"Execution Time : {timer.stop():.2f} seconds"
        )

    except Exception as e:

        if conn is not None:
            conn.rollback()

        print_error(
            f"Passenger data generation failed: {e}"
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