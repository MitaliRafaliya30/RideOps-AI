from datetime import date, timedelta
import random
import string

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
# Vehicle Generation Configuration
# ============================================================

TOTAL_VEHICLES = 20_000

BATCH_SIZE = 1_000

ACTIVE_PERCENTAGE = 98
INACTIVE_PERCENTAGE = 2

TODAY = date.today()

MASTER_TABLES = [
    "master.vehicles",
]

def generate_vehicle_type() -> int:
    """
    Generates vehicle type based on predefined distribution.
    """

    return random.choices(
        population=[
            1,  # UberX
            2,  # UberXL
            3,  # Uber Comfort
            5,  # Uber Green
            4,  # Uber Black
            6,  # Uber WAV
        ],
        weights=[
            55,
            15,
            12,
            8,
            5,
            5,
        ],
        k=1,
    )[0]

def generate_vehicle_status() -> bool:
    """
    Generates vehicle active status.
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

def generate_manufacture_year() -> int:
    """
    Generates realistic vehicle manufacture year.
    """

    return random.choices(
        population=[
            2026,
            2025,
            2024,
            2023,
            2022,
            2021,
            2020,
            2019,
            2018,
            2017,
        ],
        weights=[
            8,
            12,
            15,
            16,
            15,
            12,
            10,
            7,
            3,
            2,
        ],
        k=1,
    )[0]

def generate_last_service_date() -> date:
    """
    Generates vehicle service date within the last year.
    """

    return TODAY - timedelta(
        days=random.randint(
            0,
            365,
        )
    )

def generate_plate_number(vehicle_number: int) -> str:
    """
    Generates a unique deterministic vehicle plate number.
    """

    return f"AAA-{vehicle_number:05d}"

# ============================================================
# Vehicle Generator
# ============================================================

def generate_vehicle(vehicle_number: int) -> tuple:
    """
    Generates a single vehicle record.
    """

    vehicle_code = f"VEH{vehicle_number:06d}"

    plate_number = generate_plate_number(
    vehicle_number,
    )

    driver_id = vehicle_number

    vehicle_type_id = generate_vehicle_type()

    manufacture_year = generate_manufacture_year()

    last_service_date = generate_last_service_date()

    is_active = generate_vehicle_status()

    return (
        vehicle_code,
        plate_number,
        driver_id,
        vehicle_type_id,
        manufacture_year,
        last_service_date,
        is_active,
    )

# ============================================================
# Batch Generator
# ============================================================

def generate_vehicle_batch(
    start_vehicle_number: int,
    batch_size: int,
) -> list[tuple]:
    """
    Generates a batch of vehicle records.
    """

    vehicles = []

    for vehicle_number in range(
        start_vehicle_number,
        start_vehicle_number + batch_size,
    ):
        vehicles.append(
            generate_vehicle(vehicle_number)
        )

    return vehicles


# ============================================================
# Insert Vehicles
# ============================================================

def insert_vehicles(
    cursor: Cursor,
    vehicles: list[tuple],
) -> None:
    """
    Inserts a batch of vehicles into master.vehicles.
    """

    query = """
        INSERT INTO master.vehicles
        (
            vehicle_code,
            plate_number,
            driver_id,
            vehicle_type_id,
            manufacture_year,
            last_service_date,
            is_active
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """

    cursor.executemany(
        query,
        vehicles,
    )

    print_success(
        f"Inserted {len(vehicles)} vehicles."
    )

# ============================================================
# Main
# ============================================================

def main() -> None:
    """
    Generates and loads vehicle data into PostgreSQL.
    """

    timer = Timer()
    timer.start()

    conn: Connection | None = None
    cursor: Cursor | None = None

    try:

        print_header("RideOps AI - Vehicle Data Generator")

        conn = get_connection()
        cursor = conn.cursor()

        print_info("Connected to PostgreSQL.")

        print_info("Cleaning existing vehicle data...")

        truncate_tables(
            cursor,
            MASTER_TABLES,
        )

        total_inserted = 0

        for start_vehicle_number in range(
            1,
            TOTAL_VEHICLES + 1,
            BATCH_SIZE,
        ):

            batch_size = min(
                BATCH_SIZE,
                TOTAL_VEHICLES - start_vehicle_number + 1,
            )

            vehicles = generate_vehicle_batch(
                start_vehicle_number=start_vehicle_number,
                batch_size=batch_size,
            )

            insert_vehicles(
                cursor,
                vehicles,
            )

            total_inserted += len(vehicles)

        conn.commit()

        print_success(
            "Vehicle data generated successfully."
        )

        print_info(
            f"Total Vehicles Inserted : {total_inserted}"
        )

        print_info(
            f"Execution Time : {timer.stop():.2f} seconds"
        )

    except Exception as e:

        if conn is not None:
            conn.rollback()

        print_error(
            f"Vehicle data generation failed: {e}"
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