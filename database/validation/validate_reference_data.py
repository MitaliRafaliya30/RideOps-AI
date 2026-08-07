from database.config.db_config import get_connection

from database.utils.logger import (
    print_header,
    print_info,
    print_success,
    print_error,
)

from database.utils.timer import Timer

from psycopg import Connection
from psycopg import Cursor


EXPECTED_COUNTS = {
    "vehicle_types": 6,
    "membership_tiers": 4,
    "payment_methods": 5,
    "ride_status": 7,
    "cancellation_reasons": 8,
}

def validate_row_counts(cursor: Cursor) -> bool:
    """
    Validates row counts for all reference tables.
    """

    print_info("Validating row counts...")

    all_passed = True

    for table, expected in EXPECTED_COUNTS.items():

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM master.{table};
            """
        )

        actual = cursor.fetchone()[0]

        if actual == expected:
            print_success(f"{table:<25} Expected={expected}  Actual={actual}")

        else:
            print_error(f"{table:<25} Expected={expected}  Actual={actual}")
            all_passed = False

    return all_passed


def validate_duplicate_values(cursor: Cursor) -> bool:
    """
    Validates that all business key columns contain unique values.
    """

    print_info("Validating duplicate values...")

    duplicate_checks = [
        ("vehicle_types", "vehicle_type_name"),
        ("membership_tiers", "membership_name"),
        ("payment_methods", "payment_method_name"),
        ("ride_status", "ride_status_name"),
        ("cancellation_reasons", "cancellation_reason_name"),
    ]

    all_passed = True

    for table, column in duplicate_checks:

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM
            (
                SELECT {column}
                FROM master.{table}
                GROUP BY {column}
                HAVING COUNT(*) > 1
            ) duplicates;
            """
        )

        duplicate_count = cursor.fetchone()[0]

        if duplicate_count == 0:
            print_success(f"{table:<25} No duplicate values")

        else:
            print_error(f"{table:<25} {duplicate_count} duplicate value(s)")
            all_passed = False

    return all_passed


def validate_null_values(cursor: Cursor) -> bool:
    """
    Validates that NOT NULL business columns contain no NULL values.
    """

    print_info("Validating NULL values...")

    null_checks = [

        ("vehicle_types", "vehicle_type_name"),
        ("vehicle_types", "capacity"),
        ("vehicle_types", "base_fare_multiplier"),

        ("membership_tiers", "membership_name"),
        ("membership_tiers", "reward_points_multiplier"),

        ("payment_methods", "payment_method_name"),

        ("ride_status", "ride_status_name"),

        ("cancellation_reasons", "cancellation_reason_name"),
        ("cancellation_reasons", "reason_category"),
    ]

    all_passed = True

    for table, column in null_checks:

        cursor.execute(
            f"""
            SELECT COUNT(*)
            FROM master.{table}
            WHERE {column} IS NULL;
            """
        )

        null_count = cursor.fetchone()[0]

        if null_count == 0:
            print_success(f"{table}.{column}  No NULL values")

        else:
            print_error(f"{table}.{column}  NULL Count = {null_count}")
            all_passed = False

    return all_passed



def validate_primary_keys(cursor: Cursor) -> bool:
    """
    Validates primary key identity values.
    """

    print_info("Validating primary keys...")

    pk_checks = [

        ("vehicle_types", "vehicle_type_id"),
        ("membership_tiers", "membership_tier_id"),
        ("payment_methods", "payment_method_id"),
        ("ride_status", "ride_status_id"),
        ("cancellation_reasons", "cancellation_reason_id"),
    ]

    all_passed = True

    for table, pk in pk_checks:

        cursor.execute(
            f"""
            SELECT
                MIN({pk}),
                MAX({pk}),
                COUNT(*)
            FROM master.{table};
            """
        )

        min_id, max_id, count = cursor.fetchone()

        if min_id == 1 and max_id == count:
            print_success(
                f"{table:<25} Identity values valid"
            )

        else:
            print_error(
                f"{table:<25} Identity mismatch "
                f"(Min={min_id}, Max={max_id}, Count={count})"
            )

            all_passed = False

    return all_passed

def validate_check_constraints(cursor: Cursor) -> bool:
    """
    Validates business rule constraints.
    """

    print_info("Validating business rules...")

    all_passed = True

    cursor.execute("""
        SELECT COUNT(*)
        FROM master.vehicle_types
        WHERE capacity <= 0;
    """)

    if cursor.fetchone()[0] == 0:
        print_success("Vehicle capacity > 0")

    else:
        print_error("Invalid vehicle capacity found")
        all_passed = False

    cursor.execute("""
        SELECT COUNT(*)
        FROM master.membership_tiers
        WHERE reward_points_multiplier < 1;
    """)

    if cursor.fetchone()[0] == 0:
        print_success("Reward multiplier >= 1")

    else:
        print_error("Invalid reward multiplier found")
        all_passed = False

    cursor.execute("""
        SELECT COUNT(*)
        FROM master.cancellation_reasons
        WHERE reason_category NOT IN ('DRIVER','RIDER','SYSTEM');
    """)

    if cursor.fetchone()[0] == 0:
        print_success("Cancellation categories valid")

    else:
        print_error("Invalid cancellation category found")
        all_passed = False

    return all_passed


def main() -> None:
    """
    Validates all PostgreSQL reference data.
    """

    timer = Timer()
    timer.start()

    conn: Connection | None = None
    cursor: Cursor | None = None

    overall_result = True

    try:

        print_header("RideOps AI - Reference Data Validation")

        conn = get_connection()
        cursor = conn.cursor()

        print_info("Connected to PostgreSQL.")

        print()

        # --------------------------------------------------
        # Execute Validations
        # --------------------------------------------------

        overall_result &= validate_row_counts(cursor)

        print()

        overall_result &= validate_duplicate_values(cursor)

        print()

        overall_result &= validate_null_values(cursor)

        print()

        overall_result &= validate_primary_keys(cursor)

        print()

        overall_result &= validate_check_constraints(cursor)

        # --------------------------------------------------
        # Final Summary
        # --------------------------------------------------

        print("\n" + "=" * 70)

        if overall_result:
            print_success("REFERENCE DATA VALIDATION PASSED")

        else:
            print_error("REFERENCE DATA VALIDATION FAILED")

        print_info(f"Execution Time : {timer.stop():.2f} seconds")

        print("=" * 70)

    except Exception as e:

        print_error(f"Validation failed: {e}")

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