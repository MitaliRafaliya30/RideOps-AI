"""
Simple script to load NYC Taxi Zone Lookup CSV into PostgreSQL
Run from project root: python load_zones_simple.py
"""

import csv
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Get the project root directory
PROJECT_ROOT = Path(__file__).parent.absolute()

# Get connection parameters from environment
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", 5432))
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
POSTGRES_DB = os.getenv("POSTGRES_DB", "rideops_db")

# CSV file path (try multiple locations)
CSV_LOCATIONS = [
    PROJECT_ROOT / "data" / "taxi_zone_lookup.csv",
    Path("data/taxi_zone_lookup.csv"),
    Path("taxi_zone_lookup.csv"),
]

print(f"Project root: {PROJECT_ROOT}\n")


def find_csv_file():
    """Find the CSV file in known locations"""
    for csv_path in CSV_LOCATIONS:
        print(f"Checking: {csv_path}")
        if csv_path.exists():
            print(f"✓ Found CSV at: {csv_path}\n")
            return csv_path
    
    print(f"❌ CSV file not found in any of these locations:")
    for path in CSV_LOCATIONS:
        print(f"  - {path}")
    return None


def main():
    """Load zone lookup from CSV into PostgreSQL"""
    
    print("\n" + "=" * 70)
    print("Loading NYC Taxi Zone Lookup from CSV")
    print("=" * 70 + "\n")
    
    # Find CSV file
    csv_file = find_csv_file()
    if not csv_file:
        return False
    
    try:
        # Try to import psycopg
        try:
            import psycopg
        except ImportError:
            print("❌ ERROR: psycopg module not found")
            print("Install it with: pip install psycopg")
            return False
        
        # Connect to database
        print(f"Connecting to PostgreSQL...")
        print(f"  Host: {POSTGRES_HOST}:{POSTGRES_PORT}")
        print(f"  Database: {POSTGRES_DB}")
        print(f"  User: {POSTGRES_USER}\n")
        
        conn = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cursor = conn.cursor()
        print("✓ Connected to PostgreSQL\n")
        
        # Create table if it doesn't exist
        print("Creating master.zone_lookup table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS master.zone_lookup (
                zone_id INT PRIMARY KEY,
                borough VARCHAR(100),
                zone_name VARCHAR(100)
            );
        """)
        conn.commit()
        print("✓ Table created/verified\n")
        
        # Truncate existing data
        print("Truncating existing data...")
        cursor.execute("TRUNCATE TABLE master.zone_lookup;")
        conn.commit()
        print("✓ Data truncated\n")
        
        # Read CSV and insert data
        print(f"Reading CSV file: {csv_file}")
        rows_inserted = 0
        
        with open(csv_file, 'r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Print column names to verify
            if reader.fieldnames:
                print(f"✓ CSV columns found: {reader.fieldnames}\n")
            
            for row in reader:
                try:
                    # Handle different column name variations
                    zone_id_col = 'LocationID' if 'LocationID' in row else 'location_id'
                    borough_col = 'Borough' if 'Borough' in row else 'borough'
                    zone_col = 'Zone' if 'Zone' in row else 'zone_name'
                    
                    zone_id = int(row[zone_id_col])
                    borough = row[borough_col]
                    zone_name = row[zone_col]
                    
                    cursor.execute(
                        "INSERT INTO master.zone_lookup (zone_id, borough, zone_name) VALUES (%s, %s, %s)",
                        (zone_id, borough, zone_name)
                    )
                    rows_inserted += 1
                    
                except KeyError as e:
                    print(f"❌ ERROR: Missing column {e} in CSV")
                    print(f"Available columns: {list(row.keys())}")
                    return False
                except ValueError as e:
                    print(f"❌ ERROR: Invalid data - {e}")
                    print(f"Row: {row}")
                    return False
        
        # Commit all inserts
        conn.commit()
        print(f"✓ Inserted {rows_inserted} zones\n")
        
        # Verify
        cursor.execute("SELECT COUNT(*) FROM master.zone_lookup")
        count = cursor.fetchone()[0]
        print(f"✓ Verification: {count} zones in database\n")
        
        # Show sample
        cursor.execute("SELECT zone_id, borough, zone_name FROM master.zone_lookup LIMIT 5")
        samples = cursor.fetchall()
        print(f"✓ Sample zones:")
        for zone_id, borough, zone_name in samples:
            print(f"  {zone_id:3d} | {borough:20s} | {zone_name}")
        
        cursor.close()
        conn.close()
        print("\n" + "=" * 70)
        print("✓ Zone lookup loaded successfully!")
        print("=" * 70 + "\n")
        return True
        
    except Exception as e:
        print(f"\n❌ ERROR: {type(e).__name__}: {e}\n")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)