"""
Test Event Hubs connection (optional)
Run this to verify your setup before using the emitter.
"""

import os
import json
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv("EVENT_HUBS_CONNECTION_STRING")
hub_name = os.getenv("EVENT_HUBS_HUB_NAME", "ride-events")

print(f"Connection String: {connection_string[:50]}...")
print(f"Hub Name: {hub_name}")

if not connection_string:
    print("ERROR: EVENT_HUBS_CONNECTION_STRING not set in .env")
    exit(1)

print("\n✓ Configuration loaded successfully!")
print("\nNOTE: Full connection test requires 'azure-eventhub' library.")
print("For v1 (simulated), connection string is validated but not used.")