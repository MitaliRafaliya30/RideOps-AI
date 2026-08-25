REFERENCE_TABLES = [
    "master.cancellation_reasons",
    "master.ride_status",
    "master.payment_methods",
    "master.membership_tiers",
    "master.vehicle_types",
]

MASTER_TABLES = [
    "master.drivers",
    "master.passengers",
    "master.vehicles",
]

# Event types (7 states)
EVENT_TYPES = {
    "requested": 0,
    "assigned": 1,
    "accepted": 2,
    "arrived": 3,
    "started": 4,
    "completed": 5,
    "cancelled": 6
}

REQUIRED_FIELDS_BY_EVENT_TYPE = {
    "requested": ["event_id", "ride_id", "event_type", "event_timestamp", 
                  "passenger_id", "pickup_location_id", "dropoff_location_id", "payment_method_id"],
    "assigned": ["event_id", "ride_id", "event_type", "event_timestamp",
                 "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id"],
    "accepted": ["event_id", "ride_id", "event_type", "event_timestamp",
                 "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id"],
    "arrived": ["event_id", "ride_id", "event_type", "event_timestamp",
                "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id"],
    "started": ["event_id", "ride_id", "event_type", "event_timestamp",
                "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id"],
    "completed": ["event_id", "ride_id", "event_type", "event_timestamp",
                  "passenger_id", "driver_id", "vehicle_id", "pickup_location_id", "dropoff_location_id", "payment_method_id",
                  "trip_miles", "base_passenger_fare", "tips", "driver_pay"],
    "cancelled": ["event_id", "ride_id", "event_type", "event_timestamp",
                  "passenger_id", "cancellation_reason_id"]
}


# Defect definitions (v1 defect contract)
DEFECTS = {
    1: {"name": "out_of_order", "category": "ordering"},
    2: {"name": "late_arrival", "category": "timing"},
    3: {"name": "exact_duplicate", "category": "duplication"},
    4: {"name": "near_duplicate", "category": "duplication"},
    5: {"name": "null_required_field", "category": "malformed"},
    6: {"name": "invalid_event_type", "category": "malformed"},
    7: {"name": "out_of_range", "category": "logic"},
    8: {"name": "incomplete_ride", "category": "lifecycle"}
}

# Cancellation reasons (from Postgres)
CANCELLATION_REASONS = {
    1: "Driver Cancelled",
    2: "Rider Cancelled",
    3: "No Driver Available",
    4: "Payment Failed",
    5: "Duplicate Request",
    6: "Driver No Show",
    7: "Rider No Show",
    8: "Vehicle Breakdown"
}

EVENT_JSON_SCHEMA = {
    "event_id": "string (UUID)",
    "ride_id": "string (UUID)",
    "event_type": "string",
    "event_timestamp": "string (ISO 8601)",
    "passenger_id": "integer",
    "driver_id": "integer or null",
    "vehicle_id": "integer or null",
    "pickup_location_id": "integer",
    "dropoff_location_id": "integer",
    "payment_method_id": "integer",
    "cancellation_reason_id": "integer or null",
    "trip_miles": "double or null",
    "base_passenger_fare": "string or null",
    "tolls": "string or null",
    "bcf": "string or null",
    "sales_tax": "string or null",
    "congestion_surcharge": "string or null",
    "airport_fee": "string or null",
    "cbd_congestion_fee": "string or null",
    "tips": "string or null",
    "driver_pay": "string or null"
}

STATE_MACHINE_TRANSITIONS = {
    "requested": ["assigned", "cancelled"],
    "assigned": ["accepted", "cancelled"],
    "accepted": ["arrived", "cancelled"],
    "arrived": ["started", "cancelled"],
    "started": ["completed", "cancelled"],
    "completed": [],
    "cancelled": []
}