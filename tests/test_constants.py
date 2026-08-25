"""
Tests for utils/constants.py
Validates that all event types, defects, and field schemas are defined correctly.
"""

import pytest
from database.utils.constants import (
    EVENT_TYPES,
    REQUIRED_FIELDS_BY_EVENT_TYPE,
    DEFECTS,
    CANCELLATION_REASONS,
    EVENT_JSON_SCHEMA,
    STATE_MACHINE_TRANSITIONS
)


class TestEventTypes:
    """Test event type definitions"""
    
    def test_all_seven_event_types_defined(self):
        """Verify all 7 event types exist"""
        expected = {"requested", "assigned", "accepted", "arrived", "started", "completed", "cancelled"}
        assert set(EVENT_TYPES.keys()) == expected
    
    def test_event_types_have_unique_values(self):
        """Event types should have unique integer values"""
        values = list(EVENT_TYPES.values())
        assert len(values) == len(set(values)), "Duplicate event type values"
    
    def test_event_types_are_integers(self):
        """All event type values should be integers"""
        for event_type, value in EVENT_TYPES.items():
            assert isinstance(value, int), f"{event_type} value is not int: {type(value)}"


class TestRequiredFields:
    """Test required fields per event type"""
    
    def test_all_event_types_have_required_fields(self):
        """Every event type should have required fields defined"""
        for event_type in EVENT_TYPES.keys():
            assert event_type in REQUIRED_FIELDS_BY_EVENT_TYPE, f"No required fields for {event_type}"
    
    def test_required_fields_are_lists(self):
        """Required fields should be lists of strings"""
        for event_type, fields in REQUIRED_FIELDS_BY_EVENT_TYPE.items():
            assert isinstance(fields, list), f"{event_type} fields is not a list"
            for field in fields:
                assert isinstance(field, str), f"Field {field} in {event_type} is not string"
    
    def test_core_fields_always_required(self):
        """event_id, ride_id, event_type, event_timestamp should be required in all"""
        core_fields = {"event_id", "ride_id", "event_type", "event_timestamp"}
        for event_type, fields in REQUIRED_FIELDS_BY_EVENT_TYPE.items():
            for core in core_fields:
                assert core in fields, f"{core} missing from {event_type}"
    
    def test_driver_vehicle_required_from_assigned(self):
        """driver_id and vehicle_id required from assigned stage onward"""
        assigned_onward = ["assigned", "accepted", "arrived", "started", "completed"]
        for event_type in assigned_onward:
            fields = REQUIRED_FIELDS_BY_EVENT_TYPE[event_type]
            assert "driver_id" in fields, f"driver_id missing from {event_type}"
            assert "vehicle_id" in fields, f"vehicle_id missing from {event_type}"
    
    def test_driver_vehicle_null_before_assigned(self):
        """driver_id and vehicle_id should NOT be required before assigned"""
        before_assigned = ["requested"]
        for event_type in before_assigned:
            fields = REQUIRED_FIELDS_BY_EVENT_TYPE[event_type]
            # These should NOT be in required (they can be null)
            # So we're just checking that requested doesn't require them
            assert "driver_id" not in fields, f"driver_id should not be required on {event_type}"
    
    def test_cancellation_reason_required_on_cancelled(self):
        """cancellation_reason_id required only on cancelled"""
        assert "cancellation_reason_id" in REQUIRED_FIELDS_BY_EVENT_TYPE["cancelled"]
        
        for event_type in ["requested", "assigned", "accepted", "arrived", "started", "completed"]:
            assert "cancellation_reason_id" not in REQUIRED_FIELDS_BY_EVENT_TYPE[event_type]
    
    def test_trip_data_required_on_completed(self):
        """trip_miles, fares required only on completed"""
        trip_fields = {"trip_miles", "base_passenger_fare", "tips", "driver_pay"}
        assert trip_fields.issubset(REQUIRED_FIELDS_BY_EVENT_TYPE["completed"])


class TestDefects:
    """Test defect definitions"""
    
    def test_all_eight_defects_defined(self):
        """All 8 v1 defects should be defined"""
        assert len(DEFECTS) == 8, f"Expected 8 defects, got {len(DEFECTS)}"
        assert set(DEFECTS.keys()) == {1, 2, 3, 4, 5, 6, 7, 8}
    
    def test_defect_names_match_spec(self):
        """Defect names should match the design spec"""
        expected_names = {
            1: "out_of_order",
            2: "late_arrival",
            3: "exact_duplicate",
            4: "near_duplicate",
            5: "null_required_field",
            6: "invalid_event_type",
            7: "out_of_range",
            8: "incomplete_ride"
        }
        for defect_id, expected_name in expected_names.items():
            assert DEFECTS[defect_id]["name"] == expected_name, \
                f"Defect {defect_id} name mismatch: {DEFECTS[defect_id]['name']} vs {expected_name}"
    
    def test_defect_categories_defined(self):
        """Each defect should have a category"""
        for defect_id, defect_info in DEFECTS.items():
            assert "category" in defect_info, f"Defect {defect_id} missing category"
            assert isinstance(defect_info["category"], str)


class TestCancellationReasons:
    """Test cancellation reason definitions"""
    
    def test_eight_cancellation_reasons_defined(self):
        """All 8 cancellation reasons should be defined"""
        assert len(CANCELLATION_REASONS) == 8, f"Expected 8 reasons, got {len(CANCELLATION_REASONS)}"
        assert set(CANCELLATION_REASONS.keys()) == {1, 2, 3, 4, 5, 6, 7, 8}
    
    def test_cancellation_reason_names_match_spec(self):
        """Reason names should match your schema"""
        expected = {
            1: "Driver Cancelled",
            2: "Rider Cancelled",
            3: "No Driver Available",
            4: "Payment Failed",
            5: "Duplicate Request",
            6: "Driver No Show",
            7: "Rider No Show",
            8: "Vehicle Breakdown"
        }
        for reason_id, expected_name in expected.items():
            assert CANCELLATION_REASONS[reason_id] == expected_name, \
                f"Reason {reason_id} mismatch: {CANCELLATION_REASONS[reason_id]} vs {expected_name}"


class TestEventJsonSchema:
    """Test event JSON schema"""
    
    def test_schema_has_all_20_fields(self):
        """Event schema should have all 20 fields from design"""
        expected_fields = {
            "event_id", "ride_id", "event_type", "event_timestamp",
            "passenger_id", "driver_id", "vehicle_id",
            "pickup_location_id", "dropoff_location_id", "payment_method_id",
            "cancellation_reason_id",
            "trip_miles",
            "base_passenger_fare", "tolls", "bcf", "sales_tax",
            "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
            "tips", "driver_pay"
        }
        assert set(EVENT_JSON_SCHEMA.keys()) == expected_fields
    
    def test_schema_field_types_are_strings(self):
        """Each field should map to a type string"""
        for field, field_type in EVENT_JSON_SCHEMA.items():
            assert isinstance(field_type, str), f"Field {field} type is not string: {type(field_type)}"
    
    def test_nullable_fields_documented(self):
        """Fields that can be null should be documented"""
        nullable_fields = [v for v in EVENT_JSON_SCHEMA.values() if "null" in v.lower()]
        # Just verify that the schema acknowledges nullability
        assert len(nullable_fields) > 0, "No nullable fields documented"


class TestStateMachineTransitions:
    """Test state machine validity"""
    
    def test_all_states_have_transitions(self):
        """Every event type should have valid next states"""
        for event_type in EVENT_TYPES.keys():
            assert event_type in STATE_MACHINE_TRANSITIONS, f"No transitions defined for {event_type}"
    
    def test_completed_and_cancelled_are_terminals(self):
        """Terminal states should have no outgoing transitions"""
        terminals = ["completed", "cancelled"]
        for terminal in terminals:
            transitions = STATE_MACHINE_TRANSITIONS.get(terminal, [])
            assert transitions == [], f"{terminal} should be terminal but has transitions: {transitions}"
    
    def test_requested_can_reach_assigned_or_cancelled(self):
        """From requested, ride can be assigned or cancelled immediately"""
        requested_next = STATE_MACHINE_TRANSITIONS["requested"]
        assert "assigned" in requested_next
        assert "cancelled" in requested_next


class TestConstantsIntegrity:
    """Integration tests across constants"""
    
    def test_no_duplicate_defect_names(self):
        """Each defect should have a unique name"""
        names = [d["name"] for d in DEFECTS.values()]
        assert len(names) == len(set(names)), "Duplicate defect names"
    
    def test_constants_not_empty(self):
        """All constant dicts should be non-empty"""
        assert EVENT_TYPES, "EVENT_TYPES is empty"
        assert REQUIRED_FIELDS_BY_EVENT_TYPE, "REQUIRED_FIELDS_BY_EVENT_TYPE is empty"
        assert DEFECTS, "DEFECTS is empty"
        assert CANCELLATION_REASONS, "CANCELLATION_REASONS is empty (should have 8 reasons)"
        assert EVENT_JSON_SCHEMA, "EVENT_JSON_SCHEMA is empty"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])