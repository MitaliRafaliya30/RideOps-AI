-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 03_vehicle_types.sql
-- Purpose  : Create vehicle_types reference table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.vehicle_types
(
    vehicle_type_id          INTEGER GENERATED ALWAYS AS IDENTITY,
    vehicle_type_name        VARCHAR(30) NOT NULL,
    capacity                 SMALLINT NOT NULL,
    base_fare_multiplier     NUMERIC(4,2) NOT NULL DEFAULT 1.00,

    CONSTRAINT pk_vehicle_types
        PRIMARY KEY (vehicle_type_id),

    CONSTRAINT uq_vehicle_types_name
        UNIQUE (vehicle_type_name),

    CONSTRAINT chk_vehicle_types_capacity
        CHECK (capacity > 0),

    CONSTRAINT chk_vehicle_types_multiplier
        CHECK (base_fare_multiplier > 0)
);

COMMENT ON TABLE master.vehicle_types IS
'Reference table containing supported vehicle categories.';

COMMENT ON COLUMN master.vehicle_types.vehicle_type_id IS
'Surrogate primary key.';

COMMENT ON COLUMN master.vehicle_types.vehicle_type_name IS
'Vehicle category name (e.g. UberX, UberXL).';

COMMENT ON COLUMN master.vehicle_types.capacity IS
'Maximum passenger capacity for the vehicle type.';

COMMENT ON COLUMN master.vehicle_types.base_fare_multiplier IS
'Pricing multiplier applied for this vehicle category.';