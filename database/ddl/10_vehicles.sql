-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 10_vehicles.sql
-- Purpose  : Create vehicles master table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.vehicles
(
    vehicle_id            INTEGER GENERATED ALWAYS AS IDENTITY,
    vehicle_code          VARCHAR(10)  NOT NULL,
    plate_number          VARCHAR(20)  NOT NULL,
    driver_id             INTEGER      NOT NULL,
    vehicle_type_id       INTEGER      NOT NULL,
    manufacture_year      SMALLINT     NOT NULL,
    last_service_date     DATE         NOT NULL,
    is_active             BOOLEAN      NOT NULL,

    CONSTRAINT pk_vehicles
        PRIMARY KEY (vehicle_id),

    CONSTRAINT uq_vehicle_code
        UNIQUE (vehicle_code),

    CONSTRAINT uq_plate_number
        UNIQUE (plate_number),

    CONSTRAINT uq_vehicle_driver
        UNIQUE (driver_id),

    CONSTRAINT fk_vehicle_driver
        FOREIGN KEY (driver_id)
        REFERENCES master.drivers(driver_id),

    CONSTRAINT fk_vehicle_type
        FOREIGN KEY (vehicle_type_id)
        REFERENCES master.vehicle_types(vehicle_type_id),

    CONSTRAINT chk_vehicle_code
        CHECK (
            vehicle_code ~ '^VEH[0-9]{6}$'
        ),

    CONSTRAINT chk_manufacture_year
        CHECK (
            manufacture_year BETWEEN 2015
            AND EXTRACT(YEAR FROM CURRENT_DATE)
        ),

    CONSTRAINT chk_last_service_date
        CHECK (
            last_service_date <= CURRENT_DATE
        )
);

COMMENT ON TABLE master.vehicles IS
'Stores master information about Uber vehicles.';

COMMENT ON COLUMN master.vehicles.vehicle_id IS
'Internal surrogate primary key.';

COMMENT ON COLUMN master.vehicles.vehicle_code IS
'Unique vehicle identifier.';

COMMENT ON COLUMN master.vehicles.plate_number IS
'Unique vehicle registration number.';

COMMENT ON COLUMN master.vehicles.driver_id IS
'Reference to the assigned driver.';

COMMENT ON COLUMN master.vehicles.vehicle_type_id IS
'Reference to the vehicle category.';

COMMENT ON COLUMN master.vehicles.manufacture_year IS
'Vehicle manufacturing year.';

COMMENT ON COLUMN master.vehicles.last_service_date IS
'Most recent vehicle service date.';

COMMENT ON COLUMN master.vehicles.is_active IS
'Indicates whether the vehicle is active.';