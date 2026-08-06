-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 06_ride_status.sql
-- Purpose  : Create ride_status reference table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.ride_status
(
    ride_status_id      INTEGER GENERATED ALWAYS AS IDENTITY,
    ride_status_name    VARCHAR(30) NOT NULL,

    CONSTRAINT pk_ride_status
        PRIMARY KEY (ride_status_id),

    CONSTRAINT uq_ride_status_name
        UNIQUE (ride_status_name)
);

COMMENT ON TABLE master.ride_status IS
'Defines all possible ride lifecycle statuses.';

COMMENT ON COLUMN master.ride_status.ride_status_id IS
'Primary key of the ride status.';

COMMENT ON COLUMN master.ride_status.ride_status_name IS
'Unique ride status name.';