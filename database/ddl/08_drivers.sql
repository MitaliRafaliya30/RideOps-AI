-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 08_drivers.sql
-- Purpose  : Create drivers master table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.drivers
(
    driver_id           INTEGER GENERATED ALWAYS AS IDENTITY,
    driver_code         VARCHAR(10)  NOT NULL,
    driver_name         VARCHAR(100) NOT NULL,
    rating              DECIMAL(2,1) NOT NULL,
    experience_years    SMALLINT     NOT NULL,
    join_date           DATE         NOT NULL,
    status              VARCHAR(20)  NOT NULL,

    CONSTRAINT pk_drivers
        PRIMARY KEY (driver_id),

    CONSTRAINT uq_driver_code
        UNIQUE (driver_code),

    CONSTRAINT chk_driver_code
        CHECK (
            driver_code ~ '^DRV[0-9]{7}$'
        ),

    CONSTRAINT chk_driver_rating
        CHECK (
            rating BETWEEN 1.0 AND 5.0
        ),

    CONSTRAINT chk_driver_experience
        CHECK (
            experience_years >= 0
        ),

    CONSTRAINT chk_driver_join_date
        CHECK (
            join_date <= CURRENT_DATE
        ),

    CONSTRAINT chk_driver_status
        CHECK (
            status IN (
                'ACTIVE',
                'INACTIVE',
                'SUSPENDED'
            )
        )
);

COMMENT ON TABLE master.drivers IS
'Stores master information about Uber drivers.';

COMMENT ON COLUMN master.drivers.driver_id IS
'Internal surrogate primary key.';

COMMENT ON COLUMN master.drivers.driver_code IS
'Unique human-readable driver identifier.';

COMMENT ON COLUMN master.drivers.driver_name IS
'Full name of the driver.';

COMMENT ON COLUMN master.drivers.rating IS
'Current average driver rating.';

COMMENT ON COLUMN master.drivers.experience_years IS
'Years of driving experience.';

COMMENT ON COLUMN master.drivers.join_date IS
'Date the driver joined the platform.';

COMMENT ON COLUMN master.drivers.status IS
'Current driver availability status.';