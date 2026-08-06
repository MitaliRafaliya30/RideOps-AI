-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 09_passengers.sql
-- Purpose  : Create passengers master table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.passengers
(
    passenger_id                   INTEGER GENERATED ALWAYS AS IDENTITY,
    passenger_code                 VARCHAR(10)  NOT NULL,
    signup_date                    DATE         NOT NULL,
    membership_tier_id             INTEGER      NOT NULL,
    preferred_payment_method_id    INTEGER      NOT NULL,
    home_zone_id                   INTEGER      NOT NULL,
    avg_driver_rating_given        DECIMAL(2,1) NOT NULL,
    is_active                      BOOLEAN      NOT NULL,

    CONSTRAINT pk_passengers
        PRIMARY KEY (passenger_id),

    CONSTRAINT uq_passenger_code
        UNIQUE (passenger_code),

    CONSTRAINT fk_passenger_membership
        FOREIGN KEY (membership_tier_id)
        REFERENCES master.membership_tiers(membership_tier_id),

    CONSTRAINT fk_passenger_payment_method
        FOREIGN KEY (preferred_payment_method_id)
        REFERENCES master.payment_methods(payment_method_id),

    CONSTRAINT chk_passenger_code
        CHECK (
            passenger_code ~ '^PASS[0-9]{6}$'
        ),

    CONSTRAINT chk_signup_date
        CHECK (
            signup_date <= CURRENT_DATE
        ),

    CONSTRAINT chk_home_zone
        CHECK (
            home_zone_id BETWEEN 1 AND 265
        ),

    CONSTRAINT chk_avg_driver_rating
        CHECK (
            avg_driver_rating_given BETWEEN 1.0 AND 5.0
        )
);

COMMENT ON TABLE master.passengers IS
'Stores passenger master information.';

COMMENT ON COLUMN master.passengers.passenger_id IS
'Internal surrogate primary key.';

COMMENT ON COLUMN master.passengers.passenger_code IS
'Unique passenger identifier.';

COMMENT ON COLUMN master.passengers.signup_date IS
'Passenger registration date.';

COMMENT ON COLUMN master.passengers.membership_tier_id IS
'Reference to passenger membership tier.';

COMMENT ON COLUMN master.passengers.preferred_payment_method_id IS
'Reference to preferred payment method.';

COMMENT ON COLUMN master.passengers.home_zone_id IS
'Passenger home TLC zone.';

COMMENT ON COLUMN master.passengers.avg_driver_rating_given IS
'Average driver rating given by the passenger.';

COMMENT ON COLUMN master.passengers.is_active IS
'Indicates whether the passenger account is active.';