-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 05_payment_methods.sql
-- Purpose  : Create payment_methods reference table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.payment_methods
(
    payment_method_id      INTEGER GENERATED ALWAYS AS IDENTITY,
    payment_method_name    VARCHAR(30) NOT NULL,

    CONSTRAINT pk_payment_methods
        PRIMARY KEY (payment_method_id),

    CONSTRAINT uq_payment_method_name
        UNIQUE (payment_method_name)
);

COMMENT ON TABLE master.payment_methods IS
'Defines supported payment methods.';

COMMENT ON COLUMN master.payment_methods.payment_method_id IS
'Primary key of the payment method.';

COMMENT ON COLUMN master.payment_methods.payment_method_name IS
'Unique payment method name.';