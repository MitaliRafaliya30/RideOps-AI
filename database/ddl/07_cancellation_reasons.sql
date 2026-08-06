-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 07_cancellation_reasons.sql
-- Purpose  : Create cancellation_reasons reference table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.cancellation_reasons
(
    cancellation_reason_id      INTEGER GENERATED ALWAYS AS IDENTITY,
    cancellation_reason_name    VARCHAR(100) NOT NULL,
    reason_category             VARCHAR(20) NOT NULL,

    CONSTRAINT pk_cancellation_reasons
        PRIMARY KEY (cancellation_reason_id),

    CONSTRAINT uq_cancellation_reason_name
        UNIQUE (cancellation_reason_name),

    CONSTRAINT chk_reason_category
        CHECK (
            reason_category IN (
                'DRIVER',
                'RIDER',
                'SYSTEM'
            )
        )
);

COMMENT ON TABLE master.cancellation_reasons IS
'Stores standardized ride cancellation reasons.';

COMMENT ON COLUMN master.cancellation_reasons.cancellation_reason_id IS
'Primary key of the cancellation reason.';

COMMENT ON COLUMN master.cancellation_reasons.cancellation_reason_name IS
'Standardized cancellation reason description.';

COMMENT ON COLUMN master.cancellation_reasons.reason_category IS
'Identifies who or what caused the cancellation (DRIVER, RIDER, SYSTEM).';