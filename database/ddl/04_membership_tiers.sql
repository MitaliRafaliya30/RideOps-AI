-- ============================================================
-- Project  : RideOps AI
-- Module   : PostgreSQL Master Data
-- Schema   : master
-- File     : 04_membership_tiers.sql
-- Purpose  : Create membership_tiers reference table
-- ============================================================

CREATE TABLE IF NOT EXISTS master.membership_tiers
(
    membership_tier_id        INTEGER GENERATED ALWAYS AS IDENTITY,
    membership_name           VARCHAR(30) NOT NULL,
    reward_points_multiplier  DECIMAL(3,2) NOT NULL,

    CONSTRAINT pk_membership_tiers
        PRIMARY KEY (membership_tier_id),

    CONSTRAINT uq_membership_name
        UNIQUE (membership_name),

    CONSTRAINT chk_reward_points_multiplier
        CHECK (reward_points_multiplier >= 1)
);

COMMENT ON TABLE master.membership_tiers IS
'Defines passenger loyalty membership tiers.';

COMMENT ON COLUMN master.membership_tiers.membership_tier_id IS
'Primary key of the membership tier.';

COMMENT ON COLUMN master.membership_tiers.membership_name IS
'Unique membership tier name.';

COMMENT ON COLUMN master.membership_tiers.reward_points_multiplier IS
'Reward points earning multiplier for the membership tier.';