-- ============================================================
-- Schema : silver
-- Table  : riot_player_rankings
-- Source : Riot League API v4 (entries/by-puuid)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.riot_player_rankings (
    puuid           VARCHAR         NOT NULL    COMMENT 'Riot PUUID of the player',
    player          VARCHAR         NOT NULL    COMMENT 'Player display name (from Leaguepedia)',
    queue_type      VARCHAR         NOT NULL    COMMENT 'Queue type (e.g. RANKED_SOLO_5x5, RANKED_FLEX_SR)',
    tier            VARCHAR                     COMMENT 'Ranked tier (e.g. GOLD, PLATINUM)',
    rank            VARCHAR                     COMMENT 'Rank within tier (e.g. I, II, III, IV)',
    league_points   NUMBER                      COMMENT 'League Points (LP)',
    wins            NUMBER                      COMMENT 'Total wins in this queue',
    losses          NUMBER                      COMMENT 'Total losses in this queue',
    veteran         BOOLEAN                     COMMENT 'True if player is a veteran in this tier',
    inactive        BOOLEAN                     COMMENT 'True if player is flagged as inactive',
    fresh_blood     BOOLEAN                     COMMENT 'True if player is new to this tier',
    hot_streak      BOOLEAN                     COMMENT 'True if player is on a win streak',
    snapshot_date   DATE            NOT NULL    COMMENT 'Date the snapshot was taken',

    CONSTRAINT pk_riot_player_rankings PRIMARY KEY (puuid, queue_type, snapshot_date)
);
