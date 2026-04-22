-- ============================================================
-- Schema : silver
-- Table  : riot_match
-- Source : Riot Match API v5 (bronze/riot/match_data/.../match_details/)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.riot_match (
    match_id                VARCHAR         NOT NULL    COMMENT 'Riot match ID (e.g. EUW1_7819128666)',
    game_id                 NUMBER          NOT NULL    COMMENT 'Numeric game ID from Riot',
    platform_id             VARCHAR                     COMMENT 'Platform/server ID (e.g. EUW1)',
    game_version            VARCHAR                     COMMENT 'Game patch version (e.g. 14.7.123.456)',
    game_mode               VARCHAR                     COMMENT 'Game mode (e.g. CLASSIC)',
    game_type               VARCHAR                     COMMENT 'Game type (e.g. MATCHED_GAME)',
    queue_id                NUMBER                      COMMENT 'Queue ID (420 = Ranked Solo/Duo)',
    map_id                  NUMBER                      COMMENT 'Map ID (11 = Summoner''s Rift)',
    game_duration           NUMBER                      COMMENT 'Game duration in seconds',
    game_start_timestamp    NUMBER                      COMMENT 'Game start Unix timestamp (ms)',
    game_end_timestamp      NUMBER                      COMMENT 'Game end Unix timestamp (ms)',

    CONSTRAINT pk_riot_match PRIMARY KEY (match_id)
);
