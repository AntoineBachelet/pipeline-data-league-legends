-- ============================================================
-- Schema : silver
-- Table  : riot_match_participants
-- Source : Riot Match API v5 — info.participants
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.riot_match_participants (
    match_id                            VARCHAR         NOT NULL    COMMENT 'FK → silver.riot_match.match_id',
    participant_id                      NUMBER          NOT NULL    COMMENT 'In-game participant ID (1–10)',
    puuid                               VARCHAR                     COMMENT 'Riot PUUID of the player',
    team_id                             NUMBER                      COMMENT 'Team ID (100 = Blue, 200 = Red)',
    riot_id_game_name                   VARCHAR                     COMMENT 'Riot ID game name',
    riot_id_tagline                     VARCHAR                     COMMENT 'Riot ID tagline',
    champion_id                         NUMBER                      COMMENT 'Champion numeric ID',
    champion_name                       VARCHAR                     COMMENT 'Champion name (e.g. Jinx)',
    team_position                       VARCHAR                     COMMENT 'Assigned team position (TOP, JUNGLE, MIDDLE, BOTTOM, UTILITY)',
    individual_position                 VARCHAR                     COMMENT 'Detected individual position',
    win                                 BOOLEAN                     COMMENT 'Whether the participant won',
    kills                               NUMBER          DEFAULT 0   COMMENT 'Number of kills',
    deaths                              NUMBER          DEFAULT 0   COMMENT 'Number of deaths',
    assists                             NUMBER          DEFAULT 0   COMMENT 'Number of assists',
    gold_earned                         NUMBER          DEFAULT 0   COMMENT 'Total gold earned',
    total_damage_dealt_to_champions     NUMBER          DEFAULT 0   COMMENT 'Total damage dealt to champions',
    total_minions_killed                NUMBER          DEFAULT 0   COMMENT 'Total lane minions killed (CS)',
    neutral_minions_killed              NUMBER          DEFAULT 0   COMMENT 'Neutral minions killed (jungle CS)',
    vision_score                        NUMBER          DEFAULT 0   COMMENT 'Vision score',
    wards_placed                        NUMBER          DEFAULT 0   COMMENT 'Wards placed',
    wards_killed                        NUMBER          DEFAULT 0   COMMENT 'Wards killed',
    detector_wards_placed               NUMBER          DEFAULT 0   COMMENT 'Control wards placed',
    cs_diff_at_15                       NUMBER                      COMMENT 'CS difference vs lane opponent at 15 min (from timeline)',
    gold_diff_at_15                     NUMBER                      COMMENT 'Gold difference vs lane opponent at 15 min (from timeline)',

    CONSTRAINT pk_riot_match_participants PRIMARY KEY (match_id, participant_id)
);

-- Migration : add timeline-derived columns if they do not exist yet
ALTER TABLE silver.riot_match_participants
    ADD COLUMN IF NOT EXISTS cs_diff_at_15   NUMBER COMMENT 'CS difference vs lane opponent at 15 min (from timeline)';
ALTER TABLE silver.riot_match_participants
    ADD COLUMN IF NOT EXISTS gold_diff_at_15 NUMBER COMMENT 'Gold difference vs lane opponent at 15 min (from timeline)';
