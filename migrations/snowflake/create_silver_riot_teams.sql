-- ============================================================
-- Schema : silver
-- Table  : riot_teams
-- Source : Riot Match API v5 — info.teams
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.riot_teams (
    match_id                        VARCHAR         NOT NULL    COMMENT 'FK → silver.riot_match.match_id',
    team_id                         NUMBER          NOT NULL    COMMENT 'Team ID (100 = Blue, 200 = Red)',
    win                             BOOLEAN                     COMMENT 'Whether this team won',
    ban_1_champion_id               NUMBER                      COMMENT 'Champion banned in pick turn 1',
    ban_2_champion_id               NUMBER                      COMMENT 'Champion banned in pick turn 2',
    ban_3_champion_id               NUMBER                      COMMENT 'Champion banned in pick turn 3',
    ban_4_champion_id               NUMBER                      COMMENT 'Champion banned in pick turn 4',
    ban_5_champion_id               NUMBER                      COMMENT 'Champion banned in pick turn 5',
    objectives_baron_kills          NUMBER          DEFAULT 0   COMMENT 'Baron Nashor kills',
    objectives_dragon_kills         NUMBER          DEFAULT 0   COMMENT 'Dragon kills',
    objectives_tower_kills          NUMBER          DEFAULT 0   COMMENT 'Tower kills',
    objectives_inhibitor_kills      NUMBER          DEFAULT 0   COMMENT 'Inhibitor kills',
    objectives_rift_herald_kills    NUMBER          DEFAULT 0   COMMENT 'Rift Herald kills',
    objectives_champion_kills       NUMBER          DEFAULT 0   COMMENT 'Total champion kills by the team',

    CONSTRAINT pk_riot_teams PRIMARY KEY (match_id, team_id)
);
