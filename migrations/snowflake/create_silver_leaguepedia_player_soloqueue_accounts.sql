-- ============================================================
-- Schema : silver
-- Table  : leaguepedia_player_soloqueue_accounts
-- Source : derived from silver.leaguepedia_players (soloqueue_ids)
-- ============================================================

CREATE TABLE IF NOT EXISTS silver.leaguepedia_player_soloqueue_accounts (
    player_overview_page    VARCHAR        NOT NULL  COMMENT 'FK → silver.leaguepedia_players.overview_page',
    region                  VARCHAR        NOT NULL  COMMENT 'Server region (EUW, KR, NA, ...)',
    game_name               VARCHAR        NOT NULL  COMMENT 'Riot game name (part before #)',
    tag_line                VARCHAR                  COMMENT 'Riot tag line (part after #), NULL if absent',
    full_id                 VARCHAR        NOT NULL  COMMENT 'Canonical form: game_name#tag_line or game_name if no tag',

    CONSTRAINT pk_player_soloqueue_accounts
        PRIMARY KEY (player_overview_page, region, game_name)
);
