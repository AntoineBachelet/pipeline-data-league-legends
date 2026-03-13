-- ============================================================
-- Schema : silver
-- Table  : leaguepedia_tournament_rosters
-- Source : bronze/leaguepedia/tournamentrosters/ (S3)
-- Scope  : Un joueur par ligne avec son rôle dans le tournoi
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.leaguepedia_tournament_rosters (
    team          VARCHAR NOT NULL COMMENT 'Team name',
    overview_page VARCHAR NOT NULL COMMENT 'Tournament unique identifier (wiki page name)',
    player_link   VARCHAR NOT NULL COMMENT 'Player identifier (wiki link)',
    role          VARCHAR          COMMENT 'Player role (Top, Jungle, Mid, Bot, Support)',
    region        VARCHAR          COMMENT 'Region of the team',
    footnotes     VARCHAR          COMMENT 'Footnotes of the wiki page',
    isused        VARCHAR          COMMENT 'Did the players actually play in this event? This field is a text representation of a list with separator ;;',
    iscomplete    BOOLEAN          COMMENT 'Will be false for some legacy events with incomplete rosters',
    pageandteam   VARCHAR          COMMENT 'Used for joining with TournamentResults',

    CONSTRAINT pk_leaguepedia_tournament_rosters PRIMARY KEY (team, overview_page, player_link)
);
