-- ============================================================
-- Schema : silver
-- Table  : leaguepedia_tournaments
-- Source : bronze/leaguepedia/tournaments/ (S3)
-- Scope  : La Ligue Française + LoL EMEA Championship
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.leaguepedia_tournaments (
    overview_page     VARCHAR        NOT NULL COMMENT 'Unique identifier (wiki page name)',
    name              VARCHAR                 COMMENT 'Tournament display name',
    date_start        DATE                    COMMENT 'Tournament start date',
    league            VARCHAR                 COMMENT 'League name (e.g. La Ligue Française)',
    region            VARCHAR                 COMMENT 'Geographic region',
    prizepool         VARCHAR                 COMMENT 'Prize pool amount (raw string, may include currency symbol)',
    currency          VARCHAR(10)             COMMENT 'Currency code (e.g. EUR, USD)',
    country           VARCHAR                 COMMENT 'Host country',
    rulebook          VARCHAR                 COMMENT 'URL to the official rulebook',
    event_type        VARCHAR                 COMMENT 'Type of event (e.g. Season, Showmatch)',
    links             VARCHAR                 COMMENT 'External links (raw)',
    sponsors          VARCHAR                 COMMENT 'Sponsors list (raw)',
    organizer         VARCHAR                 COMMENT 'Primary organizer',
    organizers        VARCHAR                 COMMENT 'All organizers (raw)',
    split             VARCHAR                 COMMENT 'Split name (e.g. Spring, Summer)',
    is_qualifier      BOOLEAN                 COMMENT 'Is this a qualifier event?',
    is_playoffs       BOOLEAN                 COMMENT 'Is this a playoffs event?',
    is_official       BOOLEAN                 COMMENT 'Is this an officially recognized event?',
    year              INTEGER                 COMMENT 'Tournament year',

    CONSTRAINT pk_leaguepedia_tournaments PRIMARY KEY (overview_page)
);
