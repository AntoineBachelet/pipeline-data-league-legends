-- ============================================================
-- Schema : silver
-- Table  : leaguepedia_players
-- Source : bronze/leaguepedia/players/ (S3)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.leaguepedia_players (
    overview_page           VARCHAR        NOT NULL  COMMENT 'Unique identifier (wiki page name)',
    id                      VARCHAR                  COMMENT 'Player internal ID',
    player                  VARCHAR                  COMMENT 'In-game name (IGN)',
    name                    VARCHAR                  COMMENT 'Real name',
    native_name             VARCHAR                  COMMENT 'Name in native script',
    name_alphabet           VARCHAR                  COMMENT 'Script/alphabet used for native name',
    name_full               VARCHAR                  COMMENT 'Full legal name',
    image                   VARCHAR                  COMMENT 'Profile image file name',
    country                 VARCHAR                  COMMENT 'Country of origin',
    nationality             VARCHAR                  COMMENT 'Nationality (raw)',
    nationality_primary     VARCHAR                  COMMENT 'Primary nationality',
    age                     INTEGER                  COMMENT 'Current age',
    birthdate               DATE                     COMMENT 'Date of birth',
    deathdate               DATE                     COMMENT 'Date of death (if applicable)',
    residency               VARCHAR                  COMMENT 'Current residency region',
    residency_former        VARCHAR                  COMMENT 'Former residency region',
    team                    VARCHAR                  COMMENT 'Current primary team',
    team2                   VARCHAR                  COMMENT 'Current secondary team',
    current_teams           VARCHAR                  COMMENT 'All current teams (raw)',
    team_system             VARCHAR                  COMMENT 'Primary team system/league',
    team2_system            VARCHAR                  COMMENT 'Secondary team system/league',
    team_last               VARCHAR                  COMMENT 'Last known team',
    role                    VARCHAR                  COMMENT 'Current role (Top, Jungle, Mid, Bot, Support)',
    role_last               VARCHAR                  COMMENT 'Last known role',
    contract                VARCHAR                  COMMENT 'Contract end date (raw)',
    contract_text           VARCHAR                  COMMENT 'Contract details (free text)',
    fav_champs              VARCHAR                  COMMENT 'Favourite champions (raw list)',
    soloqueue_ids           VARCHAR                  COMMENT 'Solo queue account IDs (raw list)',
    -- social media
    askfm                   VARCHAR                  COMMENT 'Ask.fm handle',
    bluesky                 VARCHAR                  COMMENT 'Bluesky handle',
    discord                 VARCHAR                  COMMENT 'Discord handle',
    facebook                VARCHAR                  COMMENT 'Facebook page',
    instagram               VARCHAR                  COMMENT 'Instagram handle',
    lolpros                 VARCHAR                  COMMENT 'lolpros.gg profile slug',
    dpmlol                  VARCHAR                  COMMENT 'dpm.lol profile slug',
    reddit                  VARCHAR                  COMMENT 'Reddit username',
    snapchat                VARCHAR                  COMMENT 'Snapchat handle',
    stream                  VARCHAR                  COMMENT 'Twitch/streaming handle',
    kick                    VARCHAR                  COMMENT 'Kick.com handle',
    twitter                 VARCHAR                  COMMENT 'Twitter/X handle',
    threads                 VARCHAR                  COMMENT 'Threads handle',
    linkedin                VARCHAR                  COMMENT 'LinkedIn profile',
    vk                      VARCHAR                  COMMENT 'VKontakte profile',
    website                 VARCHAR                  COMMENT 'Personal website URL',
    weibo                   VARCHAR                  COMMENT 'Weibo profile',
    youtube                 VARCHAR                  COMMENT 'YouTube channel',
    -- flags
    is_retired              BOOLEAN                  COMMENT 'Has the player retired from LoL?',
    is_personality          BOOLEAN                  COMMENT 'Is the player a caster/personality (not a competitive player)?',
    is_substitute           BOOLEAN                  COMMENT 'Is the player a substitute?',
    is_trainee              BOOLEAN                  COMMENT 'Is the player a trainee/academy player?',
    is_lowercase            BOOLEAN                  COMMENT 'Is the IGN intentionally lowercased?',
    is_auto_team            BOOLEAN                  COMMENT 'Was the team assignment automated?',
    is_low_content          BOOLEAN                  COMMENT 'Is this a low-content/stub page?',
    to_wildrift             BOOLEAN                  COMMENT 'Did the player transition to Wild Rift?',
    to_valorant             BOOLEAN                  COMMENT 'Did the player transition to Valorant?',
    to_tft                  BOOLEAN                  COMMENT 'Did the player transition to TFT?',
    to_legends_of_runeterra BOOLEAN                  COMMENT 'Did the player transition to Legends of Runeterra?',
    to_2xko                 BOOLEAN                  COMMENT 'Did the player transition to 2XKO?',

    CONSTRAINT pk_leaguepedia_players PRIMARY KEY (overview_page)
);
