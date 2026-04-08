WITH tournaments AS (
    SELECT *
    FROM {{ source('silver', 'LEAGUEPEDIA_TOURNAMENTS') }}
),

players AS (
    SELECT *
    FROM {{ source('silver', 'LEAGUEPEDIA_PLAYERS') }}
),

tournament_rosters AS (
    SELECT *
    FROM {{ source('silver', 'LEAGUEPEDIA_TOURNAMENT_ROSTERS') }}
),

last_regular_split AS (
    SELECT
        NAME, OVERVIEW_PAGE, LEAGUE, DATE_START, IS_PLAYOFFS, SPLIT, YEAR
    FROM tournaments
    WHERE LEAGUE = 'LoL EMEA Championship' AND IS_PLAYOFFS = '0' AND DATE_START IS NOT NULL
    ORDER BY DATE_START DESC
    LIMIT 1
),

tournament_rosters_last_regular_split AS (
    SELECT lrs.NAME, lrs.OVERVIEW_PAGE, lrs.LEAGUE, lrs.DATE_START, lrs.IS_PLAYOFFS, lrs.SPLIT, lrs.YEAR,
        tr.PLAYER_LINK, tr.ROLE, tr.TEAM
    FROM tournament_rosters AS tr
    INNER JOIN last_regular_split AS lrs ON tr.OVERVIEW_PAGE = lrs.OVERVIEW_PAGE
)

SELECT trlrs.NAME, trlrs.OVERVIEW_PAGE, trlrs.LEAGUE, trlrs.DATE_START, trlrs.IS_PLAYOFFS, trlrs.SPLIT, trlrs.YEAR, trlrs.PLAYER_LINK, trlrs.ROLE, trlrs.TEAM,
    p.NAME AS PLAYER_NAME, p.NATIVE_NAME, p.NAME_ALPHABET, p.NAME_FULL, p.IMAGE, p.COUNTRY, p.BIRTHDATE, p.CONTRACT, p.SOLOQUEUE_IDS, p.LOLPROS
FROM tournament_rosters_last_regular_split AS trlrs
LEFT JOIN players AS p ON trlrs.PLAYER_LINK = p.PLAYER
WHERE trlrs.ROLE IN ('Top', 'Jungle', 'Mid', 'Bot', 'Support')